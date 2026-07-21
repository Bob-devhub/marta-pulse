"""Canonical event schema and feed normalizers.

Two differently-shaped MARTA streams are unified here:
  * Bus  : GTFS-Realtime protobuf (VehiclePositions + TripUpdates),
           passed in as dicts (protobuf -> MessageToDict) so this module
           stays dependency-free and unit-testable.
  * Rail : bespoke JSON REST API (developerservices.itsmarta.com).

Every normalizer returns a list of canonical event dicts with EXACTLY the
keys in CANONICAL_FIELDS. Downstream (Eventstream, Bronze, Silver) relies
on this contract — change it only with a schema-version bump.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

SCHEMA_VERSION = "1.0"

CANONICAL_FIELDS = [
    "event_id",        # deterministic hash: dedupe key across retries
    "schema_version",
    "mode",            # 'bus' | 'rail'
    "event_type",      # 'vehicle_position' | 'trip_update' | 'rail_arrival'
    "vehicle_id",
    "trip_id",
    "route_id",
    "stop_id",
    "stop_sequence",
    "lat",
    "lon",
    "bearing",
    "delay_seconds",
    "event_ts",        # ISO-8601 UTC, from the feed
    "ingest_ts",       # ISO-8601 UTC, when we saw it
    "source_feed",
]

# MARTA rail line -> pseudo route_id so rail joins the same Gold model.
RAIL_LINE_TO_ROUTE = {
    "RED": "RAIL_RED",
    "GOLD": "RAIL_GOLD",
    "BLUE": "RAIL_BLUE",
    "GREEN": "RAIL_GREEN",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _epoch_to_iso(epoch: Any) -> str | None:
    if epoch in (None, "", 0, "0"):
        return None
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).isoformat(
        timespec="seconds"
    )


def _event_id(*parts: Any) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _blank_event() -> dict[str, Any]:
    return {f: None for f in CANONICAL_FIELDS}


def _finalize(ev: dict[str, Any]) -> dict[str, Any]:
    ev["schema_version"] = SCHEMA_VERSION
    ev["ingest_ts"] = ev["ingest_ts"] or _utc_now_iso()
    unknown = set(ev) - set(CANONICAL_FIELDS)
    if unknown:
        raise ValueError(f"non-canonical fields present: {unknown}")
    return ev


# --------------------------------------------------------------------------
# Bus: GTFS-Realtime (as dicts via google.protobuf.json_format.MessageToDict)
# --------------------------------------------------------------------------

def normalize_bus_vehicle_position(entity: dict) -> list[dict]:
    """Normalize one FeedEntity containing a `vehicle` block."""
    v = entity.get("vehicle") or {}
    if not v:
        return []
    pos = v.get("position") or {}
    trip = v.get("trip") or {}
    vehicle = v.get("vehicle") or {}
    ts = v.get("timestamp")

    ev = _blank_event()
    ev.update(
        mode="bus",
        event_type="vehicle_position",
        vehicle_id=vehicle.get("id") or vehicle.get("label"),
        trip_id=trip.get("tripId"),
        route_id=trip.get("routeId"),
        stop_id=v.get("stopId"),
        lat=pos.get("latitude"),
        lon=pos.get("longitude"),
        bearing=pos.get("bearing"),
        event_ts=_epoch_to_iso(ts),
        source_feed="marta_bus_vehiclepositions",
    )
    ev["event_id"] = _event_id("bus_vp", ev["vehicle_id"], ts)
    return [_finalize(ev)]


def normalize_bus_trip_update(entity: dict) -> list[dict]:
    """Normalize one FeedEntity containing a `tripUpdate` block.

    Emits one event per StopTimeUpdate so Gold can compute per-stop
    deviation directly from the feed's own predictions.
    """
    tu = entity.get("tripUpdate") or {}
    if not tu:
        return []
    trip = tu.get("trip") or {}
    vehicle = tu.get("vehicle") or {}
    feed_ts = tu.get("timestamp")

    events: list[dict] = []
    for stu in tu.get("stopTimeUpdate", []):
        arr = stu.get("arrival") or {}
        dep = stu.get("departure") or {}
        best = arr or dep
        ev = _blank_event()
        ev.update(
            mode="bus",
            event_type="trip_update",
            vehicle_id=vehicle.get("id") or vehicle.get("label"),
            trip_id=trip.get("tripId"),
            route_id=trip.get("routeId"),
            stop_id=stu.get("stopId"),
            stop_sequence=stu.get("stopSequence"),
            delay_seconds=best.get("delay"),
            event_ts=_epoch_to_iso(best.get("time") or feed_ts),
            source_feed="marta_bus_tripupdates",
        )
        ev["event_id"] = _event_id(
            "bus_tu", ev["trip_id"], ev["stop_id"], ev["stop_sequence"], feed_ts
        )
        events.append(_finalize(ev))
    return events


# --------------------------------------------------------------------------
# Rail: bespoke JSON REST API
# --------------------------------------------------------------------------

AGENCY_TZ = ZoneInfo("America/New_York")

# MARTA rail EVENT_TIME: 'M/d/yyyy h:mm:ss AM/PM', agency-local, hours NOT
# zero-padded ('7/12/2026 9:14:25 AM'). %m/%d/%I tolerate missing padding.
_RAIL_TS_FORMAT = "%m/%d/%Y %I:%M:%S %p"


def _parse_rail_delay(delay: str | None) -> int | None:
    """MARTA encodes delay like 'T582S' (582 seconds). Be defensive."""
    if not delay:
        return None
    digits = "".join(c for c in str(delay) if c.isdigit() or c == "-")
    try:
        return int(digits)
    except ValueError:
        return None


def _rail_event_time_to_utc_iso(raw: str | None) -> str | None:
    """Normalize rail EVENT_TIME to ISO-8601 UTC *at the source*.

    Lesson from the first build: deferring this to Silver meant every
    downstream consumer had to know about the bespoke local format, and
    strict Spark parsers choked on the unpadded hours. Typing/normalizing
    at the edge keeps the canonical contract honest: event_ts is ISO UTC
    for every mode.
    """
    if not raw:
        return None
    try:
        local = datetime.strptime(raw.strip(), _RAIL_TS_FORMAT).replace(
            tzinfo=AGENCY_TZ
        )
    except ValueError:
        return None  # unparseable -> null; DQ can flag, nothing crashes
    return local.astimezone(timezone.utc).isoformat(timespec="seconds")


def normalize_rail(record: dict) -> list[dict]:
    """Normalize one record from the rail realtime REST API."""
    line = (record.get("LINE") or "").upper()
    ev = _blank_event()
    ev.update(
        mode="rail",
        event_type="rail_arrival",
        vehicle_id=record.get("TRAIN_ID"),
        route_id=RAIL_LINE_TO_ROUTE.get(line, f"RAIL_{line}" if line else None),
        stop_id=record.get("STATION"),
        lat=float(record["LATITUDE"]) if record.get("LATITUDE") else None,
        lon=float(record["LONGITUDE"]) if record.get("LONGITUDE") else None,
        delay_seconds=_parse_rail_delay(record.get("DELAY")),
        event_ts=_rail_event_time_to_utc_iso(record.get("EVENT_TIME")),
        source_feed="marta_rail_realtime",
    )
    # Hash the RAW string so event identity is stable even if parsing evolves.
    ev["event_id"] = _event_id(
        "rail", ev["vehicle_id"], ev["stop_id"], record.get("EVENT_TIME")
    )
    return [_finalize(ev)]


def to_jsonl(events: list[dict]) -> str:
    return "\n".join(json.dumps(e, separators=(",", ":")) for e in events)

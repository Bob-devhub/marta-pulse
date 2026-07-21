import json
from pathlib import Path

import pytest

from marta_pulse.canonical import (
    CANONICAL_FIELDS,
    normalize_bus_trip_update,
    normalize_bus_vehicle_position,
    normalize_rail,
)

FIXTURES = Path(__file__).parent / "fixtures"


def load(name):
    return json.loads((FIXTURES / name).read_text())


def assert_canonical(ev):
    assert set(ev) == set(CANONICAL_FIELDS)
    assert ev["event_id"] and len(ev["event_id"]) == 32
    assert ev["ingest_ts"] is not None


def test_bus_vehicle_position():
    events = normalize_bus_vehicle_position(load("bus_vehicle_position.json"))
    assert len(events) == 1
    ev = events[0]
    assert_canonical(ev)
    assert ev["mode"] == "bus"
    assert ev["event_type"] == "vehicle_position"
    assert ev["vehicle_id"] == "1478"
    assert ev["trip_id"] == "8574123"
    assert ev["route_id"] == "110"
    assert ev["lat"] == pytest.approx(33.7756)
    assert ev["event_ts"].startswith("2025-07-08T")


def test_bus_trip_update_emits_per_stop():
    events = normalize_bus_trip_update(load("bus_trip_update.json"))
    assert len(events) == 2
    for ev in events:
        assert_canonical(ev)
        assert ev["event_type"] == "trip_update"
    assert events[0]["delay_seconds"] == 240
    assert events[1]["stop_sequence"] == 13
    assert events[0]["event_id"] != events[1]["event_id"]


def test_rail_normalization():
    events = normalize_rail(load("rail_arrival.json"))
    assert len(events) == 1
    ev = events[0]
    assert_canonical(ev)
    assert ev["mode"] == "rail"
    assert ev["route_id"] == "RAIL_RED"
    assert ev["vehicle_id"] == "401"
    assert ev["delay_seconds"] == 582
    assert ev["lat"] == pytest.approx(33.660274)
    # EVENT_TIME normalized at source: 07/08/2026 11:50:06 AM EDT -> UTC ISO
    assert ev["event_ts"] == "2026-07-08T15:50:06+00:00"


def test_rail_event_time_handles_unpadded_hours():
    # Real-world format: hours are not zero-padded ('9:14:25 AM')
    rec = load("rail_arrival.json") | {"EVENT_TIME": "7/12/2026 9:14:25 AM"}
    ev = normalize_rail(rec)[0]
    assert ev["event_ts"] == "2026-07-12T13:14:25+00:00"  # EDT -> UTC


def test_rail_event_time_garbage_is_null_not_crash():
    rec = load("rail_arrival.json") | {"EVENT_TIME": "not a time"}
    ev = normalize_rail(rec)[0]
    assert ev["event_ts"] is None
    assert ev["event_id"]  # identity still derived from the raw string


def test_event_ids_are_deterministic():
    fixture = load("bus_vehicle_position.json")
    a = normalize_bus_vehicle_position(fixture)[0]["event_id"]
    b = normalize_bus_vehicle_position(fixture)[0]["event_id"]
    assert a == b  # dedupe key must be stable across retries


def test_missing_vehicle_block_is_skipped():
    assert normalize_bus_vehicle_position({"id": "x"}) == []

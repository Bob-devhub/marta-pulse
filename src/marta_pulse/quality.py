"""Data-quality gates for the Silver telemetry conformance stream.

Rule outcomes: 'pass' rows continue to Silver; 'quarantine' rows land in
silver.telemetry_quarantine with the failed rule name for triage. A very
real GTFS-RT phenomenon: realtime feeds referencing trip_ids that do not
exist in the active static schedule (deadheads, ad-hoc trips, stale IDs).
"""

from __future__ import annotations

GEO_BOUNDS = {  # generous metro-Atlanta bounding box
    "lat_min": 33.2, "lat_max": 34.2,
    "lon_min": -84.9, "lon_max": -83.9,
}


def rules() -> dict[str, str]:
    """Rule name -> Spark SQL predicate that must hold for a PASS."""
    return {
        "has_event_id": "event_id IS NOT NULL",
        # MARTA's TripUpdates feed omits the vehicle descriptor entirely;
        # vehicle identity is only required where it's the join/dedupe key.
        "has_vehicle": "event_type = 'trip_update' OR vehicle_id IS NOT NULL",
        "known_mode": "mode IN ('bus','rail')",
        "position_in_bounds": (
            "lat IS NULL OR ("
            f"lat BETWEEN {GEO_BOUNDS['lat_min']} AND {GEO_BOUNDS['lat_max']} "
            f"AND lon BETWEEN {GEO_BOUNDS['lon_min']} AND {GEO_BOUNDS['lon_max']})"
        ),
        "plausible_delay": "delay_seconds IS NULL OR abs(delay_seconds) <= 7200",
        # joined-in flag: set by the Silver notebook after the dim_trip lookup
        "trip_in_schedule": "mode = 'rail' OR trip_known = true",
    }


def failed_rules_expr() -> str:
    """SQL expr producing an array of failed rule names (empty = pass)."""
    checks = ", ".join(
        f"CASE WHEN NOT ({pred}) THEN '{name}' END" for name, pred in rules().items()
    )
    return f"filter(array({checks}), x -> x IS NOT NULL)"

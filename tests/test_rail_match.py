"""Golden tests for rail station-name matching.

The pairs below are the disagreements that actually matter: feed labels
carry "STATION" suffixes and inconsistent punctuation, GTFS mostly does
not. If normalization ever stops collapsing these, rail silently drops out
of Gold again -- which is precisely the failure these tests exist to catch.
"""

import pytest

from marta_pulse.rail_match import (
    AMBIGUOUS_MARGIN_SECONDS,
    MATCH_WINDOW_SECONDS,
    axis_direction,
    bearing_to_direction,
    direction_to_bearing,
    is_ambiguous,
    normalize_station,
    rail_route_from_line,
    within_window,
)


@pytest.mark.parametrize(
    "feed_name,gtfs_name",
    [
        ("FIVE POINTS STATION", "Five Points"),
        ("PEACHTREE CENTER STATION", "Peachtree Center Station"),
        ("GARNETT STATION", "Garnett"),
        ("N AVE STATION", "N Ave"),
        ("EAST LAKE STATION", "East Lake Station"),
        # Punctuation and connector words disagree across sources
        ("MARTIN LUTHER KING, JR. STATION", "Martin Luther King Jr"),
        ("GEORGIA STATE STATION", "Georgia State"),
        ("ASHBY STATION", "Ashby"),
        ("H.E. HOLMES STATION", "HE Holmes"),
        ("HAMILTON E HOLMES STATION", "Hamilton E. Holmes"),
        ("ARTS CENTER STATION", "Arts Center"),
        ("DOME/GWCC/PHILIPS/CNN STATION", "Dome GWCC Philips CNN"),
        ("LINDBERGH CENTER STATION", "Lindbergh Center"),
        ("AIRPORT STATION", "Airport"),
    ],
)
def test_feed_and_gtfs_names_normalize_alike(feed_name, gtfs_name):
    assert normalize_station(feed_name) == normalize_station(gtfs_name)


@pytest.mark.parametrize(
    "feed_name,gtfs_name",
    [
        # Observed 2026-07-25 against feed_version 9f6554cafaa7903f: the
        # realtime feed and GTFS genuinely disagree on these names.
        ("BROOKHAVEN STATION", "BROOKHAVEN-OGLETHORPE STATION"),
        ("LINDBERGH STATION", "LINDBERGH CENTER STATION"),
        # Handled by rules rather than aliases
        ("EDGEWOOD CANDLER PARK STATION", "EDGEWOOD-CANDLER PARK STATION"),
        ("NORTH AVE STATION", "NORTH AVENUE STATION"),
        ("HAMILTON E HOLMES STATION", "HAMILTON E. HOLMES STATION"),
    ],
)
def test_real_feed_names_match_real_gtfs_names(feed_name, gtfs_name):
    assert normalize_station(feed_name) == normalize_station(gtfs_name)


def test_ampersand_and_at_are_expanded():
    assert normalize_station("EDGEWOOD & CANDLER PARK") == normalize_station(
        "Edgewood and Candler Park"
    )
    assert normalize_station("BANKHEAD @ HOLLOWELL") == normalize_station(
        "Bankhead at Hollowell"
    )


def test_distinct_stations_do_not_collide():
    """Aggressive normalization must not merge genuinely different stops."""
    assert normalize_station("EAST LAKE STATION") != normalize_station(
        "WEST LAKE STATION"
    )
    assert normalize_station("NORTH AVE STATION") != normalize_station(
        "WEST END STATION"
    )
    assert normalize_station("DECATUR STATION") != normalize_station(
        "EAST POINT STATION"
    )


def test_empty_input_returns_none_not_empty_string():
    # An empty key would join to every other empty key -- worse than no match.
    assert normalize_station(None) is None
    assert normalize_station("") is None
    assert normalize_station("   ") is None
    assert normalize_station("STATION") is None


def test_rail_route_from_line():
    assert rail_route_from_line("RED") == "RED"
    assert rail_route_from_line("red") == "RED"
    assert rail_route_from_line("RAIL_GOLD") == "GOLD"
    assert rail_route_from_line(" Blue ") == "BLUE"
    assert rail_route_from_line("PURPLE") is None
    assert rail_route_from_line(None) is None


def test_within_window():
    assert within_window(0)
    assert within_window(MATCH_WINDOW_SECONDS)
    assert within_window(-MATCH_WINDOW_SECONDS)
    assert not within_window(MATCH_WINDOW_SECONDS + 1)
    assert not within_window(None)


def test_direction_to_bearing_roundtrip():
    for letter, deg in (("N", 0.0), ("E", 90.0), ("S", 180.0), ("W", 270.0)):
        assert direction_to_bearing(letter) == deg
        assert bearing_to_direction(deg) == letter


def test_direction_accepts_feed_variants():
    assert direction_to_bearing("north") == 0.0
    assert direction_to_bearing("NB") == 0.0
    assert direction_to_bearing(" s ") == 180.0
    assert direction_to_bearing("") is None
    assert direction_to_bearing(None) is None
    assert direction_to_bearing("X") is None
    assert bearing_to_direction(None) is None
    assert bearing_to_direction(45.0) is None  # bus bearings are not directions


def test_axis_direction_picks_dominant_axis():
    # Red/Gold run north-south: latitude dominates
    assert axis_direction(0.15, -0.01) == "N"
    assert axis_direction(-0.15, 0.01) == "S"
    # Blue/Green run east-west: longitude dominates
    assert axis_direction(0.01, 0.20) == "E"
    assert axis_direction(-0.01, -0.20) == "W"
    assert axis_direction(None, 0.1) is None


def test_is_ambiguous():
    # Nearest 30s away, next 60s away -> 30s apart -> too close to call
    assert is_ambiguous(30, 60)
    # Nearest 10s, next 600s -> confident
    assert not is_ambiguous(10, 600)
    assert not is_ambiguous(10, None)
    # Exactly at the margin is not ambiguous
    assert not is_ambiguous(0, AMBIGUOUS_MARGIN_SECONDS)

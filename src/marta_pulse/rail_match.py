"""Matching MARTA's rail realtime feed to the GTFS schedule.

The rail feed is not GTFS-RT. It carries no `trip_id`, and it identifies
stations by display *name* ("FIVE POINTS STATION") rather than `stop_id`.
The bus path joins observation to schedule on `trip_id + stop_id`; rail has
neither, so every rail row was silently dropped by that inner join
(LESSONS #50).

Rail is matched in two stages instead:

1. **Station name -> stop_id**, by normalizing both sides to a comparison
   key (below) and joining on it. Normalization is deliberately aggressive
   because the two sources disagree on suffixes, punctuation, and the
   spelling of "&"/"AT".
2. **Nearest scheduled arrival**, because there is no trip identity: for a
   given (stop, route, service day) take the scheduled arrival closest in
   time to the observation, within `MATCH_WINDOW_SECONDS`.

**Stage 2 was tried and retired.** Validated against MARTA's own published
DELAY, nearest-arrival matching correlated at 0.006 -- nil -- and 95% of
matches were flagged ambiguous. It reports "time to the closest scheduled
train", which collapses toward zero by construction and cannot exceed half
the headway. Rail deviation now comes from the agency's DELAY field, which
is computed against the trip identity the feed never exposes (LESSON #57).

Stage 1 survives: the station crosswalk is deterministic and still needed
to attribute observations to stops. The windowing helpers below are kept
for the headway/bunching work, where "nearest scheduled" is the correct
question rather than a proxy for a different one.
"""

from __future__ import annotations

import re

# Half a typical MARTA rail headway at peak. Wider windows admit more
# matches but attribute more of them to the wrong trip.
MATCH_WINDOW_SECONDS = 600

# Two candidate arrivals closer together than this make the assignment a
# coin flip; the row is kept but flagged.
AMBIGUOUS_MARGIN_SECONDS = 120

# The realtime feed's LINE values map to synthetic route ids in
# canonical.py ("RAIL_RED"). GTFS uses its own ids, so the colour is the
# only durable join key. Resolve GTFS side by route_type = 1 (subway).
LINE_COLOURS = ("RED", "GOLD", "BLUE", "GREEN")

# Tokens that carry no identifying information and appear inconsistently
# across the two sources.
_NOISE_WORDS = {
    "STATION",
    "STA",
    "MARTA",
}

# Long form -> short form, applied to both sides so either spelling lands on
# the same key ("NORTH AVE" vs "NORTH AVENUE").
_ABBREVIATIONS = {
    "AVENUE": "AVE",
    "STREET": "ST",
    "BOULEVARD": "BLVD",
    "DRIVE": "DR",
    "PARKWAY": "PKWY",
    "CENTRE": "CTR",
    "CENTER": "CTR",
    "JUNIOR": "JR",
    "MOUNT": "MT",
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
}

# Genuine naming disagreements between the realtime feed and GTFS, which no
# rule can bridge because the two sources chose different names for the same
# place. Both sides are post-normalization keys; the feed key maps to the
# GTFS key. Extend from the coverage report emitted by gold_rail_deviation.
_ALIASES = {
    "BROOKHAVEN": "BROOKHAVEN OGLETHORPE",
    "LINDBERGH": "LINDBERGH CTR",
}


# The rail feed reports travel direction as a compass letter. `bearing` is
# already in the canonical schema and already means "direction of travel",
# so directions ride in on it -- no schema change, no Eventstream/KQL churn.
DIRECTION_BEARINGS = {"N": 0.0, "E": 90.0, "S": 180.0, "W": 270.0}


def direction_to_bearing(direction: str | None) -> float | None:
    """'N' / 'north' / 'NB' -> 0.0. Unknown or empty -> None."""
    if not direction:
        return None
    return DIRECTION_BEARINGS.get(direction.strip().upper()[:1])


def bearing_to_direction(bearing: float | None) -> str | None:
    """Inverse of `direction_to_bearing`, for joining back to the schedule."""
    if bearing is None:
        return None
    for letter, deg in DIRECTION_BEARINGS.items():
        if abs(float(bearing) - deg) < 1e-6:
            return letter
    return None


def axis_direction(delta_lat: float | None, delta_lon: float | None) -> str | None:
    """Compass letter for a trip's net movement, from first to last stop.

    Used to infer what GTFS `direction_id` 0/1 mean geographically for each
    rail route, rather than hardcoding an agency convention that could
    change between feed versions. Red/Gold run north-south, Blue/Green
    east-west, so the dominant axis is unambiguous.
    """
    if delta_lat is None or delta_lon is None:
        return None
    if abs(delta_lat) >= abs(delta_lon):
        return "N" if delta_lat > 0 else "S"
    return "E" if delta_lon > 0 else "W"


def normalize_station(name: str | None) -> str | None:
    """Reduce a station label to a key comparable across both sources.

    Uppercase, expand '&'/'@', drop punctuation and noise words, collapse
    whitespace. Returns None for empty input so it never produces a key
    that would join to another null.
    """
    if not name:
        return None
    key = name.upper()
    key = key.replace("&", " AND ")
    key = key.replace("@", " AT ")
    # Strip anything that isn't a letter, digit or space (hyphens, periods,
    # apostrophes, slashes all appear inconsistently).
    key = re.sub(r"[^A-Z0-9]+", " ", key)
    tokens = [t for t in key.split() if t and t not in _NOISE_WORDS]
    if not tokens:
        return None
    tokens = [_ABBREVIATIONS.get(t, t) for t in tokens]
    tokens = _merge_initials(tokens)
    key = " ".join(tokens)
    return _ALIASES.get(key, key)


def _merge_initials(tokens: list[str]) -> list[str]:
    """Collapse runs of single characters: ['H','E','HOLMES'] -> ['HE','HOLMES'].

    'H.E. Holmes' loses its periods above and becomes two tokens, while the
    other source writes 'HE Holmes' as one. Merging adjacent single-letter
    tokens reconciles them without touching 'Hamilton E Holmes', where the
    initial stands alone between full words.
    """
    out: list[str] = []
    run: list[str] = []
    for tok in tokens:
        if len(tok) == 1 and tok.isalpha():
            run.append(tok)
            continue
        if len(run) > 1:
            out.append("".join(run))
        elif run:
            out.append(run[0])
        run = []
        out.append(tok)
    if len(run) > 1:
        out.append("".join(run))
    elif run:
        out.append(run[0])
    return out


def rail_route_from_line(line: str | None) -> str | None:
    """'RED' / 'red' -> 'RED', else None. Colour is the stable rail key."""
    if not line:
        return None
    colour = line.strip().upper()
    colour = colour.removeprefix("RAIL_")
    return colour if colour in LINE_COLOURS else None


def is_ambiguous(best_delta: float | None, second_delta: float | None) -> bool:
    """True when the two nearest candidates are too close to distinguish."""
    if best_delta is None or second_delta is None:
        return False
    return abs(abs(second_delta) - abs(best_delta)) < AMBIGUOUS_MARGIN_SECONDS


def within_window(delta_seconds: float | None) -> bool:
    """True when an observation is close enough to a scheduled arrival."""
    if delta_seconds is None:
        return False
    return abs(delta_seconds) <= MATCH_WINDOW_SECONDS

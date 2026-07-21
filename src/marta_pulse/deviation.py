"""Schedule-vs-reality math. Pure Python; UDF/`F.expr`-friendly.

Two GTFS realities this module exists to handle correctly:

1. stop_times.txt clock values can exceed 24:00:00 ("25:15:00" means
   1:15 AM on the *next calendar day* but the *same service day*).
2. A service day is defined by calendar.txt/calendar_dates.txt and is
   anchored to the agency's local timezone (America/New_York for MARTA),
   typically rolling over around 03:00, not midnight.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

AGENCY_TZ = ZoneInfo("America/New_York")
SERVICE_DAY_ROLLOVER_HOUR = 3  # observations before 03:00 local belong to prior service day

ON_TIME_EARLY_SECONDS = -60    # up to 1 min early counts as on time
ON_TIME_LATE_SECONDS = 300     # up to 5 min late counts as on time
BUNCHING_HEADWAY_RATIO = 0.25  # actual headway < 25% of planned => bunched


def parse_gtfs_time(hms: str) -> int:
    """'HH:MM:SS' (HH may exceed 23) -> seconds after service-day midnight."""
    parts = hms.strip().split(":")
    if len(parts) != 3:
        raise ValueError(f"bad GTFS time: {hms!r}")
    h, m, s = (int(p) for p in parts)
    if not (0 <= m < 60 and 0 <= s < 60 and h >= 0):
        raise ValueError(f"bad GTFS time: {hms!r}")
    return h * 3600 + m * 60 + s


def service_date_for(observed_utc: datetime) -> date:
    """Map an observation instant to its GTFS service day (agency-local)."""
    local = observed_utc.astimezone(AGENCY_TZ)
    if local.hour < SERVICE_DAY_ROLLOVER_HOUR:
        return (local - timedelta(days=1)).date()
    return local.date()


def scheduled_instant_utc(service_day: date, gtfs_seconds: int) -> datetime:
    """Service day + seconds-after-midnight -> absolute UTC instant.

    Anchoring at local *noon minus 12h* sidesteps DST-transition midnights
    (the GTFS best-practice trick: noon minus 12 hours is 'midnight' even
    on days when 2 AM doesn't exist or happens twice).
    """
    noon = datetime(
        service_day.year, service_day.month, service_day.day, 12, tzinfo=AGENCY_TZ
    )
    return (noon - timedelta(hours=12) + timedelta(seconds=gtfs_seconds)).astimezone(
        ZoneInfo("UTC")
    )


def deviation_seconds(observed_utc: datetime, service_day: date, sched_hms: str) -> int:
    """Positive = late, negative = early."""
    sched = scheduled_instant_utc(service_day, parse_gtfs_time(sched_hms))
    return int((observed_utc - sched).total_seconds())


def otp_bucket(dev_seconds: int) -> str:
    if dev_seconds < ON_TIME_EARLY_SECONDS:
        return "early"
    if dev_seconds <= ON_TIME_LATE_SECONDS:
        return "on_time"
    return "late"


def is_bunched(actual_headway_s: float, planned_headway_s: float) -> bool:
    if planned_headway_s <= 0:
        return False
    return actual_headway_s < planned_headway_s * BUNCHING_HEADWAY_RATIO

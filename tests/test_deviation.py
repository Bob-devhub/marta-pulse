from datetime import date, datetime, timezone

import pytest

from marta_pulse.deviation import (
    deviation_seconds,
    is_bunched,
    otp_bucket,
    parse_gtfs_time,
    scheduled_instant_utc,
    service_date_for,
)


class TestParseGtfsTime:
    def test_normal_time(self):
        assert parse_gtfs_time("08:30:00") == 8 * 3600 + 30 * 60

    def test_after_midnight_service_time(self):
        # 25:15:00 = 1:15 AM next calendar day, same service day
        assert parse_gtfs_time("25:15:00") == 25 * 3600 + 15 * 60

    @pytest.mark.parametrize("bad", ["8:30", "aa:bb:cc", "08:75:00", ""])
    def test_rejects_garbage(self, bad):
        with pytest.raises(ValueError):
            parse_gtfs_time(bad)


class TestServiceDay:
    def test_evening_observation_same_day(self):
        # 23:00 EDT July 8 = 03:00 UTC July 9
        obs = datetime(2026, 7, 9, 3, 0, tzinfo=timezone.utc)
        assert service_date_for(obs) == date(2026, 7, 8)

    def test_after_midnight_belongs_to_prior_service_day(self):
        # 01:30 EDT July 9 = 05:30 UTC July 9 -> service day July 8
        obs = datetime(2026, 7, 9, 5, 30, tzinfo=timezone.utc)
        assert service_date_for(obs) == date(2026, 7, 8)

    def test_early_morning_after_rollover(self):
        # 04:00 EDT July 9 -> service day July 9
        obs = datetime(2026, 7, 9, 8, 0, tzinfo=timezone.utc)
        assert service_date_for(obs) == date(2026, 7, 9)


class TestScheduledInstant:
    def test_gt24h_time_lands_next_calendar_day(self):
        sched = scheduled_instant_utc(date(2026, 7, 8), parse_gtfs_time("25:15:00"))
        # 01:15 EDT July 9 = 05:15 UTC July 9
        assert sched == datetime(2026, 7, 9, 5, 15, tzinfo=timezone.utc)

    def test_dst_fallback_day(self):
        # Nov 1 2026: clocks fall back; noon-minus-12h anchor must not drift
        sched = scheduled_instant_utc(date(2026, 11, 1), parse_gtfs_time("06:00:00"))
        assert sched.astimezone(timezone.utc).hour == 11  # 06:00 EST = 11:00 UTC


class TestDeviation:
    def test_late_arrival_positive(self):
        obs = datetime(2026, 7, 9, 5, 20, tzinfo=timezone.utc)  # 01:20 EDT
        dev = deviation_seconds(obs, date(2026, 7, 8), "25:15:00")
        assert dev == 300

    def test_otp_buckets(self):
        assert otp_bucket(-120) == "early"
        assert otp_bucket(-30) == "on_time"
        assert otp_bucket(299) == "on_time"
        assert otp_bucket(301) == "late"


class TestBunching:
    def test_bunched_when_headway_collapses(self):
        assert is_bunched(actual_headway_s=90, planned_headway_s=600)

    def test_not_bunched_at_normal_headway(self):
        assert not is_bunched(actual_headway_s=540, planned_headway_s=600)

    def test_zero_planned_headway_is_safe(self):
        assert not is_bunched(actual_headway_s=10, planned_headway_s=0)

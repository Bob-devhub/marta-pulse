# ADR 0002: SCD Type 2 keyed on GTFS feed version

## Status
Accepted

## Context
MARTA republishes google_transit.zip on service changes. A deviation
computed in March must be judged against the March schedule even after a
May service change lands.

## Decision
The SHA-256 of the static zip is the feed_version. Silver dims and
fact_scheduled_stop_time are SCD2 with effective_from/effective_to
derived from ingest dates; Gold joins on the observation's service_date
falling inside the schedule row's validity window.

## Consequences
+ Historically correct OTP; enables "did the service change help?" analysis.
+ Idempotent batch: identical hash = no-op run.
- Full-snapshot close-and-insert on every new version (acceptable: GTFS
  dims are small; stop_times ~1-2M rows).

# TODO

- [x] ~~Migrate Function App to Flex Consumption~~ — done for the rebuild:
  `infra/main.bicep` now provisions FC1 with identity-based storage and a
  deployment container. Verify quota: Flex Consumption has its own regional
  quota separate from the old Dynamic SKU.
- [ ] **Fix Function zip-deploy remote build in CD** — verify `SCM_DO_BUILD_DURING_DEPLOYMENT`
  is set (or pre-install deps into `.python_packages`) so `AzureFunctionApp@2` installs
  requirements on Linux; replace the fragile `$(basename ...)` macro/bash mix in
  `cd-fabric-deploy.yml` with a computed bash variable.
- [x] ~~Document rail `event_ts` contract exception~~ — superseded: rail timestamps are
  now normalized to ISO-8601 UTC at the source (`canonical.py` 0.2.0); the contract holds.
- [x] ~~**Rail contributes zero rows to Gold**~~ (LESSON #50) — fixed in
  0.3.0: `marta_pulse/rail_match.py` (station-name normalization, 38/38
  MARTA stations, no key collisions) + `databricks/src/gold_rail_deviation.py`
  (direction-aware nearest-scheduled match). `DIRECTION` now rides in on
  `bearing`; GTFS `direction_id` meaning is inferred from trip geometry.
- [ ] **Rail OTP is agency-reported, bus is measured** — the two rows in
  `fact_schedule_deviation` answer subtly different questions. Label them
  separately on any dashboard tile; never present a blended OTP number.
- [ ] **Confirm the last rail station aliases** — `OMNI DOME` and possibly
  `LAKEWOOD` (GTFS may spell it "Lakewood/Ft McPherson") have no GTFS
  counterpart under the current rules. The coverage report printed by
  `gold_rail_deviation` lists unmatched names; add them to `_ALIASES`.
- [ ] **Port the rail match back to Fabric** — `fabric/` still has bus-only
  Gold. Same wheel, so it's a new `NB_Gold_Rail_Deviation` notebook plus a
  pipeline activity.
- [ ] **Backfill rail direction** — rows ingested before 0.3.0 have
  `bearing` NULL and match direction-agnostically (`match_direction_known =
  false`). They cannot be repaired retroactively; exclude them from
  direction-sensitive analysis or wait them out.
- [ ] **Add per-feed row-count assertions at each layer boundary** so a feed
  silently reaching zero fails the run instead of passing quietly.
- [ ] **Separate predicted vs. observed OTP** (LESSON #51). `trip_update`
  rows are forecasts; a true "actual arrival" metric needs either
  `vehicle_position` snapped to stops, or trip_updates filtered to
  `event_ts <= ingest_ts`. Label the current metric as predicted OTP in the
  dashboard so it isn't read as realized performance.
- [ ] **`agg_otp_route_hour` uses UTC hour** — consider grouping by agency-local hour so
  hourly OTP aligns with rider-facing time.
- [ ] **Evaluate materialized lake views for Gold aggregates** — `agg_otp_route_hour` is
  currently a full rebuild each run; an MLV could refresh it incrementally. Requires
  `delta.enableChangeDataFeed = true` on the Silver source tables (set at creation;
  not possible on the Kusto-mirrored `bronze.raw_events` shortcut). The
  lakehouse CDF advisory refers to this feature and is otherwise ignorable.

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
- [ ] **`agg_otp_route_hour` uses UTC hour** — consider grouping by agency-local hour so
  hourly OTP aligns with rider-facing time.
- [ ] **Evaluate materialized lake views for Gold aggregates** — `agg_otp_route_hour` is
  currently a full rebuild each run; an MLV could refresh it incrementally. Requires
  `delta.enableChangeDataFeed = true` on the Silver source tables (set at creation;
  not possible on the Kusto-mirrored `bronze.raw_events` shortcut). The
  lakehouse CDF advisory refers to this feature and is otherwise ignorable.

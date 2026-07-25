# MARTA Pulse on Databricks (build #3)

Same canonical core (`marta_pulse` wheel, identical Silver/Gold logic), third
orchestration. Deployed with **Databricks Asset Bundles** (`databricks.yml`
at the repo root).

## Topology

```
Azure Function (30s) ──► Azure Event Hub "telemetry"
                             ├── consumer group "databricks" ──► bronze.raw_events (Delta, typed)
                             └── consumer group "fabric"     ──► Fabric Eventstream (optional, parallel)
```

One producer, per-platform consumer groups. The Function only needs its
`eventstream-connection` Key Vault secret repointed at the Event Hub's
connection string — the code already speaks Event Hub protocol.

## One-time setup

1. **Event Hub**: create namespace `ehns-martapulse-databricks` (Standard, 1 TU) +
   hub `telemetry` (2+ partitions); consumer groups `databricks` and
   `fabric`. Repoint the Function's `eventstream-connection` secret and
   restart it. (If keeping Fabric fed: switch the Eventstream's source to an
   Azure Event Hubs source on group `fabric`.)
2. **Unity Catalog**: create catalog `marta_pulse` with schemas `bronze`,
   `silver`, `gold`, plus volumes `bronze.checkpoints` and
   `bronze.gtfs_static`.
3. **Secrets**: `databricks secrets create-scope marta-pulse` and put the
   Event Hub connection string in `eventhub-connection`.
4. **Bundle**: fill the three workspace hosts in `databricks.yml`, then:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy   -t dev
   databricks bundle run      -t dev gtfs_static_weekly
   ```
   `telemetry_refresh` then runs every 15 min: Event Hub drain → Silver
   conform → Gold deviation → Gold headway/bunching.

## Jobs

| Job | Cadence | Tasks |
|---|---|---|
| `gtfs_static_weekly` | Mon 07:00 UTC | GTFS download/unzip → Silver SCD2 dims |
| `telemetry_refresh` | every 15 min | EH→Bronze (availableNow) → Silver → Gold deviation → Gold headway |

## Serving

Lakeview dashboard over `gold.*` (OTP trend, worst stops, early-running
analysis, bunching); SQL alert on `gold.fact_bunching_events` growth replaces
the Fabric Activator.

## Notes

- Bronze lands **typed** (the Kafka reader parses ISO timestamps straight to
  `timestamp`) — no post-hoc casting anywhere, per LESSONS #1/#2.
- Notebooks are thin adapters over the wheel; platform deltas are only
  `dbutils` vs `mssparkutils`, UC three-part names, and volumes vs Files/.
- CI: `databricks bundle validate` belongs in `ci-validate.yml`; deploys can
  join `cd-fabric-deploy.yml` as a `databricks bundle deploy -t test/prod`
  job using the same OIDC service principal (add it to the workspaces).

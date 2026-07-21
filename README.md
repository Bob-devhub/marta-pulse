# MARTA Pulse — Schedule vs. Reality Lakehouse

Live MARTA vehicle telemetry (streaming) continuously measured against the
published GTFS schedule (batch) in **Microsoft Fabric**, with CI/CD via
**GitHub Actions** and `fabric-cicd`.

The batch layer here is not history — it's the **reference model** the
stream is judged against: SCD2-versioned schedule dims joined to live
positions to compute schedule deviation, headway bunching, and on-time
performance.

Full design: [`docs/architecture.md`](docs/architecture.md) · Decisions: [`docs/adr/`](docs/adr/) · Hard-won: [`LESSONS.md`](LESSONS.md)

## Repo map

| Path | What |
|---|---|
| `fabric/` | Fabric workspace items (Git-integration format): notebooks, pipelines, lakehouse, KQL bootstrap, placeholders for UI-authored items |
| `src/marta_pulse/` | Shared library (canonical schema + source-side timestamp normalization, GTFS math, SCD2, DQ rules) — built as a wheel, used by notebooks **and** the Function |
| `functions/ingest_gtfs_rt/` | Azure Function (Flex Consumption): polls bus protobuf + rail JSON every 30s → Eventstream |
| `.github/workflows/` | CI (lint/test/wheel/notebook checks) and CD (Function deploy + fabric-cicd test→prod) |
| `deploy/` | fabric-cicd entrypoint, `parameter.yml` env find/replace, per-env config |
| `infra/main.bicep` | Function App (FC1), Key Vault, App Insights, storage |
| `tests/` | Golden tests: >24:00:00 GTFS times, service-day rollover, DST, feed normalizers |

## Quickstart (dev)

Follow `docs/MARTA_Pulse_Execution_Guide_v2.pdf` — it encodes every lesson from
build #1. Short version:

1. **Signup:** rail API key at itsmarta.com; bus GTFS-RT uses the full `.pb` URLs
   (`.../vehicle/vehiclepositions.pb`, `.../tripupdate/tripupdates.pb`).
2. **Fabric workspace (dev):** `MartaPulse_LH` lakehouse (schemas enabled; create
   `bronze/silver/gold`), Eventhouse + KQL DB (run
   `fabric/MartaPulse_KQLDB.KQLDatabase/DatabaseSchema.kql`), a **standard**
   Eventstream with a custom endpoint source (Event Hub protocol).
3. **Single Bronze path:** Eventstream → Eventhouse `raw_events` (Direct
   ingestion, mapping `canonical_v1`); enable **OneLake availability** and add a
   Lakehouse shortcut named `bronze.raw_events` (keep the source table name).
4. **Infra:** `az deployment group create -f infra/main.bicep -p env=dev busVpUrl=... busTuUrl=... eventstreamEntityName=...`,
   then put `rail-api-key` and `eventstream-connection` secrets in the Key Vault.
5. **Library + Function:**
   ```bash
   pip install -e ".[dev]" && pytest
   python -m build --wheel
   # copy the wheel into functions/ingest_gtfs_rt/, append it to requirements.txt,
   # then: func azure functionapp publish func-martapulse-dev --python
   ```
   Also attach the wheel to a Fabric Environment (workspace default) so notebooks
   can `from marta_pulse import ...`.
6. **Git integration:** connect the dev workspace to this repo's `fabric/` folder
   on your feature branch (Fabric supports GitHub). Initial sync direction:
   workspace → Git; commit every UI-authored item immediately.
7. **Run order:** `PL_GTFS_Static_Ingest` (weekly batch spine) →
   `PL_Telemetry_Refresh` (15–30 min: Silver conform → Gold deviation → Gold
   headway/bunching).

## CI/CD (GitHub Actions)

- **PR → `.github/workflows/ci-validate.yml`**: ruff, pytest, wheel build,
  notebook compile + format checks.
- **main → `.github/workflows/cd-fabric-deploy.yml`**: build wheel → deploy
  Function (OIDC, remote build) → `fabric-cicd` publish to **test** → manual
  approval (GitHub environment `production`) → publish to **prod**. Environment
  GUID swaps live in `deploy/parameter.yml`.
- Dev workspace is Git-synced only; test/prod are deploy-only (never edited by

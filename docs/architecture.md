# MARTA Pulse — Schedule vs. Reality Lakehouse

A streaming + batch data engineering project on **Microsoft Fabric** with CI/CD via **Azure DevOps**. Live MARTA vehicle telemetry is continuously measured against the published GTFS schedule — the batch layer isn't history, it's the *reference model* the stream is judged against.

---

## 1. Data Sources

| Feed | Type | Format | Cadence | Auth |
|---|---|---|---|---|
| MARTA GTFS Static (`google_transit.zip`) | Batch | CSV files in zip (routes, trips, stops, stop_times, calendar, shapes) | Republished on service changes; poll weekly | None |
| MARTA Bus GTFS-Realtime — Vehicle Positions | Stream | Protobuf | ~10–30 sec snapshots | None |
| MARTA Bus GTFS-Realtime — Trip Updates | Stream | Protobuf | ~10–30 sec | None |
| MARTA Rail Realtime API (`developerservices.itsmarta.com:18096`) | Stream | JSON (REST) | Poll ~10 sec | API key (free signup) |

**Why the rail feed matters for novelty:** you get two *differently shaped* streams — a standards-based protobuf feed (bus) and a bespoke JSON REST API (rail) — normalized into one canonical event model. That heterogeneous-stream-unification story is rarely shown in Fabric content.

---

## 2. Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │                MICROSOFT FABRIC              │
                        │                                              │
 MARTA GTFS-RT (bus)    │  ┌────────────┐   ┌─────────────┐            │
 protobuf ──► Azure ────┼─►│ Eventstream │──►│ Eventhouse  │──┐        │
 MARTA Rail REST (json) │  │  (custom    │   │ (KQL DB:    │  │        │
 json ─────► Function ──┼─►│  endpoint)  │   │  raw_events)│  │        │
             (decoder/  │  └─────┬──────┘   └─────────────┘  │        │
              poller)   │        │                            │        │
                        │        │ (derived stream:           │        │
                        │        ▼  filtered/renamed)         ▼        │
                        │  ┌────────────┐            ┌──────────────┐  │
                        │  │ Activator  │            │  Lakehouse   │  │
                        │  │ (bunching/ │            │  Bronze      │  │
                        │  │  gap alert)│            │  telemetry   │  │
                        │  └────────────┘            │  (Delta,     │  │
                        │                            │  Eventstream │  │
 MARTA GTFS Static zip  │  ┌────────────┐            │  destination)│  │
 ────► Data Pipeline ───┼─►│ Lakehouse  │            └──────┬───────┘  │
       (weekly schedule)│  │ Bronze     │                   │          │
                        │  │ (raw zip + │                   │          │
                        │  │  csv files)│                   │          │
                        │  └─────┬──────┘                   │          │
                        │        ▼                          ▼          │
                        │  ┌─────────────────────────────────────┐    │
                        │  │ Silver (Delta): conformed schedule   │    │
                        │  │ dims + deduped, normalized telemetry │    │
                        │  └─────────────────┬───────────────────┘    │
                        │                    ▼                         │
                        │  ┌─────────────────────────────────────┐    │
                        │  │ Gold (Delta): schedule_deviation,    │    │
                        │  │ headway_actual_vs_planned, otp_by_   │    │
                        │  │ route_hour, bunching_events          │    │
                        │  └───────┬─────────────────────┬───────┘    │
                        │          ▼                     ▼            │
                        │   Direct Lake Power BI    Real-Time Dashboard│
                        │   (daily/route analytics) (KQL, live map)   │
                        └─────────────────────────────────────────────┘
```

### 2.1 Ingestion — Streaming
- **Azure Function (timer trigger, ~15 sec)** in Python:
  - Fetches bus VehiclePositions + TripUpdates protobuf, decodes with `gtfs-realtime-bindings`.
  - Polls the rail REST API and maps its JSON (TRAIN_ID, LINE, STATION, WAITING_SECONDS, lat/lon, DELAY) into the same canonical event schema.
  - Emits flattened JSON events to the **Eventstream custom endpoint** (Event Hub–compatible).
- **Canonical event schema** (the normalization layer is a headline design decision):
  `event_id, mode (bus|rail), vehicle_id, trip_id, route_id, stop_id, lat, lon, bearing, delay_seconds, event_ts, ingest_ts, source_feed`
- **Eventstream** routes to two destinations: **Eventhouse KQL DB** (hot path: live map + Activator) and **Lakehouse Bronze** (Delta append for the batch-join path).

### 2.2 Ingestion — Batch
- **Fabric Data Pipeline**, weekly schedule + manual trigger:
  1. Copy activity: download `google_transit.zip` → Lakehouse `Files/bronze/gtfs_static/{ingest_date}/`.
  2. Notebook: unzip, land each txt file as Bronze Delta with `ingest_date` and a **feed-version hash** (checksum of the zip). Only promote to Silver if the hash changed — a clean idempotent-batch pattern to showcase.
  3. Silver notebook: conform dims — `dim_route`, `dim_stop`, `dim_trip`, `fact_scheduled_stop_time` — with **SCD Type 2 on the feed version**, so historical deviations always join to the schedule that was in force at the time. *This is the project's core intellectual hook: slowly changing reference data joined to a fast stream.*

### 2.3 Silver — Stream Conformance (incremental)
- Spark Structured Streaming (or scheduled micro-batch with checkpoints) reads Bronze telemetry Delta:
  - Dedupe on `(vehicle_id, event_ts)`; late/out-of-order handling with watermarks.
  - Enrich with `dim_trip`/`dim_route` (broadcast join against the current schedule version).
  - Data-quality quarantine table for events with unknown `trip_id` — a real phenomenon (RT feeds referencing trips absent from static) and great blog material.

### 2.4 Gold — The Schedule-vs-Reality Join
- `fact_schedule_deviation`: observed arrival (from TripUpdates, or interpolated from vehicle position sequences) minus `fact_scheduled_stop_time.arrival_time`. Handle GTFS's **>24:00:00 clock times** and service-day logic from `calendar.txt` — a second genuinely non-trivial detail almost no demo covers.
- `fact_headway`: actual gap between consecutive vehicles on the same route/direction/stop vs. planned headway. Bunching = actual headway < 25% of planned.
- `agg_otp_route_hour`: on-time performance (within −1/+5 min) by route, hour, weekday.
- `fact_bunching_events`: materialized for the Activator alert and Power BI drill-through.

### 2.5 Serving
- **Real-Time Dashboard (KQL)**: live vehicle map, delay heat by rail line (RED/GOLD/BLUE/GREEN makes a naturally strong visual).
- **Power BI Direct Lake** on Gold: OTP trends, worst stops, bunching hot spots, schedule-version comparison ("did the latest service change improve Route 110?").
- **Activator**: rule on a derived stream — N bunching events on one route within 10 minutes → Teams/email alert.

---

## 3. Azure DevOps — Repo Layout

Single repo. Fabric workspace Git integration for dev; `fabric-cicd` for test/prod promotion.

```
marta-pulse/
├── README.md
├── azure-pipelines/
│   ├── ci-validate.yml           # PR: lint, unit tests, notebook static checks
│   ├── cd-fabric-deploy.yml      # main → test → prod via fabric-cicd
│   └── templates/
│       ├── deploy-stage.yml      # parameterized fabric-cicd stage
│       └── python-setup.yml
├── fabric/                        # Fabric workspace Git-sync root
│   ├── MartaPulse_LH.Lakehouse/
│   ├── MartaPulse_EH.Eventhouse/
│   ├── MartaPulse_KQLDB.KQLDatabase/
│   ├── MartaPulse_ES.Eventstream/
│   ├── PL_GTFS_Static_Ingest.DataPipeline/
│   ├── NB_Bronze_GTFS_Unzip.Notebook/
│   ├── NB_Silver_Schedule_Conform.Notebook/
│   ├── NB_Silver_Telemetry_Stream.Notebook/
│   ├── NB_Gold_Deviation.Notebook/
│   ├── NB_Gold_Headway_Bunching.Notebook/
│   ├── RTD_LiveOps.KQLDashboard/
│   ├── ACT_Bunching_Alert.Activator/
│   ├── SM_MartaPulse.SemanticModel/
│   └── RPT_MartaPulse.Report/
├── src/
│   └── marta_pulse/               # pip-installable lib (mirrors your FleetPulse wheel pattern)
│       ├── __init__.py
│       ├── canonical.py           # event schema + normalizers (bus proto → canonical, rail json → canonical)
│       ├── gtfs_static.py         # zip parsing, feed-version hashing, SCD2 helpers
│       ├── deviation.py           # service-day math, >24h times, deviation calc
│       └── quality.py             # quarantine rules
├── functions/
│   └── ingest_gtfs_rt/            # Azure Function app (timer trigger)
│       ├── function_app.py
│       ├── requirements.txt       # gtfs-realtime-bindings, azure-eventhub, requests
│       └── host.json
├── infra/
│   └── main.bicep                 # Function App, Key Vault (rail API key), Event Hub conn refs
├── tests/
│   ├── test_canonical.py
│   ├── test_deviation.py          # golden tests: >24:00 times, service-day edges
│   └── fixtures/                  # sample protobuf + rail JSON payloads
├── deploy/
│   ├── parameter.yml              # fabric-cicd env find/replace (lakehouse IDs, connection GUIDs)
│   └── config/
│       ├── dev.yml
│       ├── test.yml
│       └── prod.yml
└── docs/
    ├── architecture.md
    └── adr/                       # architecture decision records — great LinkedIn fodder
```

### CI/CD Flow
1. **PR → `ci-validate.yml`**: ruff + pytest on `src/` and `functions/`, build the `marta_pulse` wheel, validate notebook JSON structure.
2. **Merge to main → `cd-fabric-deploy.yml`**:
   - Stage 1: publish the wheel as a pipeline artifact; deploy the Function via Bicep + zip deploy.
   - Stage 2: `fabric-cicd` (`FabricWorkspace.publish_all_items()`) with service-principal auth deploys `fabric/` to the **test** workspace, with `parameter.yml` swapping lakehouse/connection GUIDs per environment.
   - Stage 3: manual approval gate → same deploy template targets the **prod** workspace.
3. Git integration is dev-only (feature branch ↔ dev workspace); test/prod are deploy-only workspaces — the pattern Microsoft recommends but few end-to-end demos actually show.

---

## 4. Build Order

1. **Week 1 — Batch spine:** static pipeline → Bronze → Silver dims with feed-version SCD2. Pytest golden tests for stop_times math.
2. **Week 2 — Stream:** Function decoding both feeds → Eventstream → Eventhouse + Bronze Delta. Live KQL map for instant demo gratification.
3. **Week 3 — The join:** Silver telemetry conformance, Gold deviation + headway. This is the flagship blog post.
4. **Week 4 — Ops:** Activator alert, Power BI, full DevOps promotion pipeline. Second post: "fabric-cicd end to end."

## 5. Content Angles Nobody Else Has

- **Batch as reference model, not history**: SCD2 schedule versions joined against a live stream.
- **Heterogeneous stream unification**: protobuf + bespoke REST → one canonical event schema.
- **GTFS gotchas as engineering content**: >24:00:00 times, service-day calendars, RT trips missing from static.
- **Real fabric-cicd multi-stage promotion** with parameterized connections — heavily searched, thinly documented.

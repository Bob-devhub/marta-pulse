# Lessons from Build #1

Every issue hit during the first end-to-end build, its root cause, and what
changed (in code or in process) so the rebuild doesn't repeat it.

## Data contracts & typing

1. **Type and normalize at the source, not downstream.** Rail `EVENT_TIME`
   shipped as a local-format string (`7/12/2026 9:14:25 AM`, unpadded hours)
   and broke Spark's strict parser deep in Silver. *Fixed:* `canonical.py`
   now converts rail timestamps to ISO-8601 UTC in the Function
   (`_rail_event_time_to_utc_iso`); `event_ts` is uniformly ISO UTC for all
   modes. Silver contains zero per-feed format knowledge.
2. **Type Bronze at the Eventstream destination.** `ingest_ts` landed as
   string, breaking `withWatermark` (needs timestamp). When configuring the
   Lakehouse destination, explicitly set `ingest_ts`/`event_ts` to datetime
   and `delay_seconds`/`stop_sequence` to int. Silver keeps a defensive
   `to_timestamp` (no-op when Bronze is typed right).
3. **Validate DQ rules against real feed behavior.** `has_vehicle` quarantined
   ALL 840k bus TripUpdates — MARTA's TripUpdates feed has no vehicle
   descriptor. *Fixed:* rule scoped to event types where vehicle identity is
   actually the key. Rule of thumb: run the quarantine breakdown query after
   the first hour of real data; a rule failing >1% of a category is probably
   wrong about the feed, not the data.
4. **SCD2 snapshot close-out** (from pre-build review): a key-matched MERGE
   leaves removed entities open forever. GTFS is a full snapshot — close ALL
   open rows, then append.
5. **Incremental windows on ingest time + MERGE on event_id** (from review):
   event-time high-water marks silently drop late arrivals; append-only
   reruns double-count. `NB_Gold_Deviation` windows on `ingest_ts_utc` and
   upserts on `event_id`.

## Fabric Eventstream

6. **Don't create a schema-enforced Eventstream** for a plain-JSON producer.
   Schema enforcement expects CloudEvents envelopes (silently drops
   non-conforming events — "CloudEvent property type is missing") and hides
   the Lakehouse destination. Use a standard Eventstream; the contract is
   enforced by the KQL table mapping and Silver DQ instead.
7. **Eventhouse destination: use Direct ingestion** with the `canonical_v1`
   mapping from `DatabaseSchema.kql` — never the "process before ingestion"
   Map-schema UI, whose type inference chokes on null-heavy columns.
8. **The custom endpoint's connection string** goes in Key Vault; only the
   `EntityPath` value (`es_..._eh`) is the entity name / Bicep parameter.

## Fabric Git integration

9. **Never hand-author `.platform` files.** A made-up `logicalId` blocks the
   workspace from committing the real item ("unable to commit"). Placeholder
   folders in `fabric/` should hold only a `PLACEHOLDER.md`, deleted before
   the workspace commits that item — or better, contain nothing at all.
10. **Initial sync direction: workspace → Git**, always, when the workspace
    has live UI-authored items. "Git → workspace" overrides (and can delete)
    live items.
11. **Items in Git but not in the workspace show up as staged *deletions*.**
    Committing them deletes from Git. Use **Undo** on those rows to restore
    the items from Git into the workspace instead.
12. **Commit every new item immediately** — especially the Lakehouse. An
    uncommitted Lakehouse got deleted by an Undo (workspace reverted to a Git
    state that lacked it) and took its data with it. Commit-early makes Git a
    restore point.
13. **Don't keep foreign files inside item folders** (schema docs, samples) —
    they live in `docs/`.

## Notebooks & Spark

14. **Schema-enabled Lakehouses don't auto-create schemas.**
    `CREATE SCHEMA IF NOT EXISTS bronze/silver/gold` before first write.
15. **A failed first write can leave an orphan catalog entry** (table exists
    in catalog, no Delta log → `DELTA_TABLE_NOT_FOUND` on append). Drop the
    table AND `rm` the `Tables/<schema>/<name>` folder, then rerun.
16. **Import order matters across notebook cells** — `F` was used a cell
    before its import. CI lints `fabric/**/notebook-content.py` (they're
    plain Python) to catch this.
17. **T-SQL endpoint vs Spark SQL:** `is_current = 1` in the SQL endpoint,
    `= true` in Spark.
18. **Placeholder GUIDs in notebook/pipeline definitions don't survive
    contact with a real workspace** — re-attach default lakehouses and
    re-select notebooks/connections in the pipeline editor after first sync;
    never paste expressions into a pipeline activity's Connection field
    (`@activity(...).output` resolved as a connection ID and failed the run).

## Azure / DevOps

19. **Consumption-plan quota** can be 0 on fresh subscriptions (App Service →
    Dynamic SKU, per region). Request 1–3 before deploying, or pick a region
    with quota.
20. **Key Vault RBAC:** grant yourself Secrets Officer before `secret set`;
    assignments take minutes to propagate; the Function reads secrets at
    startup, so restart after changing them.
21. **Linux Consumption has no SCM log stream** — use App Insights
    (`union traces, exceptions | order by timestamp desc`).
22. **`func publish` needs `--python`** (or a `local.settings.json` with
    `FUNCTIONS_WORKER_RUNTIME`) to detect the language.
23. **The wheel must be in the Function's requirements.txt at deploy time,
    but never committed with a pinned filename** — CD appends it. Same wheel
    must ALSO go to every Fabric Environment (fabric-cicd deploys items, not
    Environment libraries). Bump the version on every change —
    identical filenames make "did the new wheel take?" undiagnosable.
24. **Windows dev:** `tzdata` is required for `zoneinfo` (now a Windows-only
    dev dependency); use `python -m pip` in a venv, not bare `pip.exe`.
25. **Keep the git working copy OUT of OneDrive.** OneDrive file locks
    corrupted the git index mid-merge and served stale/truncated file reads.
    Clone to `C:\dev\...`; let ADO be the backup.

## Architecture revisions (post-build #1)

28. **Single Bronze write path via OneLake availability.** Two Eventstream
    destinations meant duplicate storage and CU burn on an F2 that was
    already throttling ("Large capacity delays"). Revised: Eventstream →
    Eventhouse only; enable OneLake availability on `raw_events`; surface it
    in the Lakehouse as a shortcut named `bronze.raw_events`. Typed
    columns come free from the KQL schema (`event_ts` is datetime now that
    the source normalizes it).
29. **Don't Structured-Stream over Kusto-mirrored Delta** — the service
    compacts/rewrites files, which streaming readers can't tolerate. Silver
    is batch-incremental: ingest-time window + lookback (> mirror latency,
    ~7 min observed) + anti-join on event_id for idempotency.
30. **Watch capacity on F2:** 15s polling ≈ 8.9 GB raw in a few days. If the
    capacity advisor complains, easy levers: drop redundant destinations
    (done), lengthen the Function timer to 30s, and cache/limit KQL
    dashboard auto-refresh.

## Feeds

26. **MARTA bus GTFS-RT URLs need the full `.pb` paths** (the developer page's
    link text is truncated): `.../vehicle/vehiclepositions.pb`,
    `.../tripupdate/tripupdates.pb`; GTFS static path is
    `google_transit_feed/google_transit.zip` (with `_feed`).
27. **~52% of stop events run early** (>1 min ahead) vs 2.6% late — schedule
    padding is the headline analytical finding, not a data bug.

## Build #2 — GitHub Actions + Fabric promotion

31. **CD triggers watch `main`.** A fix committed to a feature branch never
    deploys — and pushing the same tree from two different local folders
    creates unrelated histories on the remote. One working clone
    (`C:\dev\marta-pulse`), work on main or PR into it, period.
32. **GitHub OIDC subjects are ID-augmented** (2026 format):
    `repo:Owner@<accountId>/repo@<repoId>:ref:refs/heads/main`. Don't type
    the subject from docs — copy it VERBATIM from the failed `azure/login`
    log ("subject claim - ..."). One federated credential per trust path
    (branch push + each protected environment). In PowerShell,
    `--parameters "@file.json"` needs the `@`; in bash, inline JSON in
    single quotes is easier. AADSTS70025 = no credentials at all;
    "no matching record" = subject mismatch.
33. **fabric-cicd's current API requires `token_credential`** —
    `DefaultAzureCredential()` picks up the `azure/login` OIDC session on
    runners and `az login` locally. Pin or re-test on fabric-cicd upgrades;
    the constructor signature has changed before.
34. **Deleting a Function App orphans its role assignments.** The Bicep's
    deterministic `guid()` names then collide with the orphans on redeploy
    (`RoleAssignmentUpdateNotPermitted` — principalId can't be updated).
    Delete the stale assignments at each scope before redeploying a
    recreated identity.
35. **Y1 → FC1 is not an in-place update.** Delete the app AND plan, then
    deploy fresh. Watch the deploy action's log line "Detected function app
    sku:" — if it says Consumption, you're deploying to the old app, and on
    Linux Consumption the action skips the build (RUN_FROM_PACKAGE), so
    dependencies silently never install.
36. **Tenant setting scoped to a security group:** the SP must be a member
    of that group (object id, not app id), and membership takes ~15 min to
    propagate into Fabric.
37. **`parameter.yml` must live INSIDE the fabric-cicd repository directory**
    (`fabric/parameter.yml`) — anywhere else it's silently skipped
    ("Parameter file not found" is only a warning).
38. **`$items.<Type>.<Name>.id` only resolves for items the repo deploys.**
    Placeholder-only folders don't count. Either commit the real item
    definition, or pre-create an item with the SAME name in the target
    workspace — fabric-cicd matches by type + displayName and adopts it.
39. **Pipelines carry three GUID flavors:** (a) tenant-scoped connection ids
    — share the connection with the SP (Manage connections and gateways) or
    item creation fails with a generic POST error; (b) the lakehouse
    `artifactId` in linkedService, exported in byte-shuffled form that
    equals the item's logicalId — map that exact string in parameter.yml;
    (c) `workspaceId` of all zeros means "this workspace" and needs no
    mapping.
40. **fabric-cicd deploys items, not state.** Promoted lakehouses still need
    one-time provisioning: bronze/silver/gold schemas, the `raw_events`
    shortcut, and an Environment with the current wheel set as default.

41. **Changing a KQL table's column type invalidates its materialized
    views** ("incompatible with source table"). Drop and recreate the views
    (`.drop materialized-view` + `.create materialized-view`); use
    `backfill=false` so they restart from live data instead of reprocessing
    history on a small capacity.

42. **`_1`-suffixed duplicate columns in a KQL table** = a second ingestion
    path with inferred types got alter-merged in (Eventstream's Map-schema
    wizard strikes again — reinforces #7). Recover with rename →
    recreate clean from DatabaseSchema.kql → `.set-or-append` migrating
    history with `coalesce(col, typecast(col_1))` → re-add the destination
    as Direct ingestion with the existing mapping → recreate materialized
    views, re-enable OneLake availability, re-verify the Lakehouse shortcut.

43. **Recreating a Function App orphans its Key Vault role assignment.** The
    new managed identity has a different principalId; the old grant lingers
    and looks fine in `az role assignment list`. Compare
    `functionapp identity show --query principalId` against the assignee on
    the vault — if they differ, grant the new one (and delete the orphan).
44. **A green check on a Key Vault reference only means it resolved** — not
    that the secret's *content* is valid. "Connection string is either blank
    or malformed" persisted after the identity fix because the secret itself
    held a bad value. Setting the connection string directly as an app
    setting isolates reference-resolution from secret-content in one step;
    restore the reference (with a corrected secret) afterward.

## Build #3 — Databricks

45. **Serverless-only workspaces reject `job_clusters`** ("Only serverless
    compute is supported"). Asset Bundle jobs need `environments:` with a
    `spec.dependencies` list for wheels instead of the classic `libraries:`
    block, and no cluster definitions at all.
46. **`databricks bundle deploy` shells out to whatever `python` is on PATH**
    — not your venv. It needed `build` installed in that interpreter
    (Anaconda's, here).
47. **Always validate a downloaded archive before parsing it.** MARTA's site
    returns non-zip content to some egress networks with a default
    `python-requests` user-agent; the symptom was a bare `BadZipFile` deep in
    the library. Fix: set a real UA, `raise_for_status()`, check the `PK`
    magic bytes, and print status/content-type/length so the next failure is
    self-diagnosing.
48. **A second remote runs your workflows too.** Pushing the same tree to a
    mirror repo fired the Fabric/Function CD there, where none of the OIDC
    secrets exist — `azure/login` failed with "Not all values are present."
    GitHub does **not** expose the `secrets` context in a job-level `if`, so
    gate on the paired repo *variable* (`vars.FUNCTION_APP_NAME != ''`)
    instead: present where the job should run, absent everywhere else.
49. **Event Hubs' Kafka SASL rejects the *whole* connection string** with
    "not of the expected format / unexpected properties" for anything beyond
    `Endpoint` / `SharedAccessKeyName` / `SharedAccessKey`. A trailing
    `;EntityPath=` (Kafka gets the topic from `subscribe`), shell-captured
    quotes, or a newline all produce the same opaque error — and it surfaces
    as a `StreamingQueryException` a hundred stack frames deep. Sanitize the
    string in code and assert its shape up front; secrets are redacted from
    output, so print derived facts (length, key names, dropped keys) instead.
    Corollary: a placeholder pasted verbatim from documentation stores
    happily and fails only at the far end of the pipeline. Never hand-type a
    secret — pipe it from `az ... --query primaryConnectionString -o tsv`
    straight into `databricks secrets put-secret`, and validate on read.
50. **Rail never reaches Gold — a silent inner-join drop.** `normalize_rail`
    sets `stop_id` to the station *name* and leaves `trip_id` null, but the
    deviation join is `trip_id AND stop_id`, inner. Every rail row vanishes
    with no error and no quarantine row; the only symptom is that
    `GROUP BY mode` on the fact table returns one row. Both builds had it.
    Lesson: when a pipeline fans in multiple feeds, assert per-feed row
    counts at every layer boundary — a feed reaching zero is invisible
    otherwise, because "no rows" is not an error anywhere in Spark.
51. **`trip_update` events are predictions, not observations.** Their
    `event_ts` is a forecast arrival, legitimately ahead of wall clock (605k
    future-dated rows here). OTP computed over them measures *predicted*
    punctuality — what the agency expects, not what happened. Comparing an
    afternoon of predictions against a multi-day archive that included
    overnight service produced a 27-point swing in "early" and looked like a
    regression; it was population mix. Always pin the window and the event
    mix before treating a metric shift as a bug.
52. **A join that needs direction, without direction, is worse than no
    join.** Rail stations have one platform stop per direction, so matching
    a train to the nearest scheduled arrival *ignoring* direction happily
    scores a northbound train against a southbound trip — small deviations,
    plausible dashboard, meaningless numbers. The feed had `DIRECTION` all
    along; `normalize_rail` was discarding it. It now rides in on `bearing`,
    an existing canonical field that already means "direction of travel", so
    no schema change and no Eventstream/KQL churn.
53. **Infer agency conventions from data, don't hardcode them.** What GTFS
    `direction_id` 0 and 1 mean geographically is not in the spec. Rather
    than assume, derive it per route from the net latitude/longitude change
    between each trip's first and last stop. Self-correcting across feed
    versions, and it fails loudly rather than silently if a route changes.
54. **Nearest-neighbour matching has a ceiling, and it's half the headway.**
    Assigning an observation to its closest scheduled arrival can never
    report a delay larger than half the headway — a train 12 minutes late on
    a 10-minute headway reads as 2 minutes early against the *next* trip. So
    the metric degrades exactly when service is worst. Emit
    `match_method` / `match_ambiguous` / `match_direction_known` and keep
    inferred rows separable from measured ones; never average an assignment
    into a measurement.
55. **Serverless blocks `spark.conf.set` for Delta schema evolution.**
    `spark.databricks.delta.schema.autoMerge.enabled` raises
    `CONFIG_NOT_AVAILABLE` outright, and `.option("mergeSchema", true)` does
    not apply to `MERGE`. Add the columns explicitly with an idempotent
    `ALTER TABLE ... ADD COLUMNS` guarded by a `columns` check — it works on
    every compute type and makes the schema change reviewable in code.
    Related: two tasks that MERGE into the same Delta table must be chained,
    not parallel, or they race on both the ALTER and the commit.
56. **Validate a derived metric against an independent source before
    publishing it — the derivation ran fine and was still wrong.** The
    nearest-scheduled rail match produced 27,696 clean rows with a plausible
    distribution. Correlated against MARTA's own published DELAY it scored
    **0.006**: the agency measured ~103s average delay, we computed ~1s.
    The method was answering "how close is the nearest scheduled train",
    not "how late is this train", and no amount of inspecting our own output
    would have revealed that — only the outside check did. A tight, tidy
    distribution is a warning sign, not a success signal: real operations
    are messy, and p5–p95 inside ±2 minutes should have been implausible on
    its face.
57. **Prefer the operator's own metric over one you reconstruct.** MARTA
    computes DELAY against trip identity that the realtime feed never
    exposes. No cleverness on our side recovers information the source
    withheld. The right move was to delete the derivation and read the
    field that was in the payload the whole time — and to `DELETE` the rows
    the retired method wrote, since `MERGE ... whenNotMatchedInsertAll`
    leaves discredited data in place forever.
58. **`a or b` on a timestamp is a silent lie.** `event_ts=_epoch_to_iso(
    best.get("time") or feed_ts)` looked like sensible defensiveness. But a
    GTFS-RT `StopTimeUpdate` may legally carry neither an absolute time nor
    a delay, and for those rows `event_ts` quietly stopped meaning
    "predicted arrival" and started meaning "when we polled" — with nothing
    in the record marking the difference. Gold then scored a 20:44 poll
    against a 00:35 scheduled arrival and reported the bus 3.8 hours early:
    235k rows, 12% of the fact table. Null is honest; a plausible wrong
    value is not. Fallbacks are only safe between values that mean the
    same thing.
59. **Check whether the fix that worked last time even applies.** After
    rail was solved by preferring the agency's own `delay`, the obvious move
    was to do the same for bus. One query first: `delay` is NULL on all
    4.3M bus rows — MARTA doesn't populate it in TripUpdates. The symmetric
    fix was unavailable, and assuming it would have quietly produced an
    all-null metric.
60. **Contamination that can't be distinguished must be fenced off by
    time.** Pre-fix rows are indistinguishable from good ones in Silver, so
    "rebuild from Silver" would have faithfully reproduced the bug. Hence
    the `min_ingest_ts` floor: rebuild only from data ingested after the
    producer was fixed, and accept the gap rather than launder bad rows
    through a recompute.
61. **Two builds shipped under one version number.** The rail fix and the
    bus fix were separate edits with no version bump between them, so both
    wheels were "0.3.0". The rail change was deployed; the bus change wasn't;
    and `bearing IS NOT NULL` — used as a proxy for "0.3.0 is live" —
    confirmed the wrong one. The tell was chronological: the cutover
    timestamp (20:09) predated bad rows (20:43+), which is impossible if the
    fix were live. Bump the version on every change that alters behaviour,
    however small, and prefer a marker carried *in the payload*
    (`schema_version`) over one inferred from a field's presence: it states
    the contract instead of implying it, works retroactively on stored data,
    and makes the downstream filter self-clearing rather than a hand-entered
    cutover time.
62. **The project's headline finding was the bug.** Build #1 concluded
    "52% early / 2.6% late — MARTA pads its schedules". That was the
    poll-timestamp fallback (LESSON #58) at scale: a poll time necessarily
    precedes the scheduled arrival of every upcoming stop, so the artifact
    manufactures "early" and nothing else. Corrected, the same feed reports
    median +36s with a late tail at evening peak — the opposite conclusion.
    A result that flatters a neat narrative deserves the most scrutiny, not
    the least, and "52% early" should have been interrogated the moment it
    appeared rather than written up. The giveaway was in the data all along:
    the early tail had no counterpart late tail, and real operations are
    roughly symmetric around a positive median.
63. **Same wheel, same feed_version across platforms** (`9f6554cafaa7903f` in
    both Fabric and Databricks) — the portability of the canonical core is
    verifiable, not just claimed.

*Standing practice: every issue hit and resolved in this project gets an
entry here, in the same commit as (or right after) the fix.*

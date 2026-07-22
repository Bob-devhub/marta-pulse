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

*Standing practice: every issue hit and resolved in this project gets an
entry here, in the same commit as (or right after) the fix.*

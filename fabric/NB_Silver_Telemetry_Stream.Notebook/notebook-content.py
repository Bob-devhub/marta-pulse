# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a1710f51-a81f-4435-ba96-883ba5514618",
# META       "default_lakehouse_name": "MartaPulse_LH",
# META       "default_lakehouse_workspace_id": "78970af8-720e-41cf-8075-553799bdcdd3",
# META       "known_lakehouses": [
# META         {
# META           "id": "a1710f51-a81f-4435-ba96-883ba5514618"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

# NB_Silver_Telemetry_Stream — batch-incremental conformance
# -----------------------------------------------------------
# Bronze is now a SHORTCUT to the Eventhouse raw_events table (OneLake
# availability): one write path, typed columns for free, and no duplicate
# storage/CU for a second Eventstream destination.
#
# Batch-incremental (NOT Structured Streaming): Kusto's mirrored Delta is
# compacted/rewritten by the service, which streaming readers can't handle.
# Incremental window on ingest_ts with a lookback buffer; idempotency via
# anti-join on event_id — reruns and window overlaps are safe.
#
# Run from PL_Telemetry_Refresh every 15-30 minutes.

from pyspark.sql import functions as F

from marta_pulse.quality import failed_rules_expr

BRONZE = "bronze.raw_events"            # shortcut -> Eventhouse raw_events
SILVER = "silver.telemetry_conformed"
QUARANTINE = "silver.telemetry_quarantine"
LOOKBACK = "90 minutes"                 # > OneLake availability latency + run cadence

dim_trip_current = (
    spark.table("silver.dim_trip").where("is_current = true")
    .select("trip_id", F.col("route_id").alias("sched_route_id"), "service_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Incremental window: everything ingested since (hwm - lookback). The
# lookback absorbs OneLake mirroring latency; the anti-join below removes
# rows already processed, so overlap never double-writes.
def table_hwm(table: str):
    if not spark.catalog.tableExists(table):
        return None
    return spark.table(table).agg(F.max("ingest_ts")).first()[0]

hwms = [h for h in (table_hwm(SILVER), table_hwm(QUARANTINE)) if h is not None]
hwm = max(hwms) if hwms else None

batch = spark.table(BRONZE).withColumn("ingest_ts", F.to_timestamp("ingest_ts"))
if hwm is not None:
    batch = batch.where(
        F.col("ingest_ts") > F.lit(hwm) - F.expr(f"INTERVAL {LOOKBACK}")
    )

batch = batch.dropDuplicates(["event_id"])

# Idempotency: drop anything already conformed or quarantined in the window.
for tbl in (SILVER, QUARANTINE):
    if spark.catalog.tableExists(tbl):
        seen = spark.table(tbl).select("event_id")
        if hwm is not None:
            seen = spark.table(tbl).where(
                F.col("ingest_ts") > F.lit(hwm) - F.expr(f"INTERVAL {LOOKBACK}")
            ).select("event_id")
        batch = batch.join(seen, "event_id", "left_anti")

print(f"incremental batch rows: {batch.count()} (hwm={hwm})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Conform: event_ts is ISO UTC for ALL modes (normalized at the source by
# marta_pulse.canonical >=0.2.0) and already datetime in the KQL schema —
# to_timestamp is a defensive no-op on typed columns.
enriched = (
    batch
    .withColumn("event_ts_utc", F.to_timestamp("event_ts"))
    .join(F.broadcast(dim_trip_current), "trip_id", "left")
    .withColumn("trip_known", F.col("service_id").isNotNull())
    .withColumn("failed_rules", F.expr(failed_rules_expr()))
)

passed = enriched.where(F.size("failed_rules") == 0).drop("failed_rules")
failed = enriched.where(F.size("failed_rules") > 0)

(passed.write.format("delta").mode("append")
    .option("mergeSchema", "true").saveAsTable(SILVER))
(failed.write.format("delta").mode("append")
    .option("mergeSchema", "true").saveAsTable(QUARANTINE))

print("silver telemetry conformance run complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

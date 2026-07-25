# Databricks notebook source
# Batch-incremental Silver conformance — direct port of the Fabric notebook.
# Bronze here is a native Delta table written by bronze_telemetry_stream
# (typed at the source), so no shortcut/mirror latency: lookback is modest.

from pyspark.sql import functions as F

from marta_pulse.quality import failed_rules_expr

dbutils.widgets.text("catalog", "marta_pulse")
CATALOG = dbutils.widgets.get("catalog")

BRONZE = f"{CATALOG}.bronze.raw_events"
SILVER = f"{CATALOG}.silver.telemetry_conformed"
QUARANTINE = f"{CATALOG}.silver.telemetry_quarantine"
LOOKBACK = "30 minutes"

dim_trip_current = (
    spark.table(f"{CATALOG}.silver.dim_trip").where("is_current = true")
    .select("trip_id", F.col("route_id").alias("sched_route_id"), "service_id")
)

# COMMAND ----------

def table_hwm(table: str):
    if not spark.catalog.tableExists(table):
        return None
    return spark.table(table).agg(F.max("ingest_ts")).first()[0]

hwms = [h for h in (table_hwm(SILVER), table_hwm(QUARANTINE)) if h is not None]
hwm = max(hwms) if hwms else None

batch = spark.table(BRONZE)
if hwm is not None:
    batch = batch.where(
        F.col("ingest_ts") > F.lit(hwm) - F.expr(f"INTERVAL {LOOKBACK}")
    )

batch = batch.dropDuplicates(["event_id"])

for tbl in (SILVER, QUARANTINE):
    if spark.catalog.tableExists(tbl):
        seen = spark.table(tbl)
        if hwm is not None:
            seen = seen.where(
                F.col("ingest_ts") > F.lit(hwm) - F.expr(f"INTERVAL {LOOKBACK}")
            )
        batch = batch.join(seen.select("event_id"), "event_id", "left_anti")

print(f"incremental batch rows: {batch.count()} (hwm={hwm})")

# COMMAND ----------

enriched = (
    batch
    .withColumn("event_ts_utc", F.col("event_ts"))   # already timestamp, UTC
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

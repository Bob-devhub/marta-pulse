# Databricks notebook source
# THE schedule-vs-reality join — direct port of NB_Gold_Deviation.
# Ingest-time incremental window + MERGE on event_id (idempotent).

from datetime import timezone

from pyspark.sql import functions as F

from marta_pulse.deviation import (
    ON_TIME_EARLY_SECONDS,
    ON_TIME_LATE_SECONDS,
    scheduled_instant_utc,
    service_date_for,
)

dbutils.widgets.text("catalog", "marta_pulse")
CATALOG = dbutils.widgets.get("catalog")

# Rows ingested before wheel 0.3.0 may carry the poll timestamp in event_ts
# instead of a predicted arrival (LESSON #58), and the two are
# indistinguishable after the fact. Set this to the moment the Function was
# upgraded so the rebuild can't re-ingest contaminated history; leave empty
# once no pre-0.3.0 rows remain in Silver.
dbutils.widgets.text("min_ingest_ts", "")
MIN_INGEST_TS = dbutils.widgets.get("min_ingest_ts").strip()

FACT = f"{CATALOG}.gold.fact_schedule_deviation"
AGG = f"{CATALOG}.gold.agg_otp_route_hour"

# The match_* columns were added after the table was first created. MERGE
# won't evolve the target schema, and serverless refuses
# spark.databricks.delta.schema.autoMerge.enabled outright, so add them
# explicitly and idempotently before writing (LESSON #56).
MATCH_COLUMNS = {
    "match_method": "STRING",
    "match_ambiguous": "BOOLEAN",
    "match_direction_known": "BOOLEAN",
}


def ensure_match_columns(table: str) -> None:
    if not spark.catalog.tableExists(table):
        return
    existing = set(spark.table(table).columns)
    missing = {c: t for c, t in MATCH_COLUMNS.items() if c not in existing}
    if missing:
        cols = ", ".join(f"{c} {t}" for c, t in missing.items())
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({cols})")
        print(f"added columns to {table}: {sorted(missing)}")


ensure_match_columns(FACT)

service_date_udf = F.udf(
    lambda ts: service_date_for(ts.replace(tzinfo=timezone.utc)) if ts else None,
    "date",
)
sched_instant_udf = F.udf(
    lambda d, s: scheduled_instant_utc(d, s) if d is not None and s is not None else None,
    "timestamp",
)

# COMMAND ----------

if spark.catalog.tableExists(FACT):
    # Scoped to bus: gold_rail_deviation writes to the same table afterwards,
    # and an unscoped max would advance the watermark past bus rows that were
    # never processed.
    hwm = (
        spark.table(FACT).where("mode = 'bus'")
        .agg(F.max("ingest_ts_utc")).first()[0]
    )
else:
    hwm = None

obs = (
    spark.table(f"{CATALOG}.silver.telemetry_conformed")
    .where("event_type = 'trip_update' AND stop_id IS NOT NULL")
    # Rail is handled by gold_rail_deviation from the agency's own DELAY.
    .where("event_ts_utc IS NOT NULL")
    # Schema 1.0 rows may carry the poll timestamp in event_ts instead of a
    # predicted arrival, and the two are indistinguishable after the fact
    # (LESSON #58). Filtering on the payload's own contract version is
    # deterministic and self-clearing — unlike a hand-entered cutover time.
    .where("schema_version <> '1.0'")
)
if MIN_INGEST_TS:
    obs = obs.where(F.col("ingest_ts") >= F.lit(MIN_INGEST_TS).cast("timestamp"))
    print(f"floor: only observations ingested at/after {MIN_INGEST_TS}")
obs = (
    obs
    .withColumn("ingest_ts_utc", F.col("ingest_ts"))
)
if hwm:
    obs = obs.where(F.col("ingest_ts_utc") > F.lit(hwm))

obs = obs.withColumn("service_date", service_date_udf("event_ts_utc"))

# COMMAND ----------

sched = spark.table(f"{CATALOG}.silver.fact_scheduled_stop_time").alias("s")

deviation = (
    obs.alias("o")
    .join(
        sched,
        (F.col("o.trip_id") == F.col("s.trip_id"))
        & (F.col("o.stop_id") == F.col("s.stop_id"))
        # 1,443 trip/stop pairs are served twice (loops, or a stop passed in
        # both directions). Without stop_sequence the join picked one of the
        # two scheduled times arbitrarily. The feed doesn't always supply it,
        # so require equality only when it does.
        & (
            F.col("o.stop_sequence").isNull()
            | (F.col("o.stop_sequence") == F.col("s.stop_sequence"))
        )
        & (F.col("o.service_date") >= F.col("s.effective_from"))
        & (
            F.col("s.effective_to").isNull()
            | (F.col("o.service_date") <= F.col("s.effective_to"))
        ),
        "inner",
    )
    .withColumn(
        "scheduled_ts_utc",
        sched_instant_udf(F.col("o.service_date"), F.col("s.arrival_seconds")),
    )
    .withColumn(
        "deviation_seconds",
        F.col("o.event_ts_utc").cast("long") - F.col("scheduled_ts_utc").cast("long"),
    )
    .withColumn(
        "otp_bucket",
        F.when(F.col("deviation_seconds") < ON_TIME_EARLY_SECONDS, "early")
        .when(F.col("deviation_seconds") <= ON_TIME_LATE_SECONDS, "on_time")
        .otherwise("late"),
    )
    # Bus joins on a real trip_id; rail is assigned to its nearest scheduled
    # arrival (gold_rail_deviation). Tagging both keeps the two from being
    # averaged together as if they were the same measurement.
    .withColumn("match_method", F.lit("trip_id"))
    .withColumn("match_ambiguous", F.lit(False))
    .withColumn("match_direction_known", F.lit(True))
    .select(
        "o.event_id", "o.mode", "o.vehicle_id", "o.trip_id",
        F.coalesce("o.route_id", "o.sched_route_id").alias("route_id"),
        "o.stop_id", "o.service_date", "o.event_ts_utc", "o.ingest_ts_utc",
        "scheduled_ts_utc", "deviation_seconds", "otp_bucket",
        F.col("s.feed_version").alias("schedule_version"),
        "match_method", "match_ambiguous", "match_direction_known",
    )
)

if not spark.catalog.tableExists(FACT):
    deviation.write.format("delta").option("mergeSchema", "true").saveAsTable(FACT)
else:
    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, FACT).alias("t")
        .merge(deviation.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
print(f"merged deviations; ingest hwm was {hwm}")

# COMMAND ----------

(
    spark.table(FACT)
    .groupBy("route_id", "service_date", F.hour("event_ts_utc").alias("hour_utc"))
    .agg(
        F.count("*").alias("observations"),
        F.avg("deviation_seconds").alias("avg_deviation_s"),
        F.expr("percentile_approx(deviation_seconds, 0.9)").alias("p90_deviation_s"),
        (F.sum(F.when(F.col("otp_bucket") == "on_time", 1).otherwise(0)) / F.count("*"))
        .alias("otp_ratio"),
    )
    .write.format("delta").mode("overwrite").saveAsTable(AGG)
)
print("agg_otp_route_hour rebuilt")

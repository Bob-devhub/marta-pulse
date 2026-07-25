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

FACT = f"{CATALOG}.gold.fact_schedule_deviation"
AGG = f"{CATALOG}.gold.agg_otp_route_hour"

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
    hwm = spark.table(FACT).agg(F.max("ingest_ts_utc")).first()[0]
else:
    hwm = None

obs = (
    spark.table(f"{CATALOG}.silver.telemetry_conformed")
    .where("event_type IN ('trip_update','rail_arrival') AND stop_id IS NOT NULL")
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
    .select(
        "o.event_id", "o.mode", "o.vehicle_id", "o.trip_id",
        F.coalesce("o.route_id", "o.sched_route_id").alias("route_id"),
        "o.stop_id", "o.service_date", "o.event_ts_utc", "o.ingest_ts_utc",
        "scheduled_ts_utc", "deviation_seconds", "otp_bucket",
        F.col("s.feed_version").alias("schedule_version"),
    )
)

if not spark.catalog.tableExists(FACT):
    deviation.write.format("delta").saveAsTable(FACT)
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

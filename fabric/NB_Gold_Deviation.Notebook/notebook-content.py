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

# NB_Gold_Deviation — THE schedule-vs-reality join.
# ---------------------------------------------------
# For each observed (trip, stop) event, find the schedule that was IN
# FORCE at observation time (SCD2 dims!) and compute deviation seconds.
# Handles: >24:00:00 scheduled times, service-day rollover at 03:00 local,
# and DST via the noon-minus-12h anchor (marta_pulse.deviation).

from datetime import datetime, timezone

from pyspark.sql import functions as F

from marta_pulse.deviation import (
    ON_TIME_EARLY_SECONDS,
    ON_TIME_LATE_SECONDS,
    scheduled_instant_utc,
    service_date_for,
)

service_date_udf = F.udf(
    lambda ts: service_date_for(ts.replace(tzinfo=timezone.utc)) if ts else None,
    "date",
)
sched_instant_udf = F.udf(
    lambda d, s: scheduled_instant_utc(d, s) if d is not None and s is not None else None,
    "timestamp",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Incremental window on INGEST time, not event time: late/out-of-order
# events carry an old event_ts but a fresh ingest_ts, so an event-time
# high-water mark would silently drop them. MERGE on event_id (below)
# makes reruns idempotent even if the window overlaps.
if spark.catalog.tableExists("gold.fact_schedule_deviation"):
    hwm = spark.table("gold.fact_schedule_deviation").agg(
        F.max("ingest_ts_utc")
    ).first()[0]
else:
    hwm = None

obs = (
    spark.table("silver.telemetry_conformed")
    .where("event_type IN ('trip_update','rail_arrival') AND stop_id IS NOT NULL")
    .withColumn("ingest_ts_utc", F.to_timestamp("ingest_ts"))
)
if hwm:
    obs = obs.where(F.col("ingest_ts_utc") > F.lit(hwm))

obs = obs.withColumn("service_date", service_date_udf("event_ts_utc"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Join to the schedule version in force on the observation's service date.
sched = spark.table("silver.fact_scheduled_stop_time").alias("s")

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

# Idempotent upsert: reruns / overlapping windows can't double-count.
if not spark.catalog.tableExists("gold.fact_schedule_deviation"):
    deviation.write.format("delta").saveAsTable("gold.fact_schedule_deviation")
else:
    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, "gold.fact_schedule_deviation").alias("t")
        .merge(deviation.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
print(f"merged deviations; ingest hwm was {hwm}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# agg_otp_route_hour — Direct Lake-friendly aggregate for Power BI
(
    spark.table("gold.fact_schedule_deviation")
    .groupBy(
        "route_id",
        "service_date",
        F.hour("event_ts_utc").alias("hour_utc"),
    )
    .agg(
        F.count("*").alias("observations"),
        F.avg("deviation_seconds").alias("avg_deviation_s"),
        F.expr("percentile_approx(deviation_seconds, 0.9)").alias("p90_deviation_s"),
        (F.sum(F.when(F.col("otp_bucket") == "on_time", 1).otherwise(0)) / F.count("*"))
        .alias("otp_ratio"),
    )
    .write.format("delta").mode("overwrite").saveAsTable("gold.agg_otp_route_hour")
)
print("gold.agg_otp_route_hour rebuilt")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Databricks notebook source
# Actual vs planned headway per (route, direction, stop); bunching when
# actual < 25% of planned. Direct port of NB_Gold_Headway_Bunching.

from pyspark.sql import Window
from pyspark.sql import functions as F

from marta_pulse.deviation import BUNCHING_HEADWAY_RATIO

dbutils.widgets.text("catalog", "marta_pulse")
CATALOG = dbutils.widgets.get("catalog")


def t(name: str) -> str:
    return f"{CATALOG}.{name}"


dim_trip = (
    spark.table(t("silver.dim_trip")).where("is_current = true")
    .select("trip_id", "direction_id")
)

arrivals = (
    spark.table(t("gold.fact_schedule_deviation"))
    .join(dim_trip, "trip_id", "left")
    .withColumn("direction_id", F.coalesce("direction_id", F.lit(0)))
)

key = ["route_id", "direction_id", "stop_id", "service_date"]

# COMMAND ----------

w_actual = Window.partitionBy(*key).orderBy("event_ts_utc")
actual = (
    arrivals
    .withColumn("prev_ts", F.lag("event_ts_utc").over(w_actual))
    .withColumn(
        "actual_headway_s",
        F.col("event_ts_utc").cast("long") - F.col("prev_ts").cast("long"),
    )
    .where("actual_headway_s IS NOT NULL AND actual_headway_s BETWEEN 30 AND 7200")
)

sched = (
    spark.table(t("silver.fact_scheduled_stop_time")).where("is_current = true")
    .join(spark.table(t("silver.dim_trip")).where("is_current = true")
          .select("trip_id", "route_id", "direction_id"), "trip_id")
)
w_planned = Window.partitionBy("route_id", "direction_id", "stop_id").orderBy("arrival_seconds")
planned = (
    sched
    .withColumn("prev_arr", F.lag("arrival_seconds").over(w_planned))
    .withColumn("planned_headway_s", F.col("arrival_seconds") - F.col("prev_arr"))
    .where("planned_headway_s IS NOT NULL AND planned_headway_s > 0")
    .groupBy("route_id", "direction_id", "stop_id")
    .agg(F.expr("percentile_approx(planned_headway_s, 0.5)").alias("planned_headway_s"))
)

# COMMAND ----------

headway = (
    actual.join(planned, ["route_id", "direction_id", "stop_id"], "left")
    .withColumn(
        "is_bunched",
        F.when(
            F.col("planned_headway_s").isNotNull()
            & (F.col("actual_headway_s")
               < F.col("planned_headway_s") * F.lit(BUNCHING_HEADWAY_RATIO)),
            True,
        ).otherwise(False),
    )
    .select(
        *key, "vehicle_id", "event_ts_utc",
        "actual_headway_s", "planned_headway_s", "is_bunched",
    )
)

headway.write.format("delta").mode("overwrite").saveAsTable(t("gold.fact_headway"))

(
    headway.where("is_bunched")
    .write.format("delta").mode("overwrite")
    .saveAsTable(t("gold.fact_bunching_events"))
)

n = headway.where("is_bunched").count()
print(f"gold.fact_headway rebuilt; {n} bunching events")

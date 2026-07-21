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

# NB_Gold_Headway_Bunching
# -------------------------
# Actual headway: gap between consecutive observed arrivals at the same
# (route, direction, stop). Planned headway: gap between consecutive
# scheduled arrivals for the same key. Bunching: actual < 25% of planned.

from pyspark.sql import Window
from pyspark.sql import functions as F

from marta_pulse.deviation import BUNCHING_HEADWAY_RATIO

dim_trip = (
    spark.table("silver.dim_trip").where("is_current = true")
    .select("trip_id", "direction_id")
)

arrivals = (
    spark.table("gold.fact_schedule_deviation")
    .join(dim_trip, "trip_id", "left")
    .withColumn("direction_id", F.coalesce("direction_id", F.lit(0)))
)

key = ["route_id", "direction_id", "stop_id", "service_date"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Actual headways
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

# Planned headways from the schedule in force
sched = (
    spark.table("silver.fact_scheduled_stop_time").where("is_current = true")
    .join(spark.table("silver.dim_trip").where("is_current = true")
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

headway.write.format("delta").mode("overwrite").saveAsTable("gold.fact_headway")

(
    headway.where("is_bunched")
    .write.format("delta").mode("overwrite")
    .saveAsTable("gold.fact_bunching_events")
)

n = headway.where("is_bunched").count()
print(f"gold.fact_headway rebuilt; {n} bunching events")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

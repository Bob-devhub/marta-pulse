# Databricks notebook source
# Bronze gtfs_* (strings) -> typed Silver dims with SCD2 on feed_version.
# Direct port of NB_Silver_Schedule_Conform; only naming and dbutils differ.

from pyspark.sql import functions as F

from marta_pulse.deviation import parse_gtfs_time
from marta_pulse.gtfs_static import scd2_merge, version_already_loaded

dbutils.widgets.text("catalog", "marta_pulse")
CATALOG = dbutils.widgets.get("catalog")


def t(name: str) -> str:
    return f"{CATALOG}.{name}"


latest = (
    spark.table(t("bronze.gtfs_feed_registry"))
    .orderBy(F.col("ingest_date").desc())
    .first()
)
VERSION, EFFECTIVE = latest["feed_version"], str(latest["ingest_date"])
print(f"conforming feed_version={VERSION} effective={EFFECTIVE}")

if version_already_loaded(spark, t("silver.dim_route"), VERSION):
    dbutils.notebook.exit(f"NOOP: {VERSION} already conformed")


def bronze_version(table: str):
    return spark.table(t(table)).where(F.col("feed_version") == VERSION)

# COMMAND ----------

dim_route = bronze_version("bronze.gtfs_routes").select(
    "route_id", "route_short_name", "route_long_name",
    F.col("route_type").cast("int").alias("route_type"),
)
scd2_merge(spark, dim_route, t("silver.dim_route"), ["route_id"], VERSION, EFFECTIVE)

dim_stop = bronze_version("bronze.gtfs_stops").select(
    "stop_id", "stop_name",
    F.col("stop_lat").cast("double").alias("stop_lat"),
    F.col("stop_lon").cast("double").alias("stop_lon"),
)
scd2_merge(spark, dim_stop, t("silver.dim_stop"), ["stop_id"], VERSION, EFFECTIVE)

dim_trip = bronze_version("bronze.gtfs_trips").select(
    "trip_id", "route_id", "service_id",
    F.col("direction_id").cast("int").alias("direction_id"),
    "shape_id",
)
scd2_merge(spark, dim_trip, t("silver.dim_trip"), ["trip_id"], VERSION, EFFECTIVE)

# COMMAND ----------

parse_udf = F.udf(parse_gtfs_time, "int")

sched = bronze_version("bronze.gtfs_stop_times").select(
    "trip_id", "stop_id",
    F.col("stop_sequence").cast("int").alias("stop_sequence"),
    parse_udf(F.col("arrival_time")).alias("arrival_seconds"),
    parse_udf(F.col("departure_time")).alias("departure_seconds"),
)
scd2_merge(
    spark, sched, t("silver.fact_scheduled_stop_time"),
    ["trip_id", "stop_sequence"], VERSION, EFFECTIVE,
)

# COMMAND ----------

cal = bronze_version("bronze.gtfs_calendar")
days = (
    cal.select(
        "service_id",
        F.explode(
            F.sequence(
                F.to_date("start_date", "yyyyMMdd"),
                F.to_date("end_date", "yyyyMMdd"),
            )
        ).alias("service_date"),
        *[F.col(d).cast("int").alias(d) for d in
          ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]],
    )
    .withColumn("dow", F.dayofweek("service_date"))
    .withColumn(
        "runs",
        F.expr(
            "CASE dow WHEN 1 THEN sunday WHEN 2 THEN monday WHEN 3 THEN tuesday "
            "WHEN 4 THEN wednesday WHEN 5 THEN thursday WHEN 6 THEN friday "
            "WHEN 7 THEN saturday END"
        ),
    )
    .where("runs = 1")
    .select("service_id", "service_date")
)

exceptions = bronze_version("bronze.gtfs_calendar_dates").select(
    "service_id",
    F.to_date("date", "yyyyMMdd").alias("service_date"),
    F.col("exception_type").cast("int").alias("exception_type"),
)
added = exceptions.where("exception_type = 1").select("service_id", "service_date")
removed = exceptions.where("exception_type = 2").select("service_id", "service_date")

dim_service_day = days.union(added).exceptAll(removed).distinct()
scd2_merge(
    spark, dim_service_day, t("silver.dim_service_day"),
    ["service_id", "service_date"], VERSION, EFFECTIVE,
)

print("Silver schedule conformance complete")

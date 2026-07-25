# Databricks notebook source
# Rail schedule deviation — from MARTA's own published DELAY, not from a
# schedule join.
#
# History worth keeping (LESSONS #50, #54, #57): rail has no trip_id, so the
# first fix matched each observation to its nearest scheduled arrival. It
# ran, produced 27,696 plausible rows, and was wrong. Validated against the
# agency's own DELAY field the correlation was 0.006 — nil. The method
# reports "time to the closest scheduled train", which collapses toward zero
# by construction, and 95% of matches were flagged ambiguous. MARTA computes
# delay against the trip identity we don't have, so the agency figure is
# strictly better information than anything we can reconstruct.
#
# What remains from that work: the station-name -> stop_id crosswalk, which
# is deterministic and still needed to attribute observations to stops.

from pyspark.sql import functions as F

from marta_pulse.deviation import ON_TIME_EARLY_SECONDS, ON_TIME_LATE_SECONDS
from marta_pulse.rail_match import normalize_station

dbutils.widgets.text("catalog", "marta_pulse")
CATALOG = dbutils.widgets.get("catalog")

FACT = f"{CATALOG}.gold.fact_schedule_deviation"
COVERAGE = f"{CATALOG}.gold.rail_match_coverage"

norm_udf = F.udf(normalize_station, "string")

MATCH_COLUMNS = {
    "match_method": "STRING",
    "match_ambiguous": "BOOLEAN",
    "match_direction_known": "BOOLEAN",
}
if spark.catalog.tableExists(FACT):
    _missing = {c: t for c, t in MATCH_COLUMNS.items()
                if c not in set(spark.table(FACT).columns)}
    if _missing:
        spark.sql(
            f"ALTER TABLE {FACT} ADD COLUMNS "
            f"({', '.join(f'{c} {t}' for c, t in _missing.items())})"
        )

# COMMAND ----------

# Purge rows written by the discredited nearest-scheduled method. MERGE only
# inserts unmatched keys, so without this the bad rows would survive forever.
if spark.catalog.tableExists(FACT):
    stale = spark.sql(
        f"SELECT COUNT(*) n FROM {FACT} "
        f"WHERE mode = 'rail' AND match_method = 'nearest_scheduled'"
    ).first()["n"]
    if stale:
        spark.sql(
            f"DELETE FROM {FACT} "
            f"WHERE mode = 'rail' AND match_method = 'nearest_scheduled'"
        )
        print(f"purged {stale} rows from the retired nearest_scheduled method")

# COMMAND ----------

feed_version = (
    spark.table(f"{CATALOG}.bronze.gtfs_feed_registry")
    .orderBy(F.col("ingest_date").desc())
    .first()["feed_version"]
)

dim_stop = spark.table(f"{CATALOG}.silver.dim_stop").where("is_current = true")
dim_trip = spark.table(f"{CATALOG}.silver.dim_trip").where("is_current = true")
stop_times = spark.table(f"{CATALOG}.silver.fact_scheduled_stop_time").where(
    "is_current = true"
)

rail_routes = (
    spark.table(f"{CATALOG}.bronze.gtfs_routes")
    .where((F.col("feed_version") == feed_version) & (F.col("route_type") == "1"))
    .select("route_id")
)
rail_stop_ids = (
    stop_times
    .join(dim_trip.join(F.broadcast(rail_routes), "route_id").select("trip_id"),
          "trip_id", "inner")
    .select("stop_id").distinct()
)

# Scope to rail-served stops so same-named bus bays ("ARTS CENTER STATION -
# BAY I") can't collide with the station.
stop_xwalk = (
    dim_stop.join(rail_stop_ids, "stop_id", "inner")
    .withColumn("match_key", norm_udf("stop_name"))
    .where(F.col("match_key").isNotNull())
    .select("stop_id", "match_key").dropDuplicates(["match_key"])
)

# COMMAND ----------

obs = (
    spark.table(f"{CATALOG}.silver.telemetry_conformed")
    .where("mode = 'rail' AND event_type = 'rail_arrival'")
    .where("stop_id IS NOT NULL AND event_ts_utc IS NOT NULL")
    .withColumnRenamed("stop_id", "station_name")
    .drop("trip_id", "sched_route_id")
    .withColumn("match_key", norm_udf("station_name"))
    .withColumn("ingest_ts_utc", F.col("ingest_ts"))
)

if spark.catalog.tableExists(FACT):
    hwm = (
        spark.table(FACT).where("mode = 'rail'")
        .agg(F.max("ingest_ts_utc")).first()[0]
    )
    if hwm:
        obs = obs.where(F.col("ingest_ts_utc") > F.lit(hwm))

obs_total = obs.count()
matched = obs.join(F.broadcast(stop_xwalk), "match_key", "left")

unmatched_names = (
    matched.where(F.col("stop_id").isNull())
    .groupBy("station_name", "match_key").count()
)

# COMMAND ----------

rail_fact = (
    matched
    .where(F.col("stop_id").isNotNull() & F.col("delay_seconds").isNotNull())
    # MARTA's DELAY is the deviation: positive = late, same sign convention
    # as the bus path's observed-minus-scheduled.
    .withColumn("deviation_seconds", F.col("delay_seconds").cast("long"))
    # Reconstructed for continuity with the bus rows, not independently
    # measured: the scheduled instant implied by the agency's own delay.
    .withColumn(
        "scheduled_ts_utc",
        (F.col("event_ts_utc").cast("long") - F.col("deviation_seconds")).cast(
            "timestamp"
        ),
    )
    .withColumn("service_date", F.to_date("event_ts_utc"))
    .withColumn(
        "otp_bucket",
        F.when(F.col("deviation_seconds") < F.lit(ON_TIME_EARLY_SECONDS), "early")
        .when(F.col("deviation_seconds") <= F.lit(ON_TIME_LATE_SECONDS), "on_time")
        .otherwise("late"),
    )
    .withColumn("match_method", F.lit("agency_reported"))
    .withColumn("match_ambiguous", F.lit(False))
    .withColumn("match_direction_known", F.col("bearing").isNotNull())
    .withColumn("trip_id", F.lit(None).cast("string"))
    .withColumn("schedule_version", F.lit(feed_version))
    .select(
        "event_id", "mode", "vehicle_id", "trip_id", "route_id", "stop_id",
        "service_date", "event_ts_utc", "ingest_ts_utc", "scheduled_ts_utc",
        "deviation_seconds", "otp_bucket", "schedule_version",
        "match_method", "match_ambiguous", "match_direction_known",
    )
)

# COMMAND ----------

from delta.tables import DeltaTable

n_fact = rail_fact.count()
if not spark.catalog.tableExists(FACT):
    rail_fact.write.format("delta").option("mergeSchema", "true").saveAsTable(FACT)
else:
    (
        DeltaTable.forName(spark, FACT).alias("t")
        .merge(rail_fact.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )

n_named = matched.where(F.col("stop_id").isNotNull()).count()
n_delay = matched.where(F.col("delay_seconds").isNotNull()).count()

(
    spark.createDataFrame(
        [(obs_total, n_named, n_delay, n_fact, feed_version)],
        "observations long, station_matched long, delay_present long, "
        "written long, feed_version string",
    )
    .withColumn("run_ts", F.current_timestamp())
    .write.format("delta").mode("append").option("mergeSchema", "true")
    .saveAsTable(COVERAGE)
)

print(
    f"rail: {obs_total} obs -> {n_named} station-matched, "
    f"{n_delay} with agency delay -> {n_fact} written"
)
if obs_total and n_named < obs_total:
    print("UNMATCHED station names — add to _ALIASES in rail_match.py:")
    unmatched_names.orderBy(F.col("count").desc()).show(50, truncate=False)

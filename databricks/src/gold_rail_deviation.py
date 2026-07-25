# Databricks notebook source
# Rail schedule deviation — the feed carries no trip_id and names stations
# by label, so the bus join (trip_id + stop_id) dropped every rail row
# silently (LESSON #50). Three-stage match instead:
#
#   1. station name  -> stop_id   (normalized key, marta_pulse.rail_match)
#   2. DIRECTION     -> direction_id  (inferred from GTFS geometry, not
#                                      hardcoded — LESSON #53)
#   3. nearest scheduled arrival within MATCH_WINDOW_SECONDS
#
# Stage 3 is an assignment, not a measurement: it cannot report a deviation
# larger than half the headway, so it understates exactly when service is
# worst. Rows are tagged match_method='nearest_scheduled' and carry
# match_ambiguous / match_direction_known so the dashboard can separate
# confident rows from inferred ones instead of averaging them into the bus
# numbers.

from datetime import timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from marta_pulse.deviation import (
    ON_TIME_EARLY_SECONDS,
    ON_TIME_LATE_SECONDS,
    scheduled_instant_utc,
    service_date_for,
)
from marta_pulse.rail_match import (
    AMBIGUOUS_MARGIN_SECONDS,
    MATCH_WINDOW_SECONDS,
    axis_direction,
    bearing_to_direction,
    normalize_station,
    rail_route_from_line,
)

dbutils.widgets.text("catalog", "marta_pulse")
CATALOG = dbutils.widgets.get("catalog")

FACT = f"{CATALOG}.gold.fact_schedule_deviation"
COVERAGE = f"{CATALOG}.gold.rail_match_coverage"

# Serverless rejects spark.databricks.delta.schema.autoMerge.enabled, and
# MERGE won't evolve the target on its own — add the match_* columns
# explicitly and idempotently (LESSON #56). gold_deviation does the same;
# whichever task runs first wins, and the second is a no-op.
MATCH_COLUMNS = {
    "match_method": "STRING",
    "match_ambiguous": "BOOLEAN",
    "match_direction_known": "BOOLEAN",
}

if spark.catalog.tableExists(FACT):
    _existing = set(spark.table(FACT).columns)
    _missing = {c: t for c, t in MATCH_COLUMNS.items() if c not in _existing}
    if _missing:
        _cols = ", ".join(f"{c} {t}" for c, t in _missing.items())
        spark.sql(f"ALTER TABLE {FACT} ADD COLUMNS ({_cols})")
        print(f"added columns to {FACT}: {sorted(_missing)}")

norm_udf = F.udf(normalize_station, "string")
colour_udf = F.udf(rail_route_from_line, "string")
dir_udf = F.udf(bearing_to_direction, "string")
axis_udf = F.udf(axis_direction, "string")
service_date_udf = F.udf(
    lambda ts: service_date_for(ts.replace(tzinfo=timezone.utc)) if ts else None, "date"
)
sched_instant_udf = F.udf(
    lambda d, s: scheduled_instant_utc(d, s) if d is not None and s is not None else None,
    "timestamp",
)

# COMMAND ----------

feed_version = (
    spark.table(f"{CATALOG}.bronze.gtfs_feed_registry")
    .orderBy(F.col("ingest_date").desc())
    .first()["feed_version"]
)
print(f"matching against feed_version {feed_version}")

# Rail routes: route_type 1 = subway. The realtime feed only knows colours.
gtfs_routes = (
    spark.table(f"{CATALOG}.bronze.gtfs_routes")
    .where((F.col("feed_version") == feed_version) & (F.col("route_type") == "1"))
    .withColumn(
        "colour",
        F.regexp_extract(
            F.upper(F.coalesce("route_short_name", "route_long_name")),
            r"(RED|GOLD|BLUE|GREEN)",
            1,
        ),
    )
    .where(F.col("colour") != "")
    .select("route_id", "colour")
)

dim_trip = spark.table(f"{CATALOG}.silver.dim_trip").where("is_current = true")
dim_stop = spark.table(f"{CATALOG}.silver.dim_stop").where("is_current = true")
stop_times = spark.table(f"{CATALOG}.silver.fact_scheduled_stop_time").where(
    "is_current = true"
)

rail_trips = dim_trip.join(F.broadcast(gtfs_routes), "route_id", "inner").select(
    "trip_id", "route_id", "colour", "direction_id"
)

# COMMAND ----------

# Stage 2 prep: what does direction_id 0/1 mean on the ground? Infer it from
# the net movement between each trip's first and last stop rather than
# assuming an agency convention that could change between feed versions.
trip_ends = (
    stop_times.join(F.broadcast(rail_trips), "trip_id", "inner")
    .join(dim_stop.select("stop_id", "stop_lat", "stop_lon"), "stop_id", "inner")
)
w_trip = Window.partitionBy("trip_id").orderBy("stop_sequence")
w_trip_all = Window.partitionBy("trip_id")

ends = (
    trip_ends
    .withColumn("rn", F.row_number().over(w_trip))
    .withColumn("last_rn", F.max("rn").over(w_trip_all))
    .where("rn = 1 OR rn = last_rn")
    .groupBy("route_id", "colour", "direction_id", "trip_id")
    .agg(
        F.first("stop_lat").alias("lat0"), F.last("stop_lat").alias("lat1"),
        F.first("stop_lon").alias("lon0"), F.last("stop_lon").alias("lon1"),
    )
)

direction_map = (
    ends.groupBy("route_id", "colour", "direction_id")
    .agg(
        F.avg(F.col("lat1") - F.col("lat0")).alias("dlat"),
        F.avg(F.col("lon1") - F.col("lon0")).alias("dlon"),
    )
    .withColumn("direction_letter", axis_udf("dlat", "dlon"))
    .select("route_id", "colour", "direction_id", "direction_letter")
)
print("inferred direction_id meanings:")
direction_map.orderBy("colour", "direction_id").show(truncate=False)

# COMMAND ----------

# Stage 1 prep: station crosswalk, scoped to stops rail actually serves so
# same-named bus bays ("ARTS CENTER STATION - BAY I") can't collide.
rail_stop_ids = (
    stop_times.join(F.broadcast(rail_trips.select("trip_id")), "trip_id", "inner")
    .select("stop_id").distinct()
)

stop_xwalk = (
    dim_stop.join(rail_stop_ids, "stop_id", "inner")
    .withColumn("match_key", norm_udf("stop_name"))
    .where(F.col("match_key").isNotNull())
    .select("stop_id", "stop_name", "match_key").distinct()
)

_dupes = stop_xwalk.groupBy("match_key").count().where("count > 1").collect()
if _dupes:
    # Expected: a station has one platform stop per direction. Direction
    # disambiguates them at join time, so this is informational.
    print(f"station keys spanning multiple stop_ids: {len(_dupes)} (direction resolves)")

# COMMAND ----------

obs = (
    spark.table(f"{CATALOG}.silver.telemetry_conformed")
    .where("mode = 'rail' AND event_type = 'rail_arrival'")
    .where("stop_id IS NOT NULL AND event_ts_utc IS NOT NULL")
    .withColumnRenamed("stop_id", "station_name")
    # Rail has no trip_id (always null) and Silver already attached its own
    # sched_route_id; both names also come from the schedule side of the
    # join below, so drop them here rather than resolve ambiguity later.
    .drop("trip_id", "sched_route_id")
    .withColumn("match_key", norm_udf("station_name"))
    .withColumn("colour", colour_udf("route_id"))
    .withColumn("direction_letter", dir_udf("bearing"))
    .withColumn("ingest_ts_utc", F.col("ingest_ts"))
)

if spark.catalog.tableExists(FACT):
    hwm = (
        spark.table(FACT).where("mode = 'rail'")
        .agg(F.max("ingest_ts_utc")).first()[0]
    )
    if hwm:
        obs = obs.where(F.col("ingest_ts_utc") > F.lit(hwm))

obs = obs.withColumn("service_date", service_date_udf("event_ts_utc"))
obs_total = obs.count()
print(f"rail observations to match: {obs_total}")

# COMMAND ----------

matched_stop = obs.join(F.broadcast(stop_xwalk), "match_key", "left")
unmatched_names = (
    matched_stop.where(F.col("stop_id").isNull())
    .groupBy("station_name", "match_key").count()
)

# Scheduled arrivals at rail stops, as absolute instants.
sched = (
    stop_times.join(F.broadcast(rail_trips), "trip_id", "inner")
    .select(
        F.col("trip_id"), F.col("stop_id"), F.col("colour"),
        F.col("route_id").alias("sched_route_id"),
        F.col("direction_id"), F.col("arrival_seconds"),
        F.col("feed_version").alias("schedule_version"),
    )
    .join(
        F.broadcast(
            direction_map
            .select("route_id", "direction_id", "direction_letter")
            .withColumnRenamed("route_id", "sched_route_id")
            # Both sides would otherwise be `direction_letter`.
            .withColumnRenamed("direction_letter", "sched_direction_letter")
        ),
        ["sched_route_id", "direction_id"],
        "left",
    )
)

# Direction is only usable once the Function emits it (bearing). Older rows
# have bearing NULL — match them direction-agnostically and flag it rather
# than dropping them.
cand = (
    matched_stop.where(F.col("stop_id").isNotNull())
    .join(sched, ["stop_id", "colour"], "inner")
    .where(
        F.col("direction_letter").isNull()
        | F.col("sched_direction_letter").isNull()
        | (F.col("direction_letter") == F.col("sched_direction_letter"))
    )
    .withColumn(
        "scheduled_ts_utc", sched_instant_udf("service_date", "arrival_seconds")
    )
    .withColumn(
        "delta",
        F.col("event_ts_utc").cast("long") - F.col("scheduled_ts_utc").cast("long"),
    )
)

w = Window.partitionBy("event_id").orderBy(F.abs(F.col("delta")))
ranked = (
    cand
    .withColumn("next_delta", F.lead("delta").over(w))
    .withColumn("rn", F.row_number().over(w))
    .where("rn = 1")
    .withColumn(
        "match_ambiguous",
        F.col("next_delta").isNotNull()
        & ((F.abs("next_delta") - F.abs("delta")) < F.lit(AMBIGUOUS_MARGIN_SECONDS)),
    )
    .withColumn("match_window_ok", F.abs("delta") <= F.lit(MATCH_WINDOW_SECONDS))
    .withColumn("match_direction_known", F.col("direction_letter").isNotNull())
)

rail_fact = (
    ranked.where("match_window_ok")
    .withColumnRenamed("delta", "deviation_seconds")
    .withColumn(
        "otp_bucket",
        F.when(F.col("deviation_seconds") < F.lit(ON_TIME_EARLY_SECONDS), "early")
        .when(F.col("deviation_seconds") <= F.lit(ON_TIME_LATE_SECONDS), "on_time")
        .otherwise("late"),
    )
    .withColumn("match_method", F.lit("nearest_scheduled"))
    .select(
        "event_id", "mode", "vehicle_id", "trip_id",
        F.col("sched_route_id").alias("route_id"),
        "stop_id", "service_date", "event_ts_utc", "ingest_ts_utc",
        "scheduled_ts_utc", "deviation_seconds", "otp_bucket",
        "schedule_version", "match_method", "match_ambiguous",
        "match_direction_known",
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

n_named = matched_stop.where(F.col("stop_id").isNotNull()).count()
n_ambig = ranked.where("match_window_ok AND match_ambiguous").count()
n_dir = ranked.where("match_window_ok AND match_direction_known").count()

(
    spark.createDataFrame(
        [(obs_total, n_named, n_fact, n_ambig, n_dir, feed_version)],
        "observations long, station_matched long, written long, "
        "ambiguous long, direction_known long, feed_version string",
    )
    .withColumn("run_ts", F.current_timestamp())
    .write.format("delta").mode("append").option("mergeSchema", "true")
    .saveAsTable(COVERAGE)
)

print(
    f"rail: {obs_total} obs -> {n_named} station-matched -> {n_fact} written "
    f"({n_ambig} ambiguous, {n_dir} with known direction)"
)
if obs_total and n_named < obs_total:
    print("UNMATCHED station names — add to _ALIASES in rail_match.py:")
    unmatched_names.orderBy(F.col("count").desc()).show(50, truncate=False)

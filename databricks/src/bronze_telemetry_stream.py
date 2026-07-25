# Databricks notebook source
# Bronze telemetry: Azure Event Hub (Kafka endpoint) -> Delta, availableNow.
# Consumer group "databricks" — Fabric's Eventstream can consume the same
# hub on its own group. Runs as the first task of the telemetry_refresh job.

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

dbutils.widgets.text("catalog", "marta_pulse")
dbutils.widgets.text("eventhub_namespace", "ehns-martapulse-dev")
dbutils.widgets.text("eventhub_name", "telemetry")
dbutils.widgets.text("secret_scope", "marta-pulse")
CATALOG = dbutils.widgets.get("catalog")
EH_NS = dbutils.widgets.get("eventhub_namespace")
EH_NAME = dbutils.widgets.get("eventhub_name")
SCOPE = dbutils.widgets.get("secret_scope")

BRONZE = f"{CATALOG}.bronze.raw_events"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/telemetry_stream"

conn = dbutils.secrets.get(SCOPE, "eventhub-connection")

# COMMAND ----------

# Canonical schema — mirrors marta_pulse.canonical.CANONICAL_FIELDS.
# event_ts/ingest_ts are ISO-8601 UTC strings from the source (v0.2.0+),
# parsed straight to timestamps here: typed Bronze from the first write.
schema = StructType([
    StructField("event_id", StringType()),
    StructField("schema_version", StringType()),
    StructField("mode", StringType()),
    StructField("event_type", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("stop_id", StringType()),
    StructField("stop_sequence", IntegerType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("bearing", DoubleType()),
    StructField("delay_seconds", IntegerType()),
    StructField("event_ts", TimestampType()),
    StructField("ingest_ts", TimestampType()),
    StructField("source_feed", StringType()),
])

# COMMAND ----------

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{EH_NS}.servicebus.windows.net:9093")
    .option("subscribe", EH_NAME)
    .option("kafka.group.id", "databricks")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{conn}";',
    )
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
    .select("e.*")
    .where("event_id IS NOT NULL")
)

(
    events.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)     # drain everything new, then stop
    .toTable(BRONZE)
    .awaitTermination()
)

print("bronze telemetry stream drained")

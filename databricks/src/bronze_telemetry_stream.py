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

# Event Hubs' Kafka SASL handler rejects the whole string with an opaque
# "not of the expected format / unexpected properties" if it carries anything
# beyond Endpoint/SharedAccessKeyName/SharedAccessKey. The usual culprits:
# a trailing ;EntityPath= (Kafka takes the topic from `subscribe`), quotes or
# a newline captured by the shell when the secret was set, or a stray ';'.
# Sanitize, then assert the shape — Databricks redacts the secret itself from
# output, so we report only derived facts.
_KEEP = ("Endpoint", "SharedAccessKeyName", "SharedAccessKey")

conn = conn.strip().strip('"').strip("'").strip()
_parts = [p for p in conn.split(";") if p.strip()]
_dropped = [p.split("=", 1)[0] for p in _parts if p.split("=", 1)[0] not in _KEEP]
conn = ";".join(p for p in _parts if p.split("=", 1)[0] in _KEEP)

_keys = [p.split("=", 1)[0] for p in conn.split(";")]
print(f"conn: {len(conn)} chars, keys={_keys}, dropped={_dropped}")
if not conn.startswith("Endpoint=sb://"):
    raise RuntimeError(
        f"Secret {SCOPE}/eventhub-connection does not start with 'Endpoint=sb://' "
        f"(starts with {conn[:14]!r}). Set the namespace-level connection string."
    )
for _required in ("SharedAccessKeyName=", "SharedAccessKey="):
    if _required not in conn:
        raise RuntimeError(f"Secret is missing {_required} — got keys {_keys}.")
if f"//{EH_NS}." not in conn:
    raise RuntimeError(
        f"Secret points at a different namespace than {EH_NS} — check you copied "
        f"the string from ehns-martapulse-databricks, not the older namespace."
    )

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

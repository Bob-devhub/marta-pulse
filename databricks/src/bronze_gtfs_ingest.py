# Databricks notebook source
# GTFS static download + Bronze landing. Replaces the Fabric pipeline's
# Copy activity AND NB_Bronze_GTFS_Unzip: no HTTP connection object needed —
# plain requests + a UC volume.

import csv
import io

import requests
from pyspark.sql import functions as F

from marta_pulse.gtfs_static import extract_members, feed_version

dbutils.widgets.text("catalog", "marta_pulse")
CATALOG = dbutils.widgets.get("catalog")

GTFS_URL = "https://itsmarta.com/google_transit_feed/google_transit.zip"
VOLUME = f"/Volumes/{CATALOG}/bronze/gtfs_static"
REGISTRY = f"{CATALOG}.bronze.gtfs_feed_registry"

# COMMAND ----------

zip_bytes = requests.get(GTFS_URL, timeout=120).content
version = feed_version(zip_bytes)
ingest_date = spark.sql("SELECT current_date()").first()[0].isoformat()
print(f"feed_version={version} ingest_date={ingest_date} size={len(zip_bytes):,}")

already = (
    spark.catalog.tableExists(REGISTRY)
    and spark.table(REGISTRY).where(f"feed_version = '{version}'").count() > 0
)
if already:
    dbutils.notebook.exit(f"NOOP: feed_version {version} already ingested")

# Archive the zip for replayability
dbutils.fs.mkdirs(f"{VOLUME}/{ingest_date}_{version}")
with open(f"{VOLUME}/{ingest_date}_{version}/google_transit.zip", "wb") as fh:
    fh.write(zip_bytes)

# COMMAND ----------

members = extract_members(zip_bytes)

for fname, data in members.items():
    table = f"{CATALOG}.bronze.gtfs_{fname.removesuffix('.txt')}"
    rows = list(csv.DictReader(io.StringIO(data.decode("utf-8-sig"))))
    if not rows:
        print(f"skip empty member {fname}")
        continue
    df = (
        spark.createDataFrame(rows)  # all strings by design (raw layer)
        .withColumn("feed_version", F.lit(version))
        .withColumn("ingest_date", F.lit(ingest_date).cast("date"))
    )
    df.write.format("delta").mode("append").saveAsTable(table)
    print(f"{table}: +{df.count()} rows")

registry_row = spark.createDataFrame(
    [(version, ingest_date, sorted(members.keys()))],
    "feed_version string, ingest_date string, members array<string>",
).withColumn("ingest_date", F.col("ingest_date").cast("date"))
registry_row.write.format("delta").mode("append").saveAsTable(REGISTRY)

dbutils.notebook.exit(f"OK: ingested feed_version {version}")

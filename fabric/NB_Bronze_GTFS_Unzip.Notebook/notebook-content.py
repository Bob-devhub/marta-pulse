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

# NB_Bronze_GTFS_Unzip
# ---------------------
# Input : Files/bronze/gtfs_static/incoming/google_transit.zip
#         (landed by PL_GTFS_Static_Ingest copy activity)
# Output: Bronze Delta per GTFS member + bronze.gtfs_feed_registry
# Contract: Bronze = raw ingestion only. No typing, no validation — that
#           belongs in Silver.

from datetime import date

from pyspark.sql import functions as F

from marta_pulse.gtfs_static import extract_members, feed_version

INCOMING = "Files/bronze/gtfs_static/incoming/google_transit.zip"
ARCHIVE_ROOT = "Files/bronze/gtfs_static"
REGISTRY = "bronze.gtfs_feed_registry"

# Read via the local lakehouse mount (binary-safe, unlike fs.head)
with open(f"/lakehouse/default/{INCOMING}", "rb") as fh:
    zip_bytes = fh.read()

version = feed_version(zip_bytes)
ingest_date = date.today().isoformat()
print(f"feed_version={version} ingest_date={ingest_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Idempotency gate: skip entirely if this exact publication is registered.
already = (
    spark.catalog.tableExists(REGISTRY)
    and spark.table(REGISTRY).where(f"feed_version = '{version}'").count() > 0
)
if already:
    mssparkutils.notebook.exit(f"NOOP: feed_version {version} already ingested")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv
import io

members = extract_members(zip_bytes)

for fname, data in members.items():
    table = f"bronze.gtfs_{fname.removesuffix('.txt')}"
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

registry_row = spark.createDataFrame(
    [(version, ingest_date, sorted(members.keys()))],
    "feed_version string, ingest_date string, members array<string>",
).withColumn("ingest_date", F.col("ingest_date").cast("date"))
registry_row.write.format("delta").mode("append").saveAsTable(REGISTRY)

# Archive the zip for replayability
mssparkutils.fs.cp(INCOMING, f"{ARCHIVE_ROOT}/{ingest_date}_{version}/google_transit.zip")
mssparkutils.notebook.exit(f"OK: ingested feed_version {version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

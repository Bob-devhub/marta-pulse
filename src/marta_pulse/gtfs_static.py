"""GTFS Static (batch) helpers: versioning, extraction, SCD2 conformance.

Design principles:
  * The zip's SHA-256 is the feed version. Identical hash => no-op run.
  * Silver dims are SCD Type 2 on feed_version so historical telemetry
    always joins to the schedule that was in force when it was observed.
PySpark imports are deferred so the module stays importable in the
Azure Function / unit-test context.
"""

from __future__ import annotations

import hashlib
import io
import zipfile
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

GTFS_CORE_FILES = [
    "agency.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "routes.txt",
    "shapes.txt",
    "stops.txt",
    "stop_times.txt",
    "trips.txt",
]


def feed_version(zip_bytes: bytes) -> str:
    """Deterministic version id for a GTFS static publication."""
    return hashlib.sha256(zip_bytes).hexdigest()[:16]


def extract_members(zip_bytes: bytes) -> dict[str, bytes]:
    """Return {filename: bytes} for known GTFS members in the zip."""
    out: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            base = name.split("/")[-1]
            if base in GTFS_CORE_FILES:
                out[base] = zf.read(name)
    missing = {"routes.txt", "trips.txt", "stops.txt", "stop_times.txt"} - set(out)
    if missing:
        raise ValueError(f"GTFS zip missing required files: {sorted(missing)}")
    return out


def version_already_loaded(spark: "SparkSession", table: str, version: str) -> bool:
    """Idempotency gate: has this feed_version already been promoted?"""
    if not spark.catalog.tableExists(table):
        return False
    return (
        spark.table(table).where(f"feed_version = '{version}'").limit(1).count() > 0
    )


def scd2_merge(
    spark: "SparkSession",
    incoming: "DataFrame",
    target_table: str,
    business_keys: list[str],
    version: str,
    effective_date: str,
) -> None:
    """SCD Type 2 versioning keyed on the GTFS feed version.

    GTFS is published as a complete snapshot, so a new feed version
    supersedes the ENTIRE previous version: every currently-open row is
    closed unconditionally — including rows absent from `incoming`
    (schedule removed the entity) — then the new snapshot is appended as
    the open version. Hash-diffing per-row is unnecessary because the
    version IS the change unit.

    `business_keys` identifies the entity grain; kept for lineage and
    call-site readability even though snapshot close-out doesn't need a
    key match. Idempotency (same version twice) is the caller's job via
    `version_already_loaded`.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    staged = (
        incoming.withColumn("feed_version", F.lit(version))
        .withColumn("effective_from", F.lit(effective_date).cast("date"))
        .withColumn("effective_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )

    if not spark.catalog.tableExists(target_table):
        staged.write.format("delta").saveAsTable(target_table)
        return

    # Close EVERY open row (snapshot semantics) — a key-matched MERGE would
    # leave entities dropped from the new schedule open forever.
    DeltaTable.forName(spark, target_table).update(
        condition=F.col("is_current") == F.lit(True),
        set={
            "is_current": F.lit(False),
            "effective_to": F.date_sub(F.to_date(F.lit(effective_date)), 1),
        },
    )
    staged.write.format("delta").mode("append").saveAsTable(target_table)

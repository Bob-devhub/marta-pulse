-- Unity Catalog bootstrap for MARTA Pulse (idempotent — run per environment).
-- Run in the SQL editor or a notebook. For test/prod substitute the catalog
-- name (marta_pulse_test / marta_pulse_prod) to match databricks.yml targets.

CREATE CATALOG IF NOT EXISTS marta_pulse
  COMMENT 'MARTA Pulse - schedule vs reality lakehouse (build #3)';

CREATE SCHEMA IF NOT EXISTS marta_pulse.bronze
  COMMENT 'Raw: canonical telemetry from Event Hub + GTFS static landings';
CREATE SCHEMA IF NOT EXISTS marta_pulse.silver
  COMMENT 'Conformed: SCD2 schedule dims + deduped/enriched telemetry';
CREATE SCHEMA IF NOT EXISTS marta_pulse.gold
  COMMENT 'Serving: schedule deviation, OTP, headway/bunching facts';

-- Volumes: streaming checkpoints + GTFS zip archive
CREATE VOLUME IF NOT EXISTS marta_pulse.bronze.checkpoints
  COMMENT 'Structured Streaming checkpoints (bronze_telemetry_stream)';
CREATE VOLUME IF NOT EXISTS marta_pulse.bronze.gtfs_static
  COMMENT 'Archived google_transit.zip per feed_version';

SHOW SCHEMAS IN marta_pulse;

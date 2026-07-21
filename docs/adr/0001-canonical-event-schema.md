# ADR 0001: One canonical event schema for two heterogeneous streams

## Status
Accepted

## Context
MARTA publishes bus telemetry as standards-based GTFS-Realtime protobuf
and rail telemetry as a bespoke JSON REST API. Downstream consumers
(Eventhouse, Silver, Gold) should not care which mode an event came from.

## Decision
Normalize at the EDGE (the Azure Function), before events enter Fabric.
A single canonical schema (marta_pulse.canonical.CANONICAL_FIELDS) with a
schema_version field and a deterministic event_id (SHA-256 of natural
keys) used as the dedupe key everywhere downstream.

## Consequences
+ One Eventstream, one Bronze table, one Silver conformance path.
+ event_id makes retries and Function overlap idempotent.
- Rail's local-format EVENT_TIME can't be resolved to UTC without a
  timezone rule; that resolution is deferred to Silver (documented in
  NB_Silver_Telemetry_Stream), keeping the Function logic trivial.

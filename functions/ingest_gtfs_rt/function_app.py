"""Timer-triggered ingestion: MARTA GTFS-RT (bus) + Rail REST -> Eventstream.

Every 30 seconds:
  1. Fetch bus VehiclePositions + TripUpdates protobuf; decode to dicts.
  2. Poll the rail realtime REST API (JSON, API key from Key Vault ref).
  3. Normalize everything to the canonical schema (marta_pulse.canonical).
  4. Batch-send JSON events to the Fabric Eventstream custom endpoint
     (Event Hub-compatible connection string).

App settings expected (see infra/main.bicep):
  BUS_VP_URL, BUS_TU_URL       - MARTA GTFS-RT endpoints
  RAIL_API_URL                 - rail realtime REST endpoint (no key in URL)
  RAIL_API_KEY                 - Key Vault reference
  EVENTSTREAM_CONNECTION       - Event Hub-compatible conn string (Key Vault ref)
  EVENTSTREAM_NAME             - entity/hub name from the Eventstream source
"""

import json
import logging
import os

import azure.functions as func
import requests
from azure.eventhub import EventData, EventHubProducerClient
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

from marta_pulse.canonical import (
    normalize_bus_trip_update,
    normalize_bus_vehicle_position,
    normalize_rail,
)

app = func.FunctionApp()

TIMEOUT = 10  # seconds per upstream call


def _fetch_protobuf(url: str) -> list[dict]:
    resp = requests.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return [MessageToDict(e) for e in feed.entity]


def _fetch_rail(url: str, api_key: str) -> list[dict]:
    resp = requests.get(url, params={"apiKey": api_key}, timeout=TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    return body if isinstance(body, list) else body.get("RailArrivals", [])


def _collect_events() -> list[dict]:
    events: list[dict] = []

    for entity in _fetch_protobuf(os.environ["BUS_VP_URL"]):
        events.extend(normalize_bus_vehicle_position(entity))
    for entity in _fetch_protobuf(os.environ["BUS_TU_URL"]):
        events.extend(normalize_bus_trip_update(entity))

    try:
        for record in _fetch_rail(
            os.environ["RAIL_API_URL"], os.environ["RAIL_API_KEY"]
        ):
            events.extend(normalize_rail(record))
    except Exception:  # rail API is flakier than the GTFS-RT feeds; don't drop bus data
        logging.exception("rail feed fetch failed; continuing with bus events only")

    return events


def _send(events: list[dict]) -> None:
    producer = EventHubProducerClient.from_connection_string(
        os.environ["EVENTSTREAM_CONNECTION"],
        eventhub_name=os.environ["EVENTSTREAM_NAME"],
    )
    with producer:
        batch = producer.create_batch()
        for ev in events:
            data = EventData(json.dumps(ev, separators=(",", ":")))
            try:
                batch.add(data)
            except ValueError:  # batch full -> flush and start a new one
                producer.send_batch(batch)
                batch = producer.create_batch()
                batch.add(data)
        if len(batch) > 0:
            producer.send_batch(batch)


# 30s cadence: halves ingestion CU vs 15s (F2 capacity headroom) while staying
# well inside the feeds' own update rates (~10-30s).
@app.timer_trigger(schedule="*/30 * * * * *", arg_name="timer", run_on_startup=False)
def ingest_gtfs_rt(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("timer past due; skipping this tick to avoid pile-up")
        return
    events = _collect_events()
    if events:
        _send(events)
    logging.info("ingested %d canonical events", len(events))

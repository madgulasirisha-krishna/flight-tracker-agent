"""
adsb.lol Live Flight Status — Lakeflow Declarative Pipeline STREAMING TABLES.

This is the ONE genuine streaming source in the whole architecture (the
project's Velocity V). Defines two tables:

  live_flight_status   -> per-poll snapshot (unchanged purpose from before,
                           now also carries lat/lon for the map view)
  flight_status_events -> STATEFUL transition detection (on_ground <-> in_air),
                           using applyInPandasWithState — genuine stateful
                           stream processing, not just a pass-through table.
                           Emits "departure" and "landing" events.

Landing events are consumed by 13_alert_dispatcher.py (a separate
notebook) to actually fire matching Lakebase alerts — pipeline table
functions can't write to an external system like Lakebase themselves,
same reasoning as why Vector Search index creation lives outside the
RAG pipeline.

LIMITATIONS (permanent characteristics of adsb.lol, not bugs):
- gate and delay_minutes are ALWAYS NULL — adsb.lol has no source for these.
- flight_date is carried through from watched_flights via the polling job.
- status is a coarse on_ground/in_air heuristic from alt_baro.

Pipeline configuration:
  adsb_catalog        -> e.g. flight_tracker
  adsb_bronze_schema  -> e.g. bronze
  adsb_landing_table  -> e.g. adsb_lol_flight_landing
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pandas as pd

adsb_catalog = spark.conf.get("adsb_catalog", "bootcamp_students")
schema = spark.conf.get("schema", "madgula_sirisha_capstone")
adsb_landing_table = spark.conf.get("adsb_landing_table", "adsb_lol_flight_landing")

LANDING_FULL_NAME = f"{adsb_catalog}.{schema}.{adsb_landing_table}"

AIRCRAFT_SCHEMA = StructType([
    StructField("hex", StringType()),
    StructField("flight", StringType()),
    StructField("alt_baro", StringType()),  # number OR the literal string "ground"
    StructField("gs", DoubleType()),
    StructField("track", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("squawk", StringType()),
    StructField("seen", DoubleType()),
    StructField("seen_pos", DoubleType()),
])


# ============================================================
# Table 1: live_flight_status (per-poll snapshot, now with lat/lon)
# ============================================================

@dp.table(
    comment="Live flight status derived from adsb.lol telemetry. gate and "
            "delay_minutes are always NULL — adsb.lol has no source for "
            "these fields. status is a coarse on_ground/in_air heuristic. "
            "lat/lon added for the live map view."
)
def live_flight_status():
    landing = spark.readStream.table(LANDING_FULL_NAME)

    return (
        landing
        .filter(F.col("found") == True)
        .withColumn("aircraft", F.from_json(F.col("raw_json"), AIRCRAFT_SCHEMA))
        .select(
            F.col("flight_number"),
            F.col("flight_date"),
            F.when(F.col("aircraft.alt_baro") == "ground", "on_ground")
             .otherwise("in_air").alias("status"),
            F.lit(None).cast("int").alias("delay_minutes"),
            F.lit(None).cast("string").alias("gate"),
            F.col("aircraft.lat").alias("lat"),
            F.col("aircraft.lon").alias("lon"),
            F.col("ingested_at").alias("last_updated"),
        )
    )


# ============================================================
# Table 2: flight_status_events (STATEFUL transition detection)
# ============================================================

EVENT_OUTPUT_SCHEMA = (
    "flight_number string, flight_date date, event_type string, "
    "event_timestamp timestamp, previous_status string, new_status string"
)
STATE_SCHEMA = "last_status string"


def detect_transitions(key, pdf_iter, state):
    """Called once per (flight_number, flight_date) group per micro-batch.
    Compares each new status reading against the last known status for
    that flight (carried in `state` across micro-batches) and emits an
    event row whenever it changes. State times out (is cleared) after 4
    hours of no new readings for a flight, so long-completed flights
    don't hold state forever.

    NOTE: this is the single most novel piece of API in this project
    (applyInPandasWithState's GroupState interface is used far less than
    the rest of Structured Streaming) — if the exact state.get/.exists/
    .update calls below don't match your PySpark version precisely, this
    is the first place to check against current docs."""
    flight_number, flight_date = key

    if state.hasTimedOut:
        state.remove()
        columns = ["flight_number", "flight_date", "event_type", "event_timestamp",
                   "previous_status", "new_status"]
        return pd.DataFrame(columns=columns)

    previous_status = state.get[0] if state.exists else None
    events = []

    for pdf in pdf_iter:
        pdf = pdf.sort_values("last_updated")
        for _, row in pdf.iterrows():
            current_status = row["status"]
            if previous_status is not None and current_status != previous_status:
                if previous_status == "on_ground" and current_status == "in_air":
                    event_type = "departure"
                elif previous_status == "in_air" and current_status == "on_ground":
                    event_type = "landing"
                else:
                    event_type = "status_change"

                events.append({
                    "flight_number": flight_number,
                    "flight_date": flight_date,
                    "event_type": event_type,
                    "event_timestamp": row["last_updated"],
                    "previous_status": previous_status,
                    "new_status": current_status,
                })
            previous_status = current_status

    if previous_status is not None:
        state.update((previous_status,))
    state.setTimeoutDuration(4 * 60 * 60 * 1000) 

    columns = ["flight_number", "flight_date", "event_type", "event_timestamp",
               "previous_status", "new_status"]
    yield pd.DataFrame(events, columns=columns) if events else pd.DataFrame(columns=columns)


@dp.table(
    comment="Departure/landing events detected via stateful stream "
            "processing (applyInPandasWithState) over live_flight_status. "
            "Landing events are consumed by 13_alert_dispatcher.py to fire "
            "matching Lakebase alerts."
)
def flight_status_events():
    status_stream = dp.read_stream("live_flight_status")

    return (
        status_stream
        .groupBy("flight_number", "flight_date")
        .applyInPandasWithState(
            detect_transitions,
            outputStructType=EVENT_OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf="ProcessingTimeTimeout",
        )
    )
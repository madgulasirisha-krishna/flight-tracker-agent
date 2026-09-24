# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Continuous adsb.lol Polling Job
# MAGIC Targeted polling: for each ACTIVE watched flight in Lakebase, polls
# MAGIC adsb.lol by callsign every ~20-30 seconds and lands the raw response
# MAGIC into a bronze table. Run this as a Databricks Job with trigger type
# MAGIC "Continuous" so it's automatically restarted if it ever crashes — the
# MAGIC actual polling cadence comes from the while-loop below, not the
# MAGIC job's restart behavior.
# MAGIC
# MAGIC LIMITATIONS (permanent characteristics of adsb.lol, not bugs):
# MAGIC - No gate or delay data exists in this source at all — only raw
# MAGIC   position/altitude/speed telemetry.
# MAGIC - No flight-date concept either — just live position right now. This
# MAGIC   is why flight_date is carried through FROM watched_flights below,
# MAGIC   rather than derived from "today" downstream.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("landing_table", "adsb_lol_flight_landing")
dbutils.widgets.text("poll_interval_seconds", "25")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing_table = dbutils.widgets.get("landing_table")
poll_interval_seconds = int(dbutils.widgets.get("poll_interval_seconds"))

full_table_name = f"{catalog}.{schema}.{landing_table}"

#spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
#spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

import os
import json
import time
from datetime import datetime, timezone
from contextlib import contextmanager

import requests
import psycopg2
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, DateType
from databricks.sdk import WorkspaceClient 

w = WorkspaceClient()
BASE_URL = "https://api.adsb.lol"

LANDING_ROW_SCHEMA = StructType([
    StructField("ingested_at", TimestampType(), nullable=False),
    StructField("flight_number", StringType(), nullable=False),
    StructField("flight_date", DateType(), nullable=False),
    StructField("found", BooleanType(), nullable=False),
    StructField("raw_json", StringType(), nullable=True),
    StructField("poll_error", StringType(), nullable=True),
])

@contextmanager
def get_lakebase_connection():
    """Yields a live Lakebase connection, closing it on exit."""
    
    endpoint_name = "projects/madgula-sirisha-capstone/branches/production/endpoints/primary"
    username = w.current_user.me().user_name

    if endpoint_name:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
        credential = client.postgres.generate_database_credential(endpoint=endpoint_name)
        conn = psycopg2.connect(
            host='ep-shiny-morning-d1a15kd9.database.us-west-2.cloud.databricks.com',
            port="5432",
            dbname="databricks_postgres",
            user=username,
            password=credential.token,
            sslmode="require",
        )
    else:
        connection_url = os.environ.get("LAKEBASE_CONNECTION_URL")
        if not connection_url:
            raise RuntimeError(
                "No Lakebase connection configured. Set LAKEBASE_ENDPOINT_NAME "
                "(inside a Databricks App) or LAKEBASE_CONNECTION_URL (local/notebook testing)."
            )
        conn = psycopg2.connect(connection_url)

    try:
        yield conn
    finally:
        conn.close()


def get_active_watched_flights() -> list[dict]:
    """Returns [{flight_number, flight_date}] for every actively watched
    flight — both fields are needed since adsb.lol has no date concept
    of its own; we carry the watched date through instead."""
    with get_lakebase_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT flight_number, flight_date FROM watched_flights "
                "WHERE status = 'active';"
            )
            return [{"flight_number": r[0], "flight_date": r[1]} for r in cur.fetchall()]


def poll_one_flight(flight_number: str, flight_date) -> dict:
    """Polls adsb.lol for one flight by callsign. found=False if the
    aircraft isn't currently airborne/visible — that's expected most of
    the time (flights aren't airborne 24/7), not an error."""
    try:
        response = requests.get(f"{BASE_URL}/v2/callsign/{flight_number}", timeout=10)
        response.raise_for_status()
        aircraft_list = (response.json() or {}).get("ac") or []
        found = bool(aircraft_list)
        raw_json = json.dumps(aircraft_list[0]) if found else None
        error = None
    except Exception as e:
        found, raw_json, error = False, None, str(e)

    return {
        "ingested_at": datetime.now(timezone.utc),
        "flight_number": flight_number,
        "flight_date": flight_date,
        "found": found,
        "raw_json": raw_json,
        "poll_error": error,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous polling loop
# MAGIC Runs until manually stopped (or the Job hits its timeout). Each
# MAGIC cycle re-reads the current watched-flights list, so newly added
# MAGIC watches are picked up without restarting the job.

# COMMAND ----------

print(f"Starting continuous polling loop (interval: {poll_interval_seconds}s). Stop the job/cell to end.")

while True:
    watched = get_active_watched_flights()

    if not watched:
        print("No active watched flights — sleeping.")
    else:
        rows = [Row(**poll_one_flight(w["flight_number"], w["flight_date"])) for w in watched]
        print(rows)
        df = spark.createDataFrame(rows, schema=LANDING_ROW_SCHEMA)
        df.write.mode("append").saveAsTable(full_table_name)
        found_count = sum(1 for r in rows if r["found"])
        print(
            f"Polled {len(watched)} flight(s), {found_count} airborne, "
            f"landed at {datetime.now(timezone.utc).isoformat()}"
        )

    time.sleep(poll_interval_seconds)
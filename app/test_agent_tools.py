# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Agent Tools Smoke Test
# MAGIC Exercises every tool (real and stubbed) end to end, including verifying
# MAGIC each call lands a row in agent_action_log. Run this before wiring the
# MAGIC tools into the actual chat/agent loop.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
# Point this at wherever lakebase_crud.py and agent_tools.py actually live
# in your workspace/repo (they're written to live alongside app.py).
sys.path.append("/Workspace/Users/madgula.sirisha@gmail.com/flight-tracker-agent/app")

import os

# For notebook testing, use the app_user connection URL (native password auth)
# from your secret scope rather than the Databricks App's ambient identity,
# since that identity only exists inside the deployed App itself.
os.environ["LAKEBASE_CONNECTION_URL"] = dbutils.secrets.get(scope="lakebase",  key="lakebase_connection_url")

import lakebase_crud as db
import agent_tools as tools

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Set up a test user

# COMMAND ----------

import os, subprocess, sys, importlib.metadata as md

_sdk_ver = md.version("databricks-sdk")
print(f"databricks-sdk version: {_sdk_ver}")

try:
    from databricks.sdk.service.postgres import PostgresAPI
except ImportError:
    print(f"SDK {_sdk_ver} lacks Lakebase (postgres) support. Upgrading...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--upgrade", "databricks-sdk>=0.118.0"],
    )
    _new_ver = md.version("databricks-sdk")
    print(f"Upgraded to {_new_ver}. Restarting Python — then re-run Cell 3 and Cell 5.")
    dbutils.library.restartPython()

# Re-import if Cell 3's state was lost after SDK upgrade restart
try:
    db, tools
except NameError:
    sys.path.insert(0, "/Workspace/Users/madgula.sirisha@gmail.com/flight-tracker-agent/app")
    import lakebase_crud as db
    import agent_tools as tools

from databricks.sdk import WorkspaceClient

# The connection URL in the secret has no password. Configure the OAuth
# credential path that get_connection() uses when LAKEBASE_ENDPOINT_NAME is set.
_w = WorkspaceClient()
_ep = _w.postgres.get_endpoint(
    name="projects/madgula-sirisha-capstone/branches/production/endpoints/primary"
)
os.environ["LAKEBASE_ENDPOINT_NAME"] = _ep.name
os.environ["PGHOST"] = _ep.status.hosts.host
os.environ["PGUSER"] = _w.current_user.me().user_name

user = db.get_or_create_user("smoketest@example.com")
user_id = user["user_id"]
print(f"Test user: {user_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exercise every write tool

# COMMAND ----------

watch = tools.add_flight_watch(
    user_id=user_id,
    flight_number="UAL123",
    flight_date="2026-10-01",
    origin_airport="ATL",
    destination_airport="ORD",
)
print("add_flight_watch:", watch)
watch_id = watch["watch_id"]

# COMMAND ----------

trip = tools.create_trip(user_id=user_id, trip_name="Smoke Test Trip", watch_ids=[watch_id])
print("create_trip:", trip)
trip_id = trip["trip_id"]

# COMMAND ----------

alert = tools.set_alert(user_id=user_id, watch_id=watch_id, alert_type="delay_threshold", threshold_minutes=30)
print("set_alert:", alert)
alert_id = alert["alert_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exercise every real read tool

# COMMAND ----------

print("list_my_watched_flights:", tools.list_my_watched_flights(user_id=user_id))
print("get_trip_summary:", tools.get_trip_summary(user_id=user_id, trip_id=trip_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exercise every stubbed read tool
# MAGIC These should succeed and return mock data with `"_stub": True`.

# COMMAND ----------

print("get_flight_status:", tools.get_flight_status(user_id=user_id, flight_number="UAL123", flight_date="2026-10-01"))
print("get_historical_delay_stats:", tools.get_historical_delay_stats(user_id=user_id, route="ATL-ORD"))
print("search_passenger_rights:", tools.search_passenger_rights(user_id=user_id, query="Can I get a refund for a cancelled flight?"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clean-up write tools

# COMMAND ----------

print("cancel_alert:", tools.cancel_alert(user_id=user_id, alert_id=alert_id))
print("remove_flight_watch:", tools.remove_flight_watch(user_id=user_id, watch_id=watch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify every call above was logged
# MAGIC Expect 9 rows: one per tool call in this notebook (all 10 tools were
# MAGIC called, but list_my_watched_flights and get_trip_summary each log once
# MAGIC too — count should match total tool calls made above).

# COMMAND ----------

with db.get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT action_type, result_status, created_at FROM agent_action_log "
            "WHERE user_id = %s ORDER BY created_at ASC;",
            (user_id,),
        )
        rows = cur.fetchall()

print(f"Logged {len(rows)} actions:")
for r in rows:
    print(f"  {r[2]}  {r[0]:<28} {r[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify the dispatcher (execute_tool) works too
# MAGIC This is the entry point the real agent loop will actually call.

# COMMAND ----------

result = tools.execute_tool(
    "get_flight_status",
    {"flight_number": "DAL456", "flight_date": "2026-10-02"},
    user_id=user_id,
)
print("execute_tool dispatch result:", result)
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # UC SQL Functions for MCP Exposure
# MAGIC Creates two Unity Catalog SQL Functions — one over historical delay
# MAGIC stats, one over live flight status — which Databricks automatically
# MAGIC exposes via its managed UC Functions MCP server. No custom MCP server
# MAGIC code needed; Unity Catalog handles governance and discovery.
# MAGIC
# MAGIC Since the real Spark enrichment and Lakeflow streaming pipelines don't
# MAGIC exist yet, this notebook also creates small placeholder Delta tables
# MAGIC with the right schema so both functions work end-to-end today. When
# MAGIC the real pipelines land (writing to these same table names), the
# MAGIC functions and their MCP exposure need zero changes.

# COMMAND ----------

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "madgula_sirisha_capstone")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

#spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Placeholder table: flights_historical_enriched
# MAGIC Matches the shape the real Spark enrichment job will eventually
# MAGIC produce (route/carrier delay aggregates from BTS data).

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS flights_historical_enriched (
    route STRING COMMENT 'e.g. ATL-ORD',
    carrier STRING COMMENT 'IATA carrier code',
    avg_delay_minutes DOUBLE,
    on_time_pct DOUBLE,
    sample_size INT
)
COMMENT 'PLACEHOLDER — will be overwritten by the Spark batch enrichment job.'
""")

spark.sql("""
INSERT OVERWRITE TABLE flights_historical_enriched VALUES
    ('ATL-ORD', 'UA', 14.2, 78.5, 3120),
    ('ATL-ORD', 'DL', 9.8,  84.1, 4560),
    ('JFK-LAX', 'AA', 21.6, 69.3, 2890),
    ('ATL-JFK', 'DL', 11.4, 81.0, 3810)
""")

display(spark.table("flights_historical_enriched"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Placeholder table: live_flight_status
# MAGIC Matches the shape the real Lakeflow streaming table will eventually
# MAGIC produce (from adsb.lol polling).

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS live_flight_status (
    flight_number STRING,
    flight_date DATE,
    status STRING COMMENT 'on_time | delayed | boarding | in_air | landed | cancelled',
    delay_minutes INT,
    gate STRING,
    last_updated TIMESTAMP
)
COMMENT 'PLACEHOLDER — will be overwritten by the Lakeflow Declarative Pipeline streaming table.'
""")

spark.sql("""
INSERT OVERWRITE TABLE live_flight_status VALUES
    ('UAL123', DATE'2026-10-01', 'on_time', 0,  'B12', current_timestamp()),
    ('DAL456', DATE'2026-10-01', 'delayed', 35, 'C4',  current_timestamp())
""")

display(spark.table("live_flight_status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. UC Function: get_historical_delay_stats
# MAGIC A table-valued SQL function — takes a route and/or carrier, returns
# MAGIC matching historical stats. `RETURNS TABLE` functions are exactly what
# MAGIC the managed UC Functions MCP server expects.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION get_historical_delay_stats(
    route_filter STRING COMMENT 'e.g. ATL-ORD, or NULL to skip this filter',
    carrier_filter STRING COMMENT 'IATA carrier code, or NULL to skip this filter'
)
RETURNS TABLE (
    route STRING,
    carrier STRING,
    avg_delay_minutes DOUBLE,
    on_time_pct DOUBLE,
    sample_size INT
)
COMMENT 'Historical on-time performance stats for a route and/or carrier, from BTS data.'
RETURN
    SELECT route, carrier, avg_delay_minutes, on_time_pct, sample_size
    FROM flights_historical_enriched
    WHERE (route_filter IS NULL OR route = route_filter)
      AND (carrier_filter IS NULL OR carrier = carrier_filter)
""")

# Quick test
display(spark.sql("SELECT * FROM get_historical_delay_stats('ATL-ORD', NULL)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. UC Function: get_flight_status

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION get_flight_status(
    flight_number_input STRING,
    flight_date_input STRING COMMENT 'YYYY-MM-DD'
)
RETURNS TABLE (
    flight_number STRING,
    flight_date DATE,
    status STRING,
    delay_minutes INT,
    gate STRING,
    last_updated TIMESTAMP
)
COMMENT 'Current live status for a specific flight, from adsb.lol via the Lakeflow streaming pipeline.'
RETURN
    SELECT flight_number, flight_date, status, delay_minutes, gate, last_updated
    FROM live_flight_status
    WHERE flight_number = flight_number_input
      AND flight_date = CAST(flight_date_input AS DATE)
""")

# Quick test
display(spark.sql("SELECT * FROM get_flight_status('UAL123', '2026-10-01')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Grant EXECUTE so the MCP server (and other users/agents) can call these

# COMMAND ----------

# Adjust the principal below — your own user, a group, or a service principal
# (e.g. the Databricks App's service principal client ID) as appropriate.
grant_to = dbutils.widgets.get("catalog")  # placeholder — replace with actual principal
spark.sql(f"GRANT EXECUTE ON FUNCTION get_historical_delay_stats TO `account users`")
spark.sql(f"GRANT EXECUTE ON FUNCTION get_flight_status TO `account users`")

print("Granted EXECUTE to account users. Narrow this to a specific group/principal for production use.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Find your managed UC Functions MCP server URL
# MAGIC In the Databricks UI: **Agents \u2192 MCP Servers**, or construct it directly:
# MAGIC
# MAGIC ```
# MAGIC https://<your-workspace-host>/api/2.0/mcp/functions/<catalog>/<schema>
# MAGIC ```
# MAGIC
# MAGIC Both functions created above will appear as tools on that server
# MAGIC automatically — no additional registration step. This URL is what
# MAGIC the LangGraph agent (built next) will connect to.

# COMMAND ----------

workspace_host = spark.conf.get("spark.databricks.workspaceUrl", "<your-workspace-host>")
print(f"Your UC Functions MCP server URL:\nhttps://{workspace_host}/api/2.0/mcp/functions/{catalog}/{schema}")
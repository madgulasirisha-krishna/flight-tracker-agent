# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Apply Lakebase Schema
# MAGIC Runs lakebase_schema.sql against your Lakebase instance and verifies all
# MAGIC tables were created. Run this from inside Databricks first (simpler auth
# MAGIC path) — you'll test the same connection from Render separately once
# MAGIC external network access is confirmed.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("Lakebase Connection URL", "", "Enter Value:")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# # 1. Initialize the Databricks Workspace Client
w = WorkspaceClient()

scope_name = "lakebase"
scopes = w.secrets.list_scopes()
scope_exists = any(s.name.lower() == scope_name.lower() for s in scopes) if scopes else False

if scope_exists:
    print(f"Scope '{scope_name}' already exists.")
else:
    print(f"Scope '{scope_name}' does not exist. Creating it now...")
    try:
    # Create the Databricks-backed scope
        w.secrets.create_scope(scope=scope_name)
        print(f"Scope '{scope_name}' created successfully!")
    except Exception as e:
        print(f"Error creating scope: {e}")

# 3. Put the secret string into the scope
w.secrets.put_secret(
    scope=scope_name,
    key="lakebase_connection_url",
    string_value=dbutils.widgets.get("Lakebase Connection URL")
)

dbutils.secrets.get(scope=scope_name, key="lakebase_connection_url")

# COMMAND ----------

# DBTITLE 1,Connect to Lakebase with OAuth credential
import psycopg2
import itertools

# 1. Discover the Lakebase project, branch, and endpoint
# projects = list(itertools.islice(w.postgres.list_projects(page_size=10), 10))
# for p in projects:
#     display = p.spec.display_name if p.spec else None
#     print(f"Project: {p.name} — {display}")

# Use the first project (update if you have multiple)
# project = projects[0]
# project_name = project.name  # e.g. "projects/<id>"

project_name = "projects/madgula-sirisha-capstone"

branches = list(w.postgres.list_branches(parent=project_name))
for b in branches:
    print(f"Branch: {b.name} — state: {b.status.current_state}")

branch = branches[0]
branch_name = branch.name  # e.g. "projects/<id>/branches/<id>"

endpoints = list(w.postgres.list_endpoints(parent=branch_name))
for ep in endpoints:
    print(f"Endpoint: {ep.name} — host: {ep.status.hosts.host}")

endpoint = endpoints[0]
endpoint_name = endpoint.name  # e.g. "projects/<id>/branches/<id>/endpoints/<id>"
host = endpoint.status.hosts.host

# 2. Generate a short-lived OAuth token (valid for 1 hour)
credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
username = w.current_user.me().user_name

# 3. Connect using the OAuth token as the password
conn = psycopg2.connect(
    host=host,
    port=5432,
    dbname="databricks_postgres",
    user=username,
    password=credential.token,
    sslmode="require"
)

cursor = conn.cursor()
cursor.execute("SELECT version();")
print("\nConnected successfully!")
print(cursor.fetchone())

# COMMAND ----------

# Pull the full connection URL from your secret scope.
LAKEBASE_CONNECTION_URL = dbutils.secrets.get(scope=scope_name, key="lakebase_connection_url")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the schema script
# MAGIC Upload lakebase_schema.sql to a workspace file or Unity Catalog volume
# MAGIC first, then point SCHEMA_SQL_PATH at it. Adjust the path below.

# COMMAND ----------

SCHEMA_SQL_PATH = "/Workspace/Users/madgula.sirisha@gmail.com/flight-tracker-agent/lakebase_schema.sql"  # <-- update this

with open(SCHEMA_SQL_PATH, "r") as f:
    schema_sql = f.read()

with conn.cursor() as cur:
    cur.execute(schema_sql)
conn.commit()
print("Schema applied successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify all tables exist

# COMMAND ----------

expected_tables = {
    "users", "watched_flights", "trip_plans",
    "trip_flights", "alerts", "agent_action_log",
}

with conn.cursor() as cur:
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    actual_tables = {row[0] for row in cur.fetchall()}

print("Tables found:", sorted(actual_tables))
missing = expected_tables - actual_tables
if missing:
    print(f"MISSING: {missing}")
else:
    print("All expected tables present.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick smoke test — insert and read back a test user
# MAGIC Confirms writes actually work, not just DDL.

# COMMAND ----------

with conn.cursor() as cur:
    cur.execute(
        "INSERT INTO users (email) VALUES (%s) RETURNING user_id;",
        ("test_user@example.com",),
    )
    test_user_id = cur.fetchone()[0]
conn.commit()

with conn.cursor() as cur:
    cur.execute("SELECT user_id, email, created_at FROM users WHERE user_id = %s;", (test_user_id,))
    print("Smoke test row:", cur.fetchone())

# Clean up the test row so it doesn't linger in your real data
with conn.cursor() as cur:
    cur.execute("DELETE FROM users WHERE user_id = %s;", (test_user_id,))
conn.commit()
print("Smoke test passed and cleaned up.")

# COMMAND ----------

conn.close()
"""
Flight Tracker & Trip-Watch Agent — Databricks App skeleton.

Purpose: prove out deployment + connectivity (Lakebase, Databricks SQL)
using ambient app-identity authentication before any real frontend logic
is built. Once this shows both connections green, replace the body of
main() with the actual chat panel / dashboard.

Both Lakebase and the SQL Warehouse must be added as App resources in
the Databricks Apps UI before this will connect successfully — that step
is what creates the app's service principal and grants it access.
"""

import os
import streamlit as st
import psycopg2
from databricks import sql as databricks_sql
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="Flight Tracker — Connectivity Check", page_icon="✈️")

workspace_client = WorkspaceClient()


def check_lakebase() -> tuple[bool, str]:
    """Attempts a simple SELECT 1 against Lakebase using a freshly
    generated OAuth database credential for this app's own service
    principal identity — no manually managed secret required."""
    try:
        endpoint_name = os.environ["LAKEBASE_ENDPOINT_NAME"]  # e.g. projects/.../branches/.../endpoints/...
        credential = workspace_client.postgres.generate_database_credential(endpoint=endpoint_name)

        conn = psycopg2.connect(
            host=os.environ["PGHOST"],
            port=os.environ.get("PGPORT", "5432"),
            dbname=os.environ.get("PGDATABASE", "databricks_postgres"),
            user=os.environ["PGUSER"],  # the app's service principal client ID
            password=credential.token,
            sslmode="require",
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        conn.close()
        return True, "Connected — SELECT 1 succeeded."
    except Exception as e:
        return False, f"Failed: {e}"


def check_databricks_sql() -> tuple[bool, str]:
    """Attempts a simple SELECT 1 against a Databricks SQL warehouse
    using the app's own ambient OAuth identity — no token to manage.
    Fetching a fresh token per call (rather than caching it) means this
    naturally handles the hourly expiry without extra refresh logic."""
    try:
        token = workspace_client.config.oauth_token().access_token
        http_path = f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}"

        with databricks_sql.connect(
            server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
            http_path=http_path,
            access_token=token,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchall()
        return True, "Connected — SELECT 1 succeeded."
    except Exception as e:
        return False, f"Failed: {e}"


def main():
    st.title("✈️ Flight Tracker — Connectivity Check")
    st.caption(
        "Skeleton deployment on Databricks Apps. Once Lakebase and Databricks "
        "SQL both show green below, this file gets replaced with the real "
        "chat + dashboard UI."
    )

    st.subheader("Lakebase")
    lakebase_ok, lakebase_msg = check_lakebase()
    (st.success if lakebase_ok else st.error)(lakebase_msg)

    st.subheader("Databricks SQL Warehouse")
    dbx_ok, dbx_msg = check_databricks_sql()
    (st.success if dbx_ok else st.error)(dbx_msg)

    st.divider()
    st.caption(
        "Auth is handled automatically via this app's service principal. "
        "LAKEBASE_ENDPOINT_NAME and DATABRICKS_WAREHOUSE_ID are auto-set in "
        "app.yaml via valueFrom, resolving your 'database' and "
        "'sql-warehouse' app resources."
    )


if __name__ == "__main__":
    main()


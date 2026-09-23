"""
dashboard.py — Queries the Lakebase Change Data Feed history tables
(lb_*_history) DIRECTLY and LIVE via the SQL Warehouse, computing each
metric fresh on every render rather than reading a pre-computed table.

This is what makes the CDF demo actually feel live: since Lakebase CDF
continuously syncs lb_agent_action_log_history, lb_watched_flights_history,
and lb_alerts_history in the background (no batch job involved in that
part), a write from the chat app followed by clicking Refresh here should
show up within moments — the same aggregation logic that
08_analytics_metrics.py materializes into a snapshot table, just run live
instead.
"""

import os

import pandas as pd
import streamlit as st
from databricks import sql as databricks_sql
from databricks.sdk import WorkspaceClient


def _run_query(sql: str) -> pd.DataFrame:
    workspace_client = WorkspaceClient()
    token = workspace_client.config.oauth_token().access_token
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    http_path = f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}"

    with databricks_sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _tables(catalog: str, schema: str) -> dict:
    return {
        "action_log": f"{catalog}.{schema}.lb_agent_action_log_history",
        "watched_flights": f"{catalog}.{schema}.lb_watched_flights_history",
        "alerts": f"{catalog}.{schema}.lb_alerts_history",
    }


def render_dashboard():
    st.subheader("📊 App & Agent Activity — Live")
    st.caption(
        "Queried directly from Lakebase Change Data Feed on every refresh — "
        "not a pre-computed snapshot. Use the chat, then hit Refresh to see it here."
    )

    catalog = os.environ.get("MCP_CATALOG", "bootcamp_students")
    schema = os.environ.get("MCP_SCHEMA", "madgula_sirisha_capstone")
    t = _tables(catalog, schema)

    refresh_clicked = st.button("🔄 Refresh now")
    st.caption(f"Last loaded: {pd.Timestamp.now().strftime('%H:%M:%S')}")

    try:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Tool calls per hour**")
            df = _run_query(f"""
                SELECT date_trunc('hour', created_at) AS hour, COUNT(*) AS tool_calls
                FROM {t['action_log']}
                WHERE _pg_change_type = 'insert'
                GROUP BY date_trunc('hour', created_at)
                ORDER BY hour
            """)
            if not df.empty:
                st.line_chart(df.set_index("hour")["tool_calls"])
            else:
                st.info("No tool calls yet — try the chat, then refresh.")

        with col2:
            st.markdown("**Alerts triggered per hour**")
            df = _run_query(f"""
                SELECT date_trunc('hour', _timestamp) AS hour, COUNT(*) AS alerts_triggered
                FROM {t['alerts']}
                WHERE _pg_change_type = 'update' AND status = 'triggered'
                GROUP BY date_trunc('hour', _timestamp)
                ORDER BY hour
            """)
            if not df.empty:
                st.bar_chart(df.set_index("hour")["alerts_triggered"])
            else:
                st.info("No alerts triggered yet.")

        col3, col4 = st.columns(2)

        with col3:
            st.markdown("**Tool success/error rate**")
            df = _run_query(f"""
                SELECT action_type, result_status, COUNT(*) AS event_count
                FROM {t['action_log']}
                WHERE _pg_change_type = 'insert'
                GROUP BY action_type, result_status
            """)
            if not df.empty:
                pivot = df.pivot_table(
                    index="action_type", columns="result_status",
                    values="event_count", fill_value=0,
                )
                st.bar_chart(pivot)
            else:
                st.info("No tool calls logged yet.")

        with col4:
            st.markdown("**Custom app tools vs. MCP tools**")
            df = _run_query(f"""
                SELECT
                    CASE WHEN action_type LIKE 'mcp::%' THEN 'mcp' ELSE 'custom' END AS tool_source,
                    COUNT(*) AS event_count
                FROM {t['action_log']}
                WHERE _pg_change_type = 'insert'
                GROUP BY CASE WHEN action_type LIKE 'mcp::%' THEN 'mcp' ELSE 'custom' END
            """)
            if not df.empty:
                st.bar_chart(df.set_index("tool_source")["event_count"])
            else:
                st.info("No data yet.")

        st.markdown("**Most-watched routes**")
        df = _run_query(f"""
            SELECT origin_airport, destination_airport, COUNT(*) AS watch_count
            FROM {t['watched_flights']}
            WHERE _pg_change_type = 'insert'
              AND origin_airport IS NOT NULL AND destination_airport IS NOT NULL
            GROUP BY origin_airport, destination_airport
            ORDER BY watch_count DESC
            LIMIT 20
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No watched flights yet — try 'watch flight UAL123 on 2026-10-01' in chat.")

        st.markdown("**Recent activity (raw log, newest first)**")
        df = _run_query(f"""
            SELECT created_at, action_type, result_status
            FROM {t['action_log']}
            WHERE _pg_change_type = 'insert'
            ORDER BY created_at DESC
            LIMIT 10
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Couldn't load analytics: {e}")
        st.caption(
            "Check that lb_agent_action_log_history, lb_watched_flights_history, "
            "and lb_alerts_history exist (Lakebase Change Data Feed set up earlier)."
        )

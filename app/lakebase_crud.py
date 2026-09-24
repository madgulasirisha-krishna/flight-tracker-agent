"""
lakebase_crud.py — Direct database operations against Lakebase.

Connection strategy:
- Inside the Databricks App (LAKEBASE_ENDPOINT_NAME set via app.yaml's
  valueFrom: database), uses the app's ambient service-principal identity
  via WorkspaceClient().postgres.generate_database_credential(). A fresh
  credential is fetched per connection rather than cached, since these
  expire hourly — this naturally handles rotation with no extra logic.
- For local/notebook testing outside the App, falls back to
  LAKEBASE_CONNECTION_URL (the app_user native-password credential),
  read from a Databricks secret scope or a local env var.

Every function returns plain dicts/lists (via RealDictCursor) so results
serialize directly to JSON for the agent's tool-call responses.
"""

import os
import uuid
import json
from datetime import date
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor


@contextmanager
def get_connection():
    """Yields a live Lakebase connection, closing it on exit."""
    endpoint_name = os.environ.get("LAKEBASE_ENDPOINT_NAME")

    if endpoint_name:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
        credential = client.postgres.generate_database_credential(endpoint=endpoint_name)
        conn = psycopg2.connect(
            host=os.environ["PGHOST"],
            port=os.environ.get("PGPORT", "5432"),
            dbname=os.environ.get("PGDATABASE", "databricks_postgres"),
            user=os.environ["PGUSER"],
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


def _fetch_one(cur):
    row = cur.fetchone()
    return dict(row) if row else None


def _fetch_all(cur):
    return [dict(row) for row in cur.fetchall()]


# ============================================================
# users
# ============================================================

def get_or_create_user(email: str) -> dict:
    """Returns the existing user row for this email, or creates one."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE email = %s;", (email,))
            existing = _fetch_one(cur)
            if existing:
                return existing

            cur.execute(
                "INSERT INTO users (email) VALUES (%s) RETURNING *;",
                (email,),
            )
            new_user = _fetch_one(cur)
        conn.commit()
        return new_user


# ============================================================
# watched_flights
# ============================================================

def add_flight_watch(
    user_id: str,
    flight_number: str,
    flight_date: str,
    origin_airport: str | None = None,
    destination_airport: str | None = None,
) -> dict:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO watched_flights
                    (user_id, flight_number, flight_date, origin_airport, destination_airport)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING *;
                """,
                (user_id, flight_number, flight_date, origin_airport, destination_airport),
            )
            row = _fetch_one(cur)
        conn.commit()
        return row


def remove_flight_watch(watch_id: str) -> dict:
    """Soft delete — sets status to 'cancelled' rather than deleting the row,
    preserving history for the analytics pipeline."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE watched_flights
                SET status = 'cancelled', updated_at = now()
                WHERE watch_id = %s
                RETURNING *;
                """,
                (watch_id,),
            )
            row = _fetch_one(cur)
        conn.commit()
        if row is None:
            raise ValueError(f"No watched_flights row found for watch_id={watch_id}")
        return row


def list_watched_flights(user_id: str, status: str = "active") -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM watched_flights
                WHERE user_id = %s AND status = %s
                ORDER BY flight_date ASC;
                """,
                (user_id, status),
            )
            return _fetch_all(cur)


# ============================================================
# trip_plans / trip_flights
# ============================================================

def create_trip(user_id: str, trip_name: str, watch_ids: list[str]) -> dict:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO trip_plans (user_id, trip_name) VALUES (%s, %s) RETURNING *;",
                (user_id, trip_name),
            )
            trip = _fetch_one(cur)

            for watch_id in watch_ids:
                cur.execute(
                    "INSERT INTO trip_flights (trip_id, watch_id) VALUES (%s, %s);",
                    (trip["trip_id"], watch_id),
                )
        conn.commit()
        trip["flight_count"] = len(watch_ids)
        return trip


def get_trip_summary(trip_id: str) -> dict:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM trip_plans WHERE trip_id = %s;", (trip_id,))
            trip = _fetch_one(cur)
            if trip is None:
                raise ValueError(f"No trip_plans row found for trip_id={trip_id}")

            cur.execute(
                """
                SELECT wf.* FROM watched_flights wf
                JOIN trip_flights tf ON tf.watch_id = wf.watch_id
                WHERE tf.trip_id = %s
                ORDER BY wf.flight_date ASC;
                """,
                (trip_id,),
            )
            trip["flights"] = _fetch_all(cur)
        return trip


# ============================================================
# alerts
# ============================================================

def set_alert(watch_id: str, alert_type: str, threshold_minutes: int | None = None) -> dict:
    valid_types = {"delay_threshold", "gate_change", "status_change", "cancellation"}
    if alert_type not in valid_types:
        raise ValueError(f"alert_type must be one of {valid_types}, got {alert_type!r}")

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO alerts (watch_id, alert_type, threshold_minutes)
                VALUES (%s, %s, %s)
                RETURNING *;
                """,
                (watch_id, alert_type, threshold_minutes),
            )
            row = _fetch_one(cur)
        conn.commit()
        return row


def cancel_alert(alert_id: str) -> dict:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE alerts
                SET status = 'cancelled'
                WHERE alert_id = %s
                RETURNING *;
                """,
                (alert_id,),
            )
            row = _fetch_one(cur)
        conn.commit()
        if row is None:
            raise ValueError(f"No alerts row found for alert_id={alert_id}")
        return row


# ============================================================
# alerts (read)
# ============================================================
 
def list_alerts(user_id: str | None = None, status: str | None = None) -> list[dict]:
    """Lists alerts, joined with watched_flights (for flight context) and
    users (for email). user_id=None lists across all users — used by the
    dashboard for an app-wide view; a specific user_id scopes it to one
    person — used by the list_my_alerts tool. status=None returns every
    status; pass 'triggered' or 'active' to filter."""
    query = """
        SELECT a.alert_id, a.alert_type, a.threshold_minutes, a.status,
               a.triggered_at, a.created_at,
               wf.flight_number, wf.flight_date, u.email
        FROM alerts a
        JOIN watched_flights wf ON wf.watch_id = a.watch_id
        JOIN users u ON u.user_id = wf.user_id
        WHERE 1=1
    """
    params = []
    if user_id:
        query += " AND wf.user_id = %s"
        params.append(user_id)
    if status:
        query += " AND a.status = %s"
        params.append(status)
    query += " ORDER BY a.created_at DESC;"
 
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, tuple(params))
            return _fetch_all(cur)

# ============================================================
# agent_action_log
# ============================================================

def log_agent_action(
    user_id: str,
    action_type: str,
    action_payload: dict,
    result_status: str,
) -> dict:
    """Every tool call — read or write, success or error — writes one row
    here. This is the sole source for the CDF-based analytics pipeline, so
    it must be called for every tool invocation without exception."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO agent_action_log (user_id, action_type, action_payload, result_status)
                VALUES (%s, %s, %s, %s)
                RETURNING *;
                """,
                (user_id, action_type, json.dumps(action_payload), result_status),
            )
            row = _fetch_one(cur)
        conn.commit()
        return row

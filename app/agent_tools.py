"""
agent_tools.py — Tool schemas + implementations for the flight tracker agent.

Tool status:
  REAL   — fully wired against Lakebase, working today.
  STUBBED — returns realistic mock data; the function signature and return
            shape are final, but the body needs to be swapped for a real
            query once its upstream pipeline exists:
              - get_flight_status          -> Lakeflow streaming table (live_flight_status)
              - get_historical_delay_stats -> Spark-enriched BTS table (flights_historical_enriched)
              - search_passenger_rights    -> Databricks Vector Search index (RAG pipeline)

Every tool call — regardless of status, success or failure — is logged to
agent_action_log via the @logged_tool decorator, so this doesn't need to
be remembered per-tool; it happens automatically.
"""

import functools

import lakebase_crud as db


# ============================================================
# Logging wrapper — every tool call goes through this
# ============================================================

def logged_tool(action_type: str):
    """Decorator: runs the wrapped tool function, then logs the call
    (success or error) to agent_action_log. The wrapped function must
    accept `user_id` as its first argument."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(user_id: str, **kwargs):
            try:
                result = func(user_id, **kwargs)
                db.log_agent_action(
                    user_id=user_id,
                    action_type=action_type,
                    action_payload=kwargs,
                    result_status="success",
                )
                return result
            except Exception as e:
                db.log_agent_action(
                    user_id=user_id,
                    action_type=action_type,
                    action_payload={**kwargs, "error": str(e)},
                    result_status="error",
                )
                raise

        return wrapper

    return decorator


# ============================================================
# WRITE TOOLS (real, wired against Lakebase)
# ============================================================

@logged_tool("add_flight_watch")
def add_flight_watch(user_id: str, flight_number: str, flight_date: str,
                      origin_airport: str = None, destination_airport: str = None) -> dict:
    watch = db.add_flight_watch(user_id, flight_number, flight_date, origin_airport, destination_airport)
    return {"watch_id": watch["watch_id"], "flight_number": watch["flight_number"],
            "flight_date": str(watch["flight_date"]), "status": watch["status"]}


@logged_tool("remove_flight_watch")
def remove_flight_watch(user_id: str, watch_id: str) -> dict:
    watch = db.remove_flight_watch(watch_id)
    return {"watch_id": watch["watch_id"], "status": watch["status"]}


@logged_tool("create_trip")
def create_trip(user_id: str, trip_name: str, watch_ids: list) -> dict:
    trip = db.create_trip(user_id, trip_name, watch_ids)
    return {"trip_id": trip["trip_id"], "trip_name": trip["trip_name"],
            "flight_count": trip["flight_count"]}


@logged_tool("set_alert")
def set_alert(user_id: str, watch_id: str, alert_type: str, threshold_minutes: int = None) -> dict:
    alert = db.set_alert(watch_id, alert_type, threshold_minutes)
    return {"alert_id": alert["alert_id"], "alert_type": alert["alert_type"], "status": alert["status"]}


@logged_tool("cancel_alert")
def cancel_alert(user_id: str, alert_id: str) -> dict:
    alert = db.cancel_alert(alert_id)
    return {"alert_id": alert["alert_id"], "status": alert["status"]}


# ============================================================
# READ TOOLS (real, wired against Lakebase)
# ============================================================

@logged_tool("list_my_watched_flights")
def list_my_watched_flights(user_id: str) -> dict:
    flights = db.list_watched_flights(user_id)
    return {"count": len(flights), "flights": [
        {"watch_id": f["watch_id"], "flight_number": f["flight_number"],
         "flight_date": str(f["flight_date"]), "status": f["status"]}
        for f in flights
    ]}


@logged_tool("get_trip_summary")
def get_trip_summary(user_id: str, trip_id: str) -> dict:
    trip = db.get_trip_summary(trip_id)
    return {"trip_id": trip["trip_id"], "trip_name": trip["trip_name"],
            "flights": [{"flight_number": f["flight_number"], "flight_date": str(f["flight_date"]),
                         "status": f["status"]} for f in trip["flights"]]}


# ============================================================
# NOTE: get_flight_status, get_historical_delay_stats, and
# search_passenger_rights previously lived here as stubbed functions.
# They're now provided for real by Databricks-managed MCP servers
# (UC Functions and AI Search, respectively) instead — see
# langgraph_agent.py. Removed here to avoid two different
# implementations of the same tool name existing at once.
# ============================================================


# ============================================================
# Tool schemas (Claude-style tool-calling format)
# ============================================================

TOOL_SCHEMAS = [
    {
        "name": "add_flight_watch",
        "description": "Start watching a flight for status updates and alerts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "flight_number": {"type": "string", "description": "e.g. 'UA123'"},
                "flight_date": {"type": "string", "description": "YYYY-MM-DD"},
                "origin_airport": {"type": "string", "description": "IATA code, optional"},
                "destination_airport": {"type": "string", "description": "IATA code, optional"},
            },
            "required": ["flight_number", "flight_date"],
        },
    },
    {
        "name": "remove_flight_watch",
        "description": "Stop watching a flight.",
        "input_schema": {
            "type": "object",
            "properties": {"watch_id": {"type": "string"}},
            "required": ["watch_id"],
        },
    },
    {
        "name": "create_trip",
        "description": "Group one or more watched flights into a named trip.",
        "input_schema": {
            "type": "object",
            "properties": {
                "trip_name": {"type": "string"},
                "watch_ids": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["trip_name", "watch_ids"],
        },
    },
    {
        "name": "set_alert",
        "description": "Create an alert rule on a watched flight.",
        "input_schema": {
            "type": "object",
            "properties": {
                "watch_id": {"type": "string"},
                "alert_type": {
                    "type": "string",
                    "enum": ["delay_threshold", "gate_change", "status_change", "cancellation"],
                },
                "threshold_minutes": {
                    "type": "integer",
                    "description": "Required only for alert_type=delay_threshold",
                },
            },
            "required": ["watch_id", "alert_type"],
        },
    },
    {
        "name": "cancel_alert",
        "description": "Cancel an existing alert rule.",
        "input_schema": {
            "type": "object",
            "properties": {"alert_id": {"type": "string"}},
            "required": ["alert_id"],
        },
    },
    {
        "name": "list_my_watched_flights",
        "description": "List all flights the current user is actively watching.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_trip_summary",
        "description": "Get all flights grouped under a specific trip.",
        "input_schema": {
            "type": "object",
            "properties": {"trip_id": {"type": "string"}},
            "required": ["trip_id"],
        },
    },
]


# ============================================================
# Dispatcher — routes a tool-call by name to its implementation
# ============================================================

_TOOL_FUNCTIONS = {
    "add_flight_watch": add_flight_watch,
    "remove_flight_watch": remove_flight_watch,
    "create_trip": create_trip,
    "set_alert": set_alert,
    "cancel_alert": cancel_alert,
    "list_my_watched_flights": list_my_watched_flights,
    "get_trip_summary": get_trip_summary,
}


def execute_tool(tool_name: str, tool_input: dict, user_id: str) -> dict:
    """Single entry point the agent loop calls for every tool invocation."""
    if tool_name not in _TOOL_FUNCTIONS:
        raise ValueError(f"Unknown tool: {tool_name}")
    return _TOOL_FUNCTIONS[tool_name](user_id=user_id, **tool_input)

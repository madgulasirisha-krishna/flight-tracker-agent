"""
langgraph_agent.py — The LangGraph-based agent, replacing the hand-rolled
loop in agent_loop.py.

Binds together two different kinds of tools into one agent:
  1. Custom Python tools — direct Lakebase writes/reads (add_flight_watch,
     remove_flight_watch, create_trip, set_alert, cancel_alert,
     list_my_watched_flights, get_trip_summary). These can't be managed
     MCP tools since Lakehouse Federation to Lakebase is read-only.
  2. Databricks-managed MCP tools — loaded live from two managed MCP
     servers, no custom server code required:
       - UC Functions server: get_historical_delay_stats, get_flight_status
         (SQL functions over Delta tables, from 04_create_uc_functions.py)
       - AI Search server: passenger-rights document search (from the
         Vector Search index built in 06/07)

MCP support here uses langchain.mcp (built into LangChain 1.4+, on top of
FastMCP) — NOT the older standalone langchain-mcp-adapters package, which
is now superseded and has incompatible dependency pins. langchain.mcp is
in beta; importing it raises a LangChainBetaWarning, which is expected
and safe to ignore.

Model: a Databricks-hosted Claude model via Foundation Model APIs,
wrapped with ChatDatabricks (databricks-langchain) — same ambient
service-principal identity as everywhere else in this app, no separate
API key.

Every tool call is logged to agent_action_log:
  - Custom Lakebase tools log themselves via agent_tools.py's
    @logged_tool decorator.
  - MCP tools are logged centrally via a @wrap_tool_call middleware
    (the officially documented interception point in langchain.agents),
    which distinguishes MCP tools from custom ones via
    tool.metadata["mcp"] to avoid double-logging the custom ones.
"""

import os
import warnings

from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
from langchain.agents import create_agent
from langchain.agents.middleware import wrap_tool_call
from langchain.tools import tool

import agent_tools
import lakebase_crud as db

# langchain.mcp is beta; the warning is expected and not actionable here.
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from langchain.mcp import MCPAdapter

from fastmcp.client import Client as FastMCPClient
from fastmcp.client.transports import StreamableHttpTransport

MODEL_ENDPOINT = os.environ.get("LLM_MODEL_NAME", "databricks-claude-sonnet-4-6")

SYSTEM_PROMPT = """You are the Flight Tracker & Trip-Watch assistant. You help \
users watch flights, manage alerts, organize trips, check flight status and \
historical delay stats, and answer passenger-rights questions.
 
Your tools come from two places:
- App tools (watching flights, alerts, trips) act directly on the user's data.
- Databricks-managed tools look up historical delay stats, live flight status, \
and passenger-rights documents (DOT guidance, airline Contracts of Carriage).
 
Rules:
- Always use a tool to look up or change real data rather than guessing.
- Neither your live status data nor your historical stats give you a
  computed delay for a specific flight — adsb.lol has no schedule data,
  and BTS stats are historical route/carrier averages, not this flight's
  schedule. When asked if a flight is delayed, call both
  get_flight_status (current position: on_ground/in_air) and
  get_historical_delay_stats (the route/carrier's typical performance),
  present both, and be explicit that you can't state an actual delay
  for this specific flight — only its current physical state and how
  that route/carrier usually performs.
- For passenger-rights questions, use the document search tool and cite the \
source document/section in your answer.
- Confirm the details of any write action (e.g. which flight, which alert) back \
to the user in your final reply, since these are real changes to their data.
- Keep replies concise and conversational."""


def _get_workspace_host_and_token() -> tuple[str, str]:
    workspace_client = WorkspaceClient()
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    token = workspace_client.config.oauth_token().access_token
    return host, token


async def _load_tools_from_server(url: str, token: str) -> list:
    """Loads tools from one managed MCP server. Tools returned by
    list_tools() manage their own MCP session per call internally, so
    they remain usable after this function returns (the `async with`
    block only needs to be open for the listing call itself)."""
    transport = StreamableHttpTransport(url, headers={"Authorization": f"Bearer {token}"})
    fastmcp_client = FastMCPClient(transport)
    async with MCPAdapter(fastmcp_client) as adapter:
        return await adapter.list_tools()


async def load_mcp_tools() -> list:
    """Fetches the current tool list from both managed MCP servers. Call
    this once per session and cache the result — it's a network
    round-trip, not something to redo on every message."""
    host, token = _get_workspace_host_and_token()
    catalog = os.environ.get("MCP_CATALOG", "bootcamp_students")
    schema = os.environ.get("MCP_SCHEMA", "madgula_sirisha_capstone")

    uc_functions_url = f"https://{host}/api/2.0/mcp/functions/{catalog}/{schema}"
    ai_search_url = f"https://{host}/api/2.0/mcp/ai-search/{catalog}/{schema}"

    uc_tools = await _load_tools_from_server(uc_functions_url, token)
    search_tools = await _load_tools_from_server(ai_search_url, token)
    return uc_tools + search_tools


def build_lakebase_tools(user_id: str) -> list:
    """Wraps agent_tools.py's real Lakebase functions as LangChain tools,
    binding user_id via closure so it's never a model-visible parameter —
    the model should never choose whose data to act on. These already
    log to agent_action_log via @logged_tool inside agent_tools.py."""

    @tool
    def add_flight_watch(flight_number: str, flight_date: str,
                          origin_airport: str = None, destination_airport: str = None) -> dict:
        """Start watching a flight for status updates and alerts."""
        return agent_tools.add_flight_watch(
            user_id=user_id, flight_number=flight_number, flight_date=flight_date,
            origin_airport=origin_airport, destination_airport=destination_airport,
        )

    @tool
    def remove_flight_watch(watch_id: str) -> dict:
        """Stop watching a flight."""
        return agent_tools.remove_flight_watch(user_id=user_id, watch_id=watch_id)

    @tool
    def create_trip(trip_name: str, watch_ids: list) -> dict:
        """Group one or more watched flights into a named trip."""
        return agent_tools.create_trip(user_id=user_id, trip_name=trip_name, watch_ids=watch_ids)

    @tool
    def set_alert(watch_id: str, alert_type: str, threshold_minutes: int = None) -> dict:
        """Create an alert rule on a watched flight. alert_type is one of:
        delay_threshold, gate_change, status_change, cancellation."""
        return agent_tools.set_alert(
            user_id=user_id, watch_id=watch_id, alert_type=alert_type,
            threshold_minutes=threshold_minutes,
        )

    @tool
    def cancel_alert(alert_id: str) -> dict:
        """Cancel an existing alert rule."""
        return agent_tools.cancel_alert(user_id=user_id, alert_id=alert_id)

    @tool
    def list_my_watched_flights() -> dict:
        """List all flights the current user is actively watching."""
        return agent_tools.list_my_watched_flights(user_id=user_id)

    @tool
    def get_trip_summary(trip_id: str) -> dict:
        """Get all flights grouped under a specific trip."""
        return agent_tools.get_trip_summary(user_id=user_id, trip_id=trip_id)
    
    @tool
    def list_my_alerts() -> dict:
        """List all alerts for the current user, including their status
        (active/triggered/cancelled) and which flight each is attached to."""
        return agent_tools.list_my_alerts(user_id=user_id)

    return [
        add_flight_watch, remove_flight_watch, create_trip,
        set_alert, cancel_alert, list_my_watched_flights, get_trip_summary,
        list_my_alerts,
    ]


def _sanitize_tool_message(message):
    """MCP tool results sometimes carry extra fields (e.g. an `id` on a
    text content block) that Anthropic's strict schema validation rejects
    with 'Extra inputs are not permitted'. Strips content blocks down to
    only the fields Anthropic's Messages API actually allows."""
    content = getattr(message, "content", None)
    if not isinstance(content, list):
        return message

    cleaned = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            cleaned.append({"type": "text", "text": block.get("text", "")})
        else:
            cleaned.append(block)

    try:
        return message.model_copy(update={"content": cleaned})
    except Exception:
        message.content = cleaned
        return message


def build_logging_middleware(user_id: str):
    """Logs every MCP tool call to agent_action_log via the officially
    documented @wrap_tool_call interception point, and sanitizes tool
    result content (see _sanitize_tool_message) before it reaches the
    model. Custom Lakebase tools are skipped for logging here (via the
    tool's metadata["mcp"] check) since they already log themselves —
    this avoids double-logging the same call."""

    @wrap_tool_call
    async def log_mcp_tool_calls(request, handler):
        tool_name = request.tool_call.get("name", "unknown")
        tool_args = request.tool_call.get("args", {})
        is_mcp = bool(getattr(request.tool, "metadata", None) and request.tool.metadata.get("mcp"))

        try:
            result = await handler(request)
        except Exception as e:
            if is_mcp:
                db.log_agent_action(
                    user_id=user_id,
                    action_type=f"mcp::{tool_name}",
                    action_payload={**tool_args, "error": str(e)},
                    result_status="error",
                )
            raise

        result = _sanitize_tool_message(result)

        if is_mcp:
            db.log_agent_action(
                user_id=user_id,
                action_type=f"mcp::{tool_name}",
                action_payload=tool_args,
                result_status="error" if getattr(result, "status", None) == "error" else "success",
            )
        return result

    return log_mcp_tool_calls


def build_model() -> ChatDatabricks:
    """A Databricks-hosted Claude model via Foundation Model APIs — same
    ambient identity as the rest of this app, no separate API key."""
    return ChatDatabricks(endpoint=MODEL_ENDPOINT, temperature=0.2, max_tokens=1024)


async def build_agent(user_id: str):
    """Assembles the full agent: model + custom Lakebase tools + both
    managed MCP tool sets + the logging middleware. Call once per session
    (tools don't change mid-conversation) and reuse across turns."""
    mcp_tools = await load_mcp_tools()
    lakebase_tools = build_lakebase_tools(user_id)
    all_tools = lakebase_tools + mcp_tools

    model = build_model()
    return create_agent(
        model, all_tools,
        system_prompt=SYSTEM_PROMPT,
        middleware=[build_logging_middleware(user_id)],
    )


async def run_agent_turn(agent, messages: list) -> list:
    """Runs one turn of the conversation. `messages` is a plain list of
    {"role": ..., "content": ...} dicts; returns the updated list including
    the assistant's reply. Async throughout since MCP tool calls are
    async-only — call this via asyncio.run() from sync code (e.g.
    Streamlit)."""
    result = await agent.ainvoke({"messages": messages})
    return result["messages"]


def get_latest_text_reply(messages: list) -> str:
    """Extracts the text of the most recent assistant message."""
    for message in reversed(messages):
        role = message.get("role") if isinstance(message, dict) else getattr(message, "type", None)
        if role in ("assistant", "ai"):
            content = message.get("content") if isinstance(message, dict) else message.content
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                return "".join(
                    block.get("text", "") for block in content
                    if isinstance(block, dict) and block.get("type") == "text"
                )
    return ""

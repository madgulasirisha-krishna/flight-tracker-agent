# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Agent Evaluation (MCP-independent version)
# MAGIC Evaluates the agent's real behavior WITHOUT importing langchain.mcp,
# MAGIC after hitting a confirmed, currently-unresolved upstream
# MAGIC incompatibility between the installed `mcp` package (which renamed
# MAGIC RequestContext away in mcp/shared/context.py) and `fastmcp`/
# MAGIC `langchain.mcp`'s beta code, which still expects the old name. This
# MAGIC is a real bug in that dependency chain at its currently-published
# MAGIC versions, not something fixable via pinning from our side — verified
# MAGIC by inspecting mcp.shared.context directly rather than guessing further.
# MAGIC
# MAGIC Two evaluation strategies, split by whether the tool needs MCP:
# MAGIC   1. Write-action tools (add_flight_watch, set_alert, etc.) never
# MAGIC      depended on MCP — build_lakebase_tools() is pure Lakebase code.
# MAGIC      These get evaluated with a REAL agent, real tool execution.
# MAGIC   2. RAG / historical-stats / live-status cases (which DO need the
# MAGIC      broken MCP servers) are evaluated by simulating a realistic
# MAGIC      tool result directly in the conversation, then scoring the
# MAGIC      model's response synthesis — a legitimate way to test "does the
# MAGIC      model reason/cite/avoid fabrication well" independent of
# MAGIC      whether retrieval itself is currently working in this notebook.
# MAGIC
# MAGIC IMPORTANT: strategy 1 actually invokes real write tools. Uses a
# MAGIC dedicated eval-test@example.com user to avoid polluting real data.

# COMMAND ----------

_requirements = """
mlflow[databricks]>=3.1
nest_asyncio
psycopg2-binary
databricks-sdk
databricks-langchain
langgraph>=0.2.0
langchain>=1.4.0
"""
with open("/tmp/eval_requirements.txt", "w") as f:
    f.write(_requirements)

# COMMAND ----------

# MAGIC %pip install -r /tmp/eval_requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
sys.path.append("/Workspace/Users/madgula.sirisha@gmail.com/flight-tracker-agent/app")

import asyncio
import nest_asyncio
nest_asyncio.apply()

import mlflow
from mlflow.genai.scorers import Correctness, Guidelines, RelevanceToQuery, Safety
from databricks.sdk import WorkspaceClient
from langchain.agents import create_agent

_w = WorkspaceClient()
os.environ["LAKEBASE_ENDPOINT_NAME"] = "projects/madgula-sirisha-capstone/branches/production/endpoints/primary"
os.environ["PGHOST"] = "ep-shiny-morning-d1a15kd9.database.us-west-2.cloud.databricks.com"
os.environ["PGUSER"] = _w.current_user.me().user_name
os.environ["DATABRICKS_HOST"] = _w.config.host

import lakebase_crud as db
# Import ONLY what doesn't touch langchain.mcp — build_lakebase_tools and
# build_model are pure Lakebase/ChatDatabricks code with no MCP import.
from langgraph_agent import build_lakebase_tools, build_model, SYSTEM_PROMPT, get_latest_text_reply

mlflow.set_experiment("/Shared/flight-tracker-agent-eval")

# COMMAND ----------

eval_user = db.get_or_create_user("eval-test@example.com")
eval_user_id = eval_user["user_id"]
print(f"Eval user_id: {eval_user_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 1: real agent, MCP-free, for write-action tools

# COMMAND ----------

_write_only_tools = build_lakebase_tools(eval_user_id)
_write_only_agent = create_agent(build_model(), _write_only_tools, system_prompt=SYSTEM_PROMPT)

def predict_write_action(request: str) -> dict:
    messages = [{"role": "user", "content": request}]
    result = _write_only_agent.invoke({"messages": messages})
    return {"response": get_latest_text_reply(result["messages"])}

# Quick sanity check before running the full eval
print(predict_write_action("Watch flight UAL999 on 2026-12-01"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 2: simulated tool context, for MCP-dependent behaviors
# MAGIC Directly invokes the model with a realistic tool_result already in
# MAGIC the conversation, then scores the model's final synthesized answer.

# COMMAND ----------

_model = build_model()

def predict_with_simulated_tool_result(request: str, simulated_tool_name: str, simulated_tool_output: str) -> dict:
    """Simplified to a single natural turn rather than faking a
    multi-turn tool-calling exchange — an earlier version used a plain-
    text placeholder like '[Calling get_flight_status...]' as a fake
    assistant message, and the model literally imitated that bracket-
    narration style in its own next turn instead of giving a real answer,
    since it wasn't a properly-structured tool call the model recognizes.
    This version just tells the model what it already knows and asks it
    to answer, avoiding that failure mode entirely."""
    combined_prompt = (
        f"The user asked: \"{request}\"\n\n"
        f"You already have this information available (from {simulated_tool_name}), "
        f"no need to call any tool: {simulated_tool_output}\n\n"
        f"Answer the user's question directly using this information, following your rules."
    )
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": combined_prompt},
    ]
    response = _model.invoke(messages)
    return {"response": response.content}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluation dataset

# COMMAND ----------

write_action_cases = [
    {
        "inputs": {"request": "Watch flight UAL999 on 2026-12-01"},
        "expectations": {
            "expected_facts": [
                "A flight watch was created for UAL999",
                "The watch is for the date 2026-12-01",
            ]
        },
    },
]

simulated_cases = [
    {
        "request": "What are my rights if my flight gets cancelled? Can I get a refund?",
        "simulated_tool_name": "search_passenger_rights",
        "simulated_tool_output": (
            "[Source: DOT Fly Rights guide, Refunds section] Passengers are entitled to a "
            "refund if their flight is cancelled, regardless of the reason for the cancellation, "
            "if they choose not to travel on the rebooked flight offered by the airline."
        ),
        "expected_facts": [
            "Passengers may be entitled to a refund for a cancelled flight",
            "The response cites the DOT Fly Rights guide as the source",
        ],
    },
    {
        "request": "Is my UAL999 flight on 2026-12-01 delayed right now?",
        "simulated_tool_name": "get_flight_status",
        "simulated_tool_output": '{"flight_number": "UAL999", "status": "in_air", "delay_minutes": null, "gate": null}',
        "expected_facts": [
            "The response does not state a specific number of minutes of delay for this flight",
            "The response explains that real-time delay data isn't available for this specific flight",
        ],
    },
    {
        "request": "How does UA typically perform on the ATL-ORD route historically?",
        "simulated_tool_name": "get_historical_delay_stats",
        "simulated_tool_output": '{"route": "ATL-ORD", "carrier": "UA", "avg_delay_minutes": 14.2, "on_time_pct": 78.5}',
        "expected_facts": [
            "The response gives historical on-time performance or average delay statistics",
            "The stats are described as historical/average, not a real-time figure",
        ],
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scorers

# COMMAND ----------

no_fake_delay_guideline = Guidelines(
    name="no_fabricated_delay",
    guidelines=(
        "If the response discusses whether a specific flight is delayed, it must NOT "
        "state a specific number of minutes of delay for that flight unless the "
        "conversation already established real, measured delay data for it. It's "
        "acceptable to mention historical average delay statistics for a route/carrier, "
        "but not to imply that number applies to this specific flight's current delay."
    ),
)

citation_guideline = Guidelines(
    name="cites_sources_for_policy_questions",
    guidelines=(
        "If the response answers a passenger-rights or policy question (refunds, "
        "compensation, cancellations), it must reference a specific source document "
        "or section, not present the answer as unsourced general knowledge."
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run evaluation 1: write actions (real agent)

# COMMAND ----------

write_results = mlflow.genai.evaluate(
    data=write_action_cases,
    predict_fn=predict_write_action,
    scorers=[Correctness(), RelevanceToQuery(), Safety()],
)
print(write_results.metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run evaluation 2: simulated-context reasoning quality

# COMMAND ----------

simulated_eval_data = [
    {
        "inputs": {
            "request": c["request"],
            "simulated_tool_name": c["simulated_tool_name"],
            "simulated_tool_output": c["simulated_tool_output"],
        },
        "expectations": {"expected_facts": c["expected_facts"]},
    }
    for c in simulated_cases
]

simulated_results = mlflow.genai.evaluate(
    data=simulated_eval_data,
    predict_fn=predict_with_simulated_tool_result,
    scorers=[Correctness(), RelevanceToQuery(), Safety(), no_fake_delay_guideline, citation_guideline],
)
print(simulated_results.metrics)
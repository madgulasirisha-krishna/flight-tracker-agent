"""
Flight Tracker & Trip-Watch Agent — main chat interface.

Now backed by the LangGraph agent (langgraph_agent.py), which binds
custom Lakebase tools together with two Databricks-managed MCP servers
(UC Functions, AI Search) — replacing the earlier hand-rolled tool loop
in agent_loop.py.
"""

import asyncio
import streamlit as st

import lakebase_crud as db
from langgraph_agent import build_agent, run_agent_turn, get_latest_text_reply
from dashboard import render_dashboard

st.set_page_config(page_title="Flight Tracker Agent", page_icon="✈️")
st.title("✈️ Flight Tracker & Trip-Watch Agent")
st.caption(
    "Watch flights, get alerts, manage trips, and ask about your passenger rights."
)

# ============================================================
# User identification
# ============================================================

with st.sidebar:
    st.subheader("Session")
    email = st.text_input("Your email", value=st.session_state.get("email", "demo@example.com"))

    if email != st.session_state.get("email"):
        st.session_state.email = email
        st.session_state.user = db.get_or_create_user(email)
        st.session_state.messages = []
        st.session_state.agent = None  # force rebuild for the new user_id

    if "user" not in st.session_state:
        st.session_state.user = db.get_or_create_user(email)
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "agent" not in st.session_state:
        st.session_state.agent = None

    user_id = st.session_state.user["user_id"]
    st.caption(f"user_id: `{user_id}`")

    if st.button("Clear conversation"):
        st.session_state.messages = []
        st.rerun()

    with st.expander("Connection status (debug)"):
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
            st.success("Lakebase: connected")
        except Exception as e:
            st.error(f"Lakebase: {e}")

        if st.session_state.agent is not None:
            st.success("Agent + MCP tools: loaded")
        else:
            st.info("Agent + MCP tools: not loaded yet (loads on first message)")

# ============================================================
# Build the agent once per session (loading MCP tools is a network
# round-trip — don't redo it on every message)
# ============================================================

def get_or_build_agent():
    if st.session_state.agent is None:
        st.session_state.agent = asyncio.run(build_agent(user_id))
    return st.session_state.agent

# ============================================================
# Tabs: Chat (primary) and Insights (analytics dashboard)
# ============================================================

tab_chat, tab_insights = st.tabs(["💬 Chat", "📊 Insights"])

with tab_chat:
    def _extract_role_and_text(message) -> tuple[str, str]:
        """Handles both plain dicts (the first user turn, appended directly
        in this file) and LangChain message objects (everything returned by
        the agent — HumanMessage, AIMessage, ToolMessage — which don't
        support dict-style .get())."""
        if isinstance(message, dict):
            role = message.get("role", "")
            content = message.get("content", "")
        else:
            role = {"human": "user", "ai": "assistant"}.get(getattr(message, "type", ""), "")
            content = getattr(message, "content", "")

        if isinstance(content, str):
            return role, content
        if isinstance(content, list):
            text = "".join(
                b.get("text", "") for b in content
                if isinstance(b, dict) and b.get("type") == "text"
            )
            return role, text
        return role, ""

    for message in st.session_state.messages:
        role, text = _extract_role_and_text(message)
        # Only render user/assistant turns — skip tool calls/results so the
        # chat doesn't show internal tool chatter as separate bubbles.
        if role in ("user", "assistant") and text:
            with st.chat_message(role):
                st.markdown(text)

    if prompt := st.chat_input("Try: 'Watch flight UAL123 on 2026-10-01' or 'What if my flight is cancelled?'"):
        with st.chat_message("user"):
            st.markdown(prompt)

        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    agent = get_or_build_agent()
                    st.session_state.messages = asyncio.run(
                        run_agent_turn(agent, st.session_state.messages)
                    )
                    reply = get_latest_text_reply(st.session_state.messages)
                except Exception as e:
                    reply = f"Something went wrong: {e}"
            st.markdown(reply)

with tab_insights:
    render_dashboard()

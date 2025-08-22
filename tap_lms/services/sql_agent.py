# File: tap_lms/services/sql_agent.py

import json
import time
import logging
import re
from typing import Optional, Dict, Any, List, Tuple, Set

import frappe
from langchain.agents.agent_types import AgentType
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_openai import ChatOpenAI

from tap_lms.infra.config import get_config
from tap_lms.infra.db import get_sqldb, get_allowlisted_tables
from tap_lms.infra.sql_catalog import load_schema

logger = logging.getLogger(__name__)

# --------- Load declarative schema (allowlist + columns + joins + guardrails) ----------
_schema = load_schema()

_allowlisted: List[str] = _schema.get("allowlist", [])
_tables: Dict[str, Dict[str, Any]] = _schema.get("tables", {})
_joins: List[Dict[str, str]] = _schema.get("allowed_joins", [])
_guardrails: List[str] = _schema.get("guardrails", [])

# Columns map for quick validation
_table_cols: Dict[str, Set[str]] = {
    t: set((_tables.get(t) or {}).get("columns", [])) for t in _allowlisted if t in _tables
}

# ---------- Prompt helpers ----------
def _format_tables_section() -> str:
    # Show only allowlisted tables with a short description
    lines = []
    for t in _allowlisted:
        if t in _tables:
            desc = (_tables[t].get("description") or "").strip()
            cols = ", ".join(sorted(_table_cols.get(t, set())))
            lines.append(f"- {t}: {desc}\n  Columns: {cols}")
    return "\n".join(lines)

def _format_joins_section() -> str:
    if not _joins:
        return "- (no joins unless explicitly listed)"
    lines = []
    for j in _joins:
        why = f" ({j.get('why')})" if j.get('why') else ""
        lines.append(f"- {j['left_table']}.{j['left_key']} = {j['right_table']}.{j['right_key']}{why}")
    return "\n".join(lines)

SYSTEM_RULES = f"""
You are a SQL agent for TAP LMS on MariaDB.

HARD CONSTRAINTS:
- Use ONLY these tables: {", ".join(sorted(_allowlisted))}
- Use ONLY the listed columns for each table (do not invent columns).
- Primary key is always `name`.
- Prefer `name1` for display but JOINs must use `name` as the key.
- Allowed JOINS (use exactly; do NOT invent other joins):
{_format_joins_section()}
- Never reference tables or columns outside this schema.
- If the question needs multiple tables, use only the allowed joins above.
- Always add LIMIT for non-aggregate queries (default LIMIT 50).

Tables & Columns:
{_format_tables_section()}
""".strip()

def _get_llm() -> Optional[ChatOpenAI]:
    api_key = get_config("openai_api_key")
    model = get_config("primary_llm_model") or "gpt-4o-mini"
    if not api_key:
        logger.error("OpenAI API key missing in site_config or env.")
        return None
    return ChatOpenAI(
        model_name=model,
        openai_api_key=api_key,
        temperature=0.1,
        max_tokens=1000,
    )

# ---------- SQL safety helpers ----------
_SQL_TABLE_RE = re.compile(r"\bfrom\s+([`\"\[]?\w+[`\"\]]?)|\bjoin\s+([`\"\[]?\w+[`\"\]]?)", re.IGNORECASE)
_SQL_COL_RE = re.compile(r"\b(\w+)\.(\w+)\b")

from typing import Any, Iterable, Optional

def _extract_candidate_sql(intermediate_steps: Iterable[Any]) -> Optional[str]:
    """
    Robustly scan LangChain intermediate steps to find the most recent SQL query.
    Handles common formats:
      - list[tuple[AgentAction, str]]
      - list[dict] with keys like {'tool': 'sql_db_query', 'tool_input': '...'}
    Returns the last SQL string found, or None.
    """
    last_sql: Optional[str] = None

    def _to_str(x: Any) -> Optional[str]:
        if x is None:
            return None
        if isinstance(x, str):
            return x
        if isinstance(x, dict):
            # sometimes tool_input is {"query": "..."}
            if "query" in x and isinstance(x["query"], str):
                return x["query"]
        # best-effort stringification
        return str(x)

    for step in intermediate_steps or []:
        tool_name = None
        tool_input = None

        # Format A: (AgentAction, observation)
        if isinstance(step, tuple) and step:
            action = step[0]
            if hasattr(action, "tool"):
                tool_name = getattr(action, "tool", None)
            if hasattr(action, "tool_input"):
                tool_input = getattr(action, "tool_input", None)

        # Format B: dict-based
        elif isinstance(step, dict):
            tool_name = step.get("tool") or step.get("action")
            tool_input = step.get("tool_input") or step.get("input") or step.get("action_input")

        # Only grab SQL tools
        if tool_name in (
            "sql_db_query",
            "sql_db_query_checker",
            "query_sql_db",
            "sql_db_schema",
            "sql_db_list_tables",
        ):
            s = _to_str(tool_input)
            # heuristics: keep proper SQL statements only
            if s and any(tok in s.lower() for tok in ("select", "insert", "update", "delete", "with ")):
                last_sql = s

    return last_sql


def _violates_allowlist(sql: str) -> Tuple[bool, str]:
    """
    Check referenced tables / columns are within allowlist/columns.
    Returns (violates, reason).
    """
    if not sql:
        return (False, "")
    # Tables
    found_tables = set()
    for m in _SQL_TABLE_RE.finditer(sql):
        tbl = m.group(1) or m.group(2)
        if not tbl:
            continue
        tbl = tbl.strip("`\"[]")
        found_tables.add(tbl)

    # If any table is not allowlisted -> violation
    for t in found_tables:
        if t not in _allowlisted:
            return (True, f"disallowed table: {t}")

    # Columns: look for table.column
    for m in _SQL_COL_RE.finditer(sql):
        t, c = m.group(1), m.group(2)
        if t in _allowlisted:
            if _table_cols.get(t) and c not in _table_cols[t]:
                # allow commonly generated aliases (e.g., COUNT, alias names), skip if not a real column ref
                if c.lower() not in {"count", "sum", "avg", "min", "max"}:
                    return (True, f"disallowed column {t}.{c}")
    return (False, "")

def _wrap_user_question(q: str, default_limit: int = 50) -> str:
    note = f"[NOTE] Add LIMIT {default_limit} for non-aggregate queries."
    return f"{SYSTEM_RULES}\n\nUSER QUESTION:\n{q.strip()}\n\n{note}"

# ---------- Agent builder ----------
def build_sql_agent(sample_rows_in_table_info: int = 2):
    """
    Build a SQL agent bound to MariaDB using the SAFE allow-list from tap_lms.infra.sql_catalog.
    """
    # Use our declarative allow-list (preferred over scanning DB)
    include_tables = list(_allowlisted)

    # Bind DB with include_tables; if list is empty, fallback to None (exposes all)
    db = get_sqldb(
        include_tables=include_tables if include_tables else None,
        sample_rows_in_table_info=sample_rows_in_table_info,
    )

    llm = _get_llm()
    if not llm:
        raise RuntimeError("LLM not available; set openai_api_key in site_config.json")

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    # Create a robust zero-shot SQL agent (keep intermediate steps so we can sniff its SQL)
    agent = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        max_iterations=6,
        max_execution_time=45,
        agent_executor_kwargs={
            "return_intermediate_steps": True,
            "handle_parsing_errors": True,
        },
        system_message=SYSTEM_RULES,
    )
    return agent, db, include_tables

# ---------- Public API ----------
def answer_sql(q: str) -> Dict[str, Any]:
    t0 = time.time()
    agent, db, allowlisted = build_sql_agent()

    try:
        tables = sorted(db.get_usable_table_names())
        logger.info("SQL agent can see %d tables; sample: %s",
                    len(tables), ", ".join(tables[:15]))
    except Exception:
        tables = []

    result = agent.invoke({"input": q})

    # Extract the raw answer
    answer = result.get("output", str(result)).strip()

    # Pull intermediate SQL if available
    intermediate = result.get("intermediate_steps", [])
    candidate_sql = _extract_candidate_sql(intermediate)

    return {
        "question": q,
        "answer": answer,
        "success": True,
        "engine": "sql",
        "execution_time": time.time() - t0,
        "metadata": {
            "visible_tables": len(allowlisted),
            "candidate_sql": candidate_sql,
        },
    }


# ---------- Bench CLI ----------
def cli(q: str):
    """
    Bench command:
      bench execute tap_lms.services.sql_agent.cli --kwargs "{'q':'how many students in grade 9'}"
    """
    out = answer_sql(q)
    print(json.dumps(out, indent=2))
    return out

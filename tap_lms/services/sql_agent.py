# File: tap_lms/services/sql_agent.py

import json
import time
import logging
from typing import Optional, Dict, Any

import frappe
from langchain.agents.agent_types import AgentType
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_openai import ChatOpenAI

from tap_lms.infra.config import get_config
from tap_lms.infra.db import get_sqldb, get_allowlisted_tables

logger = logging.getLogger(__name__)

SYSTEM_RULES = """You are a SQL assistant for TAP LMS (MariaDB).
- The PRIMARY KEY of most Frappe DocTypes is `name`.
- Display labels often live in `name1`.
- Use only tables visible to you (agent schema).
- Always add LIMIT for listing queries.
- Never invent columns; inspect schema first when unsure.
"""

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

def build_sql_agent(sample_rows_in_table_info: int = 2):
    """
    Build a SQL agent bound to MariaDB using the SAFE allow-list from tap_lms.infra.db.
    """
    # Use our safe allow-list (excludes Singles, virtuals, and non-existent tables)
    allowlisted = get_allowlisted_tables(module_name="TAP LMS")

    # Bind DB with include_tables; if list is empty, fallback to None (exposes all)
    db = get_sqldb(
        include_tables=allowlisted if allowlisted else None,
        sample_rows_in_table_info=sample_rows_in_table_info,
    )

    llm = _get_llm()
    if not llm:
        raise RuntimeError("LLM not available; set openai_api_key in site_config.json")

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    # Create a robust zero-shot SQL agent (keeps intermediate steps)
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
    return agent, db, allowlisted

def answer_sql(q: str) -> Dict[str, Any]:
    """
    Single-call helper to run a SQL Q&A with safe allow-list applied.
    """
    t0 = time.time()
    agent, db, allowlisted = build_sql_agent()

    # Optional: log a short preview of the visible tables
    try:
        tables = sorted(db.get_usable_table_names())
        logger.info("SQL agent can see %d tables; sample: %s",
                    len(tables), ", ".join(tables[:15]))
    except Exception:
        pass

    result = agent.invoke({"input": q})
    # LangChain returns either dict with 'output' or a string
    answer = result.get("output", str(result))
    return {
        "question": q,
        "answer": answer.strip(),
        "success": True,
        "engine": "sql",
        "execution_time": time.time() - t0,
        "metadata": {
            "visible_tables": len(allowlisted),
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

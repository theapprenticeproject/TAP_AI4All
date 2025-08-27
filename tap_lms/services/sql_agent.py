import json as _json
import time as _time
import logging as _logging
import re as _re
from typing import Optional as _Optional, Dict as _Dict, Any as _Any, List as _List, Tuple as _Tuple, Set as _Set

import frappe
from langchain.agents.agent_types import AgentType
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_openai import ChatOpenAI as _ChatOpenAI

from tap_lms.infra.config import get_config as _get_config
from tap_lms.infra.db import get_sqldb as _get_sqldb
from tap_lms.infra.sql_catalog import load_schema as _load_schema
from tap_lms.services.doctype_selector import pick_doctypes as _pick_doctypes

logger = _logging.getLogger(__name__)

# --------- Load declarative schema ----------
_schema = _load_schema() or {}
_allowlisted: _List[str] = _schema.get("allowlist", [])
_tables: _Dict[str, _Dict[str, _Any]] = _schema.get("tables", {})
_joins: _List[_Dict[str, str]] = _schema.get("allowed_joins", [])

# Build column map and field options
_table_cols: _Dict[str, _Set[str]] = {}
_table_field_options: _Dict[str, _Dict[str, _List[str]]] = {}

for t in _allowlisted:
    if t in _tables:
        table_info = _tables[t] or {}
        columns = set(table_info.get("columns", []))
        _table_cols[t] = columns

        # Extract field options (if any)
        fields = table_info.get("fields", [])
        options_map = {}
        for f in fields:
            if isinstance(f, dict) and "fieldname" in f:
                opts = f.get("options")
                if opts and isinstance(opts, str) and "\n" in opts:
                    options_map[f["fieldname"]] = [
                        o.strip() for o in opts.split("\n") if o.strip()
                    ]
        if options_map:
            _table_field_options[t] = options_map


def _format_tables_section(only: _Optional[_Set[str]] = None) -> str:
    lines = []
    display = [t for t in _allowlisted if (only is None or t in only)]
    for t in display:
        if t in _tables:
            desc = (_tables[t].get("description") or "").strip()
            cols = ", ".join(sorted(_table_cols.get(t, set())))
            options_info = ""
            if t in _table_field_options:
                options_lines = []
                for col, opts in _table_field_options[t].items():
                    options_lines.append(f"    - {col}: {', '.join(opts)}")
                options_info = "\n  Options:\n" + "\n".join(options_lines)
            lines.append(f"- {t}: {desc}\n  Columns: {cols}{options_info}")
    return "\n".join(lines) or "- (none)"


def _format_joins_section(only_tables: _Optional[_Set[str]] = None) -> str:
    if not _joins:
        return "- (no joins unless explicitly listed)"
    lines = []
    for j in _joins:
        lt, rt = j["left_table"], j["right_table"]
        if only_tables and not ({lt, rt} & only_tables):
            continue
        why = f" ({j.get('why')})" if j.get('why') else ""
        lines.append(f"- {lt}.{j['left_key']} = {rt}.{j['right_key']}{why}")
    return "\n".join(lines) or "- (no joins among selected tables)"


def _system_rules_for(only_tables: _Optional[_Set[str]] = None) -> str:
    return f"""
You are a SQL agent for TAP LMS on MariaDB.

HARD CONSTRAINTS:
- Use ONLY these tables: {', '.join(sorted(only_tables or set(_allowlisted)))}
- Use ONLY the listed columns for each table (do not invent columns).
- For fields with OPTIONS, only use the provided allowed values (case-insensitive).
- Prefer `name1` for display but JOINs must use `name` as the key.
- Allowed JOINS (use exactly; do NOT invent other joins):
{_format_joins_section(only_tables)}
- Never reference tables or columns outside this schema.
- Always add LIMIT for non-aggregate queries (default LIMIT 50).

Tables & Columns:
{_format_tables_section(only_tables)}
""".strip()


def _get_llm() -> _Optional[_ChatOpenAI]:
    api_key = _get_config("openai_api_key")
    model = "gpt-3.5-turbo" or _get_config("primary_llm_model") or "gpt-4o-mini"
    if not api_key:
        logger.error("OpenAI API key missing in site_config or env.")
        return None
    return _ChatOpenAI(model_name=model, openai_api_key=api_key, temperature=0.0, max_tokens=1000)


# ---------- SQL helpers ----------
_SQL_TABLE_RE = _re.compile(r"\b(?:FROM|JOIN)\s+([`\"]?[\w\.]+[`\"]?)", flags=_re.IGNORECASE)
_SQL_SELECT_LIST_RE = _re.compile(r'(\bSELECT\s+)(.*?)(\s+FROM\s)', flags=_re.IGNORECASE | _re.DOTALL)


def _extract_candidate_sql(intermediate_steps) -> str:
    try:
        for step in intermediate_steps:
            action = step[0] if isinstance(step, (list, tuple)) and step else step
            tool = getattr(action, "tool", None)
            tool_input = getattr(action, "tool_input", None)
            if tool and "sql" in str(tool).lower():
                if isinstance(tool_input, str) and "select" in tool_input.lower():
                    return tool_input.strip() if tool_input.strip().endswith(";") else tool_input.strip() + ";"
                if isinstance(tool_input, dict):
                    for k in ("query", "sql"):
                        v = tool_input.get(k)
                        if isinstance(v, str) and "select" in v.lower():
                            return v.strip() if v.strip().endswith(";") else v.strip() + ";"
    except Exception:
        pass
    try:
        blob = "\n".join([str(s) for s in intermediate_steps])
        m = _re.search(r"(SELECT\b[\s\S]+?;)", blob, flags=_re.IGNORECASE)
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return ""


def _tables_in_sql(sql: str) -> _List[str]:
    if not sql:
        return []
    candidates = _SQL_TABLE_RE.findall(sql)
    seen, out = set(), []
    for t in candidates:
        cleaned = t.strip().strip("`").strip('"')
        if cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def _rewrite_select_to_display_pref(sql: str) -> str:
    if not sql:
        return sql

    m = _SQL_SELECT_LIST_RE.search(sql)
    if not m:
        return sql

    prefix, select_list, suffix = m.group(1), m.group(2), m.group(3)
    rest = sql[m.end():]

    table_candidates = _tables_in_sql(sql)
    preferred_columns = {}

    for t in table_candidates:
        table_info = _tables.get(t) or {}
        cols = set(table_info.get("columns", []))
        if "display_name" in cols:
            preferred_columns[t] = "display_name"
        elif "name1" in cols:
            preferred_columns[t] = "name1"

    select_items = [item.strip() for item in select_list.split(",")]

    rewritten_items = []
    for item in select_items:
        rewritten_item = item
        for table, preferred_col in preferred_columns.items():
            pattern = rf"^\s*({table}\.)?name\s*$"
            if _re.match(pattern, item, flags=_re.IGNORECASE):
                rewritten_item = f"{table}.{preferred_col}" if f"{table}." in item else preferred_col
        rewritten_items.append(rewritten_item)

    new_select = ", ".join(rewritten_items)
    rewritten = prefix + new_select + suffix + rest
    if not rewritten.strip().endswith(";"):
        rewritten += ";"
    return rewritten


def _validate_sql_options(sql: str) -> _Optional[str]:
    """Return error message if SQL violates field options."""
    for table, fields_opts in _table_field_options.items():
        for field, allowed in fields_opts.items():
            m = _re.findall(rf"{field}\s*=\s*'([^']+)'", sql, flags=_re.IGNORECASE)
            for found in m:
                if found not in allowed:
                    return f"Invalid value '{found}' for field '{field}' in table '{table}'. Allowed: {allowed}"
    return None


def _safe_execute(sql: str):
    err = _validate_sql_options(sql)
    if err:
        return None, err
    try:
        return frappe.db.sql(sql, as_dict=True), None
    except Exception as e:
        return None, str(e)


# ---------- Agent builder ----------

def _tables_for_doctypes(doctypes: _List[str]) -> _Set[str]:
    want = set()
    dt_set = set(doctypes or [])
    for t in _allowlisted:
        dt = t[3:] if t.startswith("tab") else t
        if dt in dt_set:
            want.add(t)
    return want or set(_allowlisted)


def build_sql_agent(sample_rows_in_table_info: int = 2, q: str | None = None):
    routed: _List[str] = _pick_doctypes(q, top_n=6) if q else []
    include_tables = list(_tables_for_doctypes(routed))

    db = _get_sqldb(
        include_tables=include_tables if include_tables else None,
        sample_rows_in_table_info=sample_rows_in_table_info,
    )

    llm = _get_llm()
    if not llm:
        raise RuntimeError("LLM not available; set openai_api_key in site_config.json")

    sys_rules = _system_rules_for(set(include_tables))

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
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
        system_message=sys_rules,
    )
    return agent, db, include_tables


# ---------- Public API ----------

def answer_sql(q: str, doctypes: list[str] | None = None) -> dict:
    t0 = _time.time()
    agent, db, allowlisted = build_sql_agent(q=q)

    result = agent.invoke({"input": q})
    answer = result.get("output", "").strip()
    intermediate = result.get("intermediate_steps", [])
    candidate_sql = _extract_candidate_sql(intermediate)

    rewritten_sql = _rewrite_select_to_display_pref(candidate_sql) if candidate_sql else None

    rows, err = (None, None)
    if rewritten_sql:
        rows, err = _safe_execute(rewritten_sql)

    if rows is not None and not err:
        answer = _json.dumps(rows, ensure_ascii=False, default=str)

    return {
        "question": q,
        "answer": answer,
        "success": rows is not None and not err,
        "engine": "sql",
        "execution_time": _time.time() - t0,
        "metadata": {
            "candidate_sql": candidate_sql,
            
            "rewritten_sql": rewritten_sql,
            "doctypes_routed": doctypes if doctypes else _pick_doctypes(q, top_n=6),
            "rows_returned": len(rows) if rows else 0,
            "execution_error": err,

        },
    }


def explain_sql(q: str, doctypes: list[str] | None = None) -> _Dict[str, _Any]:
    t0 = _time.time()
    agent, db, allowlisted = build_sql_agent(q=q)
    result = agent.invoke({"input": q})
    intermediate = result.get("intermediate_steps", []) if isinstance(result, dict) else []
    sql = _extract_candidate_sql(intermediate)
    tables = _tables_in_sql(sql)
    allowlist_set = set(allowlisted or [])
    used_not_allowed = [t for t in tables if t not in allowlist_set]

    return {
        "question": q,
        "engine": "sql",
        "success": bool(sql),
        "execution_time": _time.time() - t0,
        "candidate_sql": sql,
        "tables_detected": tables,
        "allowlist_ok": len(used_not_allowed) == 0,
        "not_in_allowlist": used_not_allowed,
        "doctypes_routed": doctypes if doctypes else _pick_doctypes(q, top_n=6),
    }


# ---------- Bench CLI ----------

def cli(q: str):
    '''  bench execute tap_lms.services.sql_agent.cli --kwargs "{'q':'total number of student distribution by schools'}"
    '''
    out = answer_sql(q)
    print(_json.dumps(out, indent=2, ensure_ascii=False))
    return out

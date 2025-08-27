# tap_lms/services/graph_rag.py
"""
Graph-only RAG for TAP LMS (Neo4j)
- Uses tap_lms_schema.json (via load_schema) as single source of truth
- Routes queries to candidate DocTypes using pick_doctypes()
- Builds strict prompt with allowed doctypes/properties/joins + VALUE HINTS sampled from Neo4j
- Generates Cypher with LLM (ChatOpenAI), validates & sanitizes it, executes on Neo4j
- Prints debug info (selected doctypes, props, relationships, sample values) to terminal
"""

import json
import re
import time
import logging
from typing import Dict, Any, List, Tuple, Set, Optional

from neo4j import GraphDatabase
from langchain_openai import ChatOpenAI

from tap_lms.infra.sql_catalog import load_schema
from tap_lms.infra.config import get_config, get_neo4j_config
from tap_lms.services.doctype_selector import pick_doctypes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ----------------------------
# Helpers: Schema extraction
# ----------------------------
def _load_decl_schema() -> Dict[str, Any]:
    try:
        return load_schema() or {}
    except Exception as e:
        logger.warning("Failed to load declarative schema: %s", e)
        return {}

def _canonical_doctype_from_tab(tabname: str) -> str:
    return tabname[3:] if tabname.startswith("tab") else tabname

def _allowed_props_by_doctype(schema: Dict[str, Any]) -> Dict[str, Set[str]]:
    tables = schema.get("tables", {}) or {}
    out: Dict[str, Set[str]] = {}
    for tab, info in tables.items():
        dt = info.get("doctype") or _canonical_doctype_from_tab(tab)
        cols = set(info.get("columns", []) or [])
        cols.update({"name", "display_name", "name1", "_doctype"})  # migrator standard props
        out[dt] = cols
    return out

def _relname_from_tables(lt: str, rt: str, lk: str, rk: str) -> str:
    import re as _re
    def safe(s: str) -> str:
        return _re.sub(r"\W+", "_", s).strip("_")
    left = safe(lt[3:] if lt.startswith("tab") else lt)
    right = safe(rt[3:] if rt.startswith("tab") else rt)
    base = f"{left}_{lk}_TO_{right}_{rk}"
    return _re.sub(r"\W+", "_", base).strip("_").upper()

def _allowed_joins_for_doctypes(schema: Dict[str, Any], doctypes: List[str]) -> List[Dict[str, Any]]:
    out = []
    for j in (schema.get("allowed_joins") or []):
        lt, lk, rt, rk = j.get("left_table"), j.get("left_key"), j.get("right_table"), j.get("right_key")
        if not lt or not rt:
            continue
        ldt = _canonical_doctype_from_tab(lt)
        rdt = _canonical_doctype_from_tab(rt)
        if ldt in doctypes or rdt in doctypes:
            out.append({
                "left_table": lt, "left_doctype": ldt, "left_key": lk,
                "right_table": rt, "right_doctype": rdt, "right_key": rk,
                "why": j.get("why", ""), "relname": _relname_from_tables(lt, rt, lk, rk),
            })
    return out

# ----------------------------
# LLM & Neo4j helpers
# ----------------------------
def _get_llm():
    api_key = get_config("openai_api_key")
    model = get_config("primary_llm_model") or "gpt-4o"
    if not api_key:
        raise RuntimeError("openai_api_key missing; set site_config.json or env")
    return ChatOpenAI(model_name=model, openai_api_key=api_key, temperature=0.0, max_tokens=800)

def _get_neo4j_driver():
    try:
        neo = get_neo4j_config()
    except Exception:
        neo = None
    if not neo:
        raise ValueError("Neo4j config missing; set uri/user/password")
    uri = neo.get("uri") 
    user = neo.get("user")
    pwd = neo.get("password") 
    if not uri or user is None or pwd is None:
        raise ValueError("Neo4j uri/user/password incomplete")
    return GraphDatabase.driver(uri, auth=(user, pwd))

# ----------------------------
# Cypher validation & sanitizer
# ----------------------------
_PROP_USE_RE = re.compile(r"\b([a-zA-Z][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b")

def _extract_prop_uses(cypher: str) -> List[Tuple[str, str]]:
    return _PROP_USE_RE.findall(cypher or "")

def _strip_fences(text: str) -> str:
    if not text:
        return text
    return re.sub(r"(?s)```(?:\w+)?(.*?)```", r"\1", text).strip()

def _remove_group_by(cypher: str) -> str:
    return re.sub(r"(?im)^\s*GROUP\s+BY\b[^\n]*\n?", "", cypher)

def _strip_invalid_props(cypher: str, allowed_props_flat: Set[str]) -> Tuple[str, List[str]]:
    if not cypher:
        return cypher, []
    removed = set()
    cleaned = cypher
    for var, prop in _extract_prop_uses(cypher):
        if prop not in allowed_props_flat:
            removed.add(prop)
    if not removed:
        return cleaned, []
    for bad in sorted(removed, key=len, reverse=True):
        cleaned = re.sub(fr"\b\w+\.{re.escape(bad)}\s*(?:=|!=|<>|<=|>=|<|>)\s*[^)\n,]+", "TRUE", cleaned)
        cleaned = re.sub(fr"(?i)\bAND\s+\w+\.{re.escape(bad)}\s*[^)\n,]+", "", cleaned)
        cleaned = re.sub(fr"\b(\w+)\.{re.escape(bad)}\b", r"\1.NULL", cleaned)
    return cleaned, sorted(list(removed))

# fields that are stored as strings in Frappe but LLM may treat as numbers
_STRINGY_FIELDS = {
    "grade"
    # add more if you see similar issues (e.g., 'year' if stored as text)
}

_NUM_LIKE = re.compile(r"""
    (?P<lhs>\b[a-zA-Z_][\w\.]*\.(?P<field>[a-zA-Z_][\w]*)\s*=\s*)
    (?P<rhs>\d+)\b
""", re.VERBOSE)

_IN_LIST = re.compile(r"""
    (?P<lhs>\b[a-zA-Z_][\w\.]*\.(?P<field>[a-zA-Z_][\w]*)\s+IN\s*\()\s*
    (?P<items>(?:\s*\d+\s*,\s*)*\s*\d+\s*)
    (?P<rpar>\))
""", re.VERBOSE)

def _postprocess_cypher_literals(cypher: str) -> str:
    """
    Fix common literal issues where string-typed properties (e.g., grade) are
    emitted as unquoted numbers by the LLM.

    - n.grade = 9        -> n.grade = '9'
    - n.grade IN (9,10)  -> n.grade IN ['9','10']   (Cypher supports lists with [] when mixing types)
    """
    # a) equality: field = <number>
    def _fix_eq(m: re.Match) -> str:
        field = m.group("field")
        if field in _STRINGY_FIELDS:
            return f"{m.group('lhs')}'{m.group('rhs')}'"
        return m.group(0)

    cypher = _NUM_LIKE.sub(_fix_eq, cypher)

    # b) IN lists: field IN (1, 2, 3)  -> field IN ['1','2','3'] for stringy fields
    def _fix_in(m: re.Match) -> str:
        field = m.group("field")
        if field not in _STRINGY_FIELDS:
            return m.group(0)
        items = [x.strip() for x in m.group("items").split(",")]
        items = [x for x in items if x]  # drop empties
        # quote numerics as strings
        items = [f"'{x}'" if x.isdigit() else x for x in items]
        return f"{m.group('lhs')}[{', '.join(items)}]{m.group('rpar')}"

    cypher = _IN_LIST.sub(_fix_in, cypher)

    return cypher


# ----------------------------
# Value sampling (NEW)
# ----------------------------
#  try to sample a few “high-signal” fields per DocType to guide the LLM.
_VALUE_FIELDS_HINTS: Dict[str, List[str]] = {
    "Student": ["grade", "status", "language", "rigour", "preferred_day"],
    "Activities": ["rigor", "content_skill", "sel_skill", "sdg"],
    "Assignment": ["assignment_type", "subject"],
    "Course Verticals": ["name1", "name2"],
}

def _sample_distinct_values(session, doctype: str, field: str, k: int = 8) -> List[str]:
    # Distinct values for a property; only non-null/non-empty
    cy = (
        "MATCH (n) WHERE n._doctype = $dt AND n." + field + " IS NOT NULL "
        "WITH DISTINCT n." + field + " AS v "
        "WHERE (v IS NOT NULL AND toString(v) <> '') "
        "RETURN v LIMIT $k"
    )
    try:
        res = session.run(cy, dt=doctype, k=k)
        vals = []
        for r in res:
            v = r.get("v")
            if v is None:
                continue
            vals.append(str(v))
        return vals
    except Exception:
        return []

def _collect_value_hints(driver, doctypes: List[str], allowed_props_map: Dict[str, Set[str]]) -> Dict[str, Dict[str, List[str]]]:
    hints: Dict[str, Dict[str, List[str]]] = {}
    with driver.session() as s:
        for dt in doctypes:
            fields = _VALUE_FIELDS_HINTS.get(dt, [])
            # keep only fields that actually exist in allowed props
            fields = [f for f in fields if f in allowed_props_map.get(dt, set())]
            if not fields:
                continue
            hints[dt] = {}
            for f in fields:
                vals = _sample_distinct_values(s, dt, f, k=8)
                if vals:
                    hints[dt][f] = vals
    return hints

# ----------------------------
# Prompt builder
# ----------------------------
_CYPHER_PROMPT_TEMPLATE = """You are a Neo4j Cypher generator. Produce a SINGLE valid Cypher query (no explanation).

STRICT RULES:
- Use ONLY the DocTypes and properties listed below (do not invent).
- Prefer filtering labels via: MATCH (n) WHERE n._doctype = '<DocType>'.
- If relationships are needed, use ONLY the permitted types listed.
- NEVER use SQL constructs like GROUP BY (use aggregates in RETURN).
- Include LIMIT (default 100).
- When comparing categorical fields (e.g., grade/status), prefer values from the Value Hints.

DocTypes & allowed properties:
{doctype_props}

Allowed relationships (left_doctype -[REL]-> right_doctype; keys):
{rel_hints}

Value hints (observed examples in DB):
{value_hints}

Question:
{question}

Cypher:
"""

def _build_prompt_for_doctypes(doctypes: List[str], schema: Dict[str, Any], question: str,
                               value_hints: Dict[str, Dict[str, List[str]]] | None) -> str:
    allowed_map = _allowed_props_by_doctype(schema)
    # DocType -> props
    lines = []
    for dt in doctypes:
        props = sorted(list(allowed_map.get(dt, set())))
        lines.append(f"- {dt}: {', '.join(props) if props else '(no props)'}")
    doctype_props = "\n".join(lines) if lines else "(none)"

    # Relationships
    joins = _allowed_joins_for_doctypes(schema, doctypes)
    rlines = []
    for j in joins:
        rlines.append(f"- {j['left_doctype']} -[:{j['relname']}]-> {j['right_doctype']}  (keys: {j['left_key']}={j['right_key']})")
    rel_hints = "\n".join(rlines) if rlines else "(no relationships)"

    # Value hints
    vh_lines = []
    if value_hints:
        for dt, fmap in value_hints.items():
            if not fmap:
                continue
            for f, vals in fmap.items():
                # cap list to avoid long prompts
                show = ", ".join(vals[:10])
                vh_lines.append(f"- {dt}.{f}: {show}")
    value_hints_block = "\n".join(vh_lines) if vh_lines else "(no hints found)"

    return _CYPHER_PROMPT_TEMPLATE.format(
        doctype_props=doctype_props,
        rel_hints=rel_hints,
        value_hints=value_hints_block,
        question=question
    )

# ----------------------------
# Main flow
# ----------------------------
def answer_graph(q: str, doctypes: list[str] | None = None) -> dict:
    t0 = time.time()
    schema = _load_decl_schema()

    # 1) route doctypes
    if not doctypes:
        try:
            doctypes = pick_doctypes(q, top_n=6) or []
        except Exception as e:
            logger.warning("pick_doctypes failed: %s", e)
            doctypes = []

    # fallback doctypes if routing empty
    if not doctypes:
        allowlist_tabs = schema.get("allowlist", []) or []
        doctypes = [_canonical_doctype_from_tab(t) for t in allowlist_tabs[:8]]

    allowed_props_map = _allowed_props_by_doctype(schema)
    allowed_props_flat = set().union(*[allowed_props_map.get(d, set()) for d in doctypes]) if doctypes else set()
    joins = _allowed_joins_for_doctypes(schema, doctypes)

    # 2) open driver once (also used for sampling)
    driver = _get_neo4j_driver()

    # 3) collect VALUE HINTS from graph
    value_hints = _collect_value_hints(driver, doctypes, allowed_props_map)

    # 4) debug: print doctypes/props/rels + sample values and 2-row peeks
    print("\n=== Doctype Selector Debug ===")
    print("Question:", q)
    print("Selected DocTypes (ordered):", doctypes)
    print("\nAllowed properties (per DocType):")
    for d in doctypes:
        print(f"- {d}: {sorted(list(allowed_props_map.get(d, set())))[:100]}")
    print("\nAllowed relationships touching these DocTypes:")
    for j in joins:
        print(f"- {j['left_doctype']} -[:{j['relname']}]-> {j['right_doctype']}  (keys: {j['left_key']}={j['right_key']})   why: {j.get('why','')}")
    if value_hints:
        print("\nObserved value hints:")
        for dt, fmap in value_hints.items():
            for f, vals in fmap.items():
                print(f"- {dt}.{f}: {vals[:10]}")
    # quick 2-row peek for a few fields to emulate SQL-preview feel
    with driver.session() as s:
        for d in doctypes[:4]:
            sample_fields = list((value_hints.get(d, {}) or {}).keys())[:3] or ["name1", "display_name"]
            fields_cy = ", ".join([f"n.{f} AS {f}" for f in sample_fields])
            cy = f"MATCH (n) WHERE n._doctype = $dt RETURN {fields_cy} LIMIT 2"
            try:
                rows = [r.data() for r in s.run(cy, dt=d)]
                if rows:
                    print(f"\nSample rows from {d}:")
                    for r in rows:
                        print("  ", r)
            except Exception:
                pass
    print("================================\n")

    # 5) build prompt & call LLM
    prompt = _build_prompt_for_doctypes(doctypes, schema, q, value_hints)
    llm = _get_llm()
    try:
        resp = llm.invoke(prompt)
        cypher_candidate = resp.content if hasattr(resp, "content") else (resp if isinstance(resp, str) else str(resp))
        cypher_candidate = _strip_fences(cypher_candidate)
    except Exception as e:
        logger.exception("LLM call failed: %s", e)
        try:
            driver.close()
        except Exception:
            pass
        return {
            "question": q,
            "answer": "LLM generation failed.",
            "success": False,
            "engine": "graph",
            "execution_time": time.time() - t0,
            "metadata": {"error": str(e)},
        }

    # 6) sanitize cypher (GROUP BY removal + invalid prop neutralization)
    cypher_no_group = _remove_group_by(cypher_candidate)
    cleaned_cypher, removed_props = _strip_invalid_props(cypher_no_group, allowed_props_flat)
    if not re.search(r"(?i)\bLIMIT\b", cleaned_cypher or ""):
        cleaned_cypher = (cleaned_cypher or cypher_no_group or cypher_candidate).rstrip("; \n") + "\nLIMIT 100"
    
    if removed_props:
        logger.warning("Removed invalid properties from Cypher: %s", removed_props)
        print("⚠️ Removed invalid properties from generated Cypher:", removed_props)
    
    final_cypher = (cleaned_cypher or cypher_no_group or cypher_candidate).strip()
    
    # 🔧 NEW: fix number-vs-string issues for fields like 'grade'
    final_cypher = _postprocess_cypher_literals(final_cypher)


    # 7) execute
    rows = []
    exec_error = None
    try:
        with driver.session() as session:
            result = session.run(final_cypher)
            rows = [r.data() for r in result]
    except Exception as e:
        exec_error = str(e)
        logger.exception("Neo4j execution failed: %s", e)
    finally:
        try:
            driver.close()
        except Exception:
            pass

    # 8) prepare answer
    if rows:
        answer = json.dumps(rows[:20], ensure_ascii=False)
    else:
        if removed_props:
            answer = f"No direct results after removing invalid properties: {removed_props}."
        elif exec_error:
            answer = f"Query execution failed: {exec_error}"
        else:
            answer = "No matching records found. Please check if your query values exist in the database."

    return {
        "question": q,
        "answer": answer,
        "success": bool(rows) and exec_error is None,
        "engine": "graph",
        "execution_time": time.time() - t0,
        "metadata": {
            "doctypes_routed": doctypes,
            "generated_cypher_raw": cypher_candidate,
            "sanitized_cypher": final_cypher,
            "invalid_props_removed": removed_props,
            "rows_returned": len(rows),
            "execution_error": exec_error,
        },
    }

# ----------------------------
# CLI wrapper
# ----------------------------
def cli(q: str):
    """
    bench execute tap_lms.services.graph_rag.cli --kwargs "{'q':'recommend activities for grade 9 student'}"
    """
    out = answer_graph(q)
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return out

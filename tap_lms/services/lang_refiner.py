# File: tap_lms/services/lang_refiner.py

import re
import json
import time
import logging
from typing import Dict, Any, List, Optional

import frappe

from tap_lms.infra.config import get_config
from tap_lms.services.doctype_selector import pick_doctypes

# already have a schema loader (used by SQL agent), reuse it:
try:
    from tap_lms.infra.sql_catalog import load_schema  # returns dict with tables/fields/joins/guardrails
except Exception:
    load_schema = None  # optional; handle None gracefully

try:
    from langchain_openai import ChatOpenAI
except Exception:
    ChatOpenAI = None

logger = logging.getLogger(__name__)


# -----------------------------
# Helpers
# -----------------------------
DEVANAGARI_RANGE = re.compile(r"[\u0900-\u097F]")  # quick Hindi/Hinglish hint

def _llm() -> Optional["ChatOpenAI"]:
    """Get an LLM configured via site_config or env."""
    if ChatOpenAI is None:
        logger.warning("langchain_openai not available; returning None")
        return None
    api_key = get_config("openai_api_key")
    model = get_config("primary_llm_model") or "gpt-4o-mini"
    if not api_key:
        logger.warning("OpenAI API key missing; refiner will fall back to pass-through.")
        return None
    return ChatOpenAI(model_name=model, openai_api_key=api_key, temperature=0.0, max_tokens=800)

def _now_ms() -> int:
    return int(time.time() * 1000)

def _looks_hindi(text: str) -> bool:
    return bool(DEVANAGARI_RANGE.search(text))


# -----------------------------
# Core functions
# -----------------------------
def detect_language(q: str) -> str:
    """
    Super lightweight language detector:
    - If Devanagari chars => 'hi'
    - Else default 'en' (we keep it simple to avoid extra deps).
    """
    if not q:
        return "en"
    return "hi" if _looks_hindi(q) else "en"

def translate_to_english(text: str) -> str:
    """
    Translate to English using the configured LLM.
    If LLM unavailable, return text as-is.
    """
    llm = _llm()
    if not llm:
        return text

    system = (
        "You are a precise translator. Translate user text to natural English.\n"
        "Preserve meaning; do not add extra context. Output English only."
    )
    user = text
    try:
        msg = llm.invoke([("system", system), ("user", user)])
        return (msg.content or "").strip() or text
    except Exception as e:
        logger.exception("translate_to_english failed: %s", e)
        return text

def _schema_hints(doctypes: List[str]) -> Dict[str, Any]:
    """
    Build small, bounded schema hints for the LLM (max 10 fields per doctype for brevity).
    """
    out: Dict[str, Any] = {"doctypes": []}
    if not load_schema:
        return out

    try:
        schema = load_schema()
        tables = schema.get("tables", {})
        for dt in doctypes:
            tname = f"tab{dt}"
            fields = []
            if tname in tables and "columns" in tables[tname]:
                # columns may be list of dicts or list of strings depending on your builder
                cols = tables[tname]["columns"]
                for c in cols[:10]:  # keep short
                    if isinstance(c, dict):
                        fields.append(c.get("name") or c.get("fieldname") or "")
                    else:
                        fields.append(str(c))
            out["doctypes"].append({"doctype": dt, "fields": [f for f in fields if f]})
        return out
    except Exception as e:
        logger.warning("schema hints unavailable: %s", e)
        return out

def refine_query(
    q: str,
    route_top_n: int = 4,
    max_hint_fields: int = 10
) -> Dict[str, Any]:
    """
    Normalize language, propose DocTypes, and rewrite the query for downstream systems.

    Returns:
    {
      "original": ...,
      "language": "hi|en|...",
      "translated": "...",               # if needed; else same as original
      "doctypes": ["Student", ...],      # from doctype_selector
      "refined_query": "...",            # LLM rewrite that is concise & schema-aware
      "reason": "...",                   # short rationale
      "used_llm": bool,
      "ms": int
    }
    """
    t0 = _now_ms()
    doc_types: List[str] = []
    used_llm = False

    lang = detect_language(q)
    q_en = translate_to_english(q) if lang != "en" else q
    if q_en != q:
        used_llm = True

    # 1) ask your existing selector to propose DocTypes
    try:
        doc_types = pick_doctypes(q_en, top_n=route_top_n)
    except Exception as e:
        logger.exception("doctype selection failed: %s", e)
        doc_types = []

    # 2) provide very small schema hints to help LLM phrase a crisp question
    hints = _schema_hints(doc_types)

    # 3) rewrite/refine the user query for your data systems
    llm = _llm()
    refined = q_en
    reason = "No rewrite (LLM unavailable)."
    if llm:
        used_llm = True
        system = (
            "You are a query refiner for a Learning Management System (TAP LMS).\n"
            "Goal: produce a concise, schema-aware English question we can hand to SQL/Graph or vector search.\n"
            "Constraints:\n"
            "- Do NOT invent fields; use only generic phrasing (e.g., 'grade', 'student', 'activity', 'score') if applicable.\n"
            "- If the user intent suggests filters (grade, subject, time), keep them explicit.\n"
            "- Write 1 sentence. No bullets. No extra commentary.\n"
            "- Return a compact JSON object: {\"refined_query\": \"...\", \"reason\": \"...\"} and nothing else."
        )
        # keep hints compact and safe
        user = json.dumps({
            "user_query": q_en,
            "candidate_doctypes": doc_types,
            "schema_hints": hints
        }, ensure_ascii=False)

        try:
            msg = llm.invoke([("system", system), ("user", user)])
            txt = (msg.content or "").strip()
            # Try to parse JSON; if that fails, fallback to raw text
            m = re.search(r"\{.*\}", txt, flags=re.DOTALL)
            if m:
                data = json.loads(m.group(0))
                refined = data.get("refined_query", q_en).strip() or q_en
                reason = data.get("reason", "refined")
            else:
                refined = txt
                reason = "refined"
        except Exception as e:
            logger.exception("refine LLM failed: %s", e)
            refined = q_en
            reason = "fallback to original (LLM error)."

    return {
        "original": q,
        "language": lang,
        "translated": q_en,
        "doctypes": doc_types,
        "refined_query": refined,
        "reason": reason,
        "used_llm": used_llm,
        "ms": _now_ms() - t0
    }


# -----------------------------
# Bench CLI for quick testing
# -----------------------------
def cli(q: str, route_top_n: int = 4):
    """
    Example:
      bench execute tap_lms.services.lang_refiner.cli --kwargs "{'q':'9th ke liye activities suggest karo','route_top_n':4}"
    """
    out = refine_query(q=q, route_top_n=route_top_n)
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return out

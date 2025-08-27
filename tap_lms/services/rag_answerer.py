# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_lms.infra.config import get_config
from tap_lms.services.pinecone_store import (
    search_auto_namespaces,          # LLM routes query -> DocTypes (namespaces)
    get_safe_select_columns,         # auto-pick real DB columns (no hardcoding)
)

# --- tiny, general record->text (mirrors pinecone_store’s shape)
def _record_to_text(doctype: str, row: Dict[str, Any]) -> str:
    parts = [f"DocType: {doctype}", f"ID: {row.get('name','')}"]
    for k, v in row.items():
        if k == "name": 
            continue
        if v is None or v == "":
            continue
        # make JSON-serializable-ish for display
        if hasattr(v, "isoformat"):
            v = v.isoformat()
        parts.append(f"{k}: {v}")
    return "\n".join(parts)

def _get_llm() -> ChatOpenAI:
    api_key = get_config("openai_api_key")
    model = get_config("primary_llm_model") or "gpt-4o-mini"
    if not api_key:
        raise RuntimeError("Missing openai_api_key in site_config.json")
    return ChatOpenAI(model_name=model, openai_api_key=api_key, temperature=0.2, max_tokens=800)

def _build_context_from_hits(hits: List[Dict[str, Any]], max_chars: int = 9000) -> Dict[str, Any]:
    """
    Turn Pinecone hits into a single context string by fetching rows (no hardcoding).
    We respect max_chars to stay token-safe.
    """
    context_chunks: List[str] = []
    sources: List[Dict[str, Any]] = []
    used = 0

    for h in hits:
        meta = h.get("metadata") or {}
        doctype = meta.get("doctype") or h.get("namespace")
        record_ids = meta.get("record_ids") or []
        if not doctype or not record_ids:
            continue

        # Pick columns that truly exist in SQL (skips Table/Section Break/etc.)
        fields = get_safe_select_columns(doctype)
        rows = frappe.db.get_all(doctype, filters={"name": ["in", record_ids]}, fields=fields) or []

        for r in rows:
            block = _record_to_text(doctype, r)
            if used + len(block) + 2 > max_chars:
                break
            context_chunks.append(block)
            sources.append({"doctype": doctype, "id": r.get("name")})
            used += len(block) + 2

        if used >= max_chars:
            break

    context_text = "\n\n---\n\n".join(context_chunks)
    return {"context_text": context_text, "sources": sources}

def answer_from_pinecone(
    q: str,
    k: int = 8,
    doctypes: list[str] | None = None,
    route_top_n: int = 4,
    max_context_chars: int = 9000,
) -> Dict[str, Any]:
    """
    General RAG answerer:
      1) Route to DocTypes with your schema-aware LLM router
      2) Pinecone vector search (BYO embeddings) within those namespaces
      3) Pull the matching rows from MariaDB and render neutral chunks
      4) Ask the LLM to synthesize an answer grounded ONLY in those chunks
    """
    t0 = time.time()

    # 1+2) Route & search (namespaces == DocTypes)
    routed = search_auto_namespaces(q=q, k=k, route_top_n=route_top_n, include_metadata=True)
    matches = routed.get("matches") or []

    # 3) Build neutral text context from DB (no per-DocType hardcoding)
    ctx = _build_context_from_hits(matches, max_chars=max_context_chars)
    context_text = ctx["context_text"]
    sources = ctx["sources"]

    # If no context, return graceful fallback
    if not context_text.strip():
        return {
            "question": q,
            "answer": "I don't have enough context in the knowledge base to answer that.",
            "success": True,
            "engine": "rag-pinecone",
            "execution_time": time.time() - t0,
            "metadata": {
                "routed_doctypes": routed.get("routed_doctypes"),
                "matches_preview": [
                    {"id": m.get("id"), "score": m.get("score"), "namespace": m.get("namespace")}
                    for m in matches[:5]
                ],
                "records_loaded": 0,
            },
        }

    # 4) LLM synthesis — generic instruction, grounded, no tool-specific priors
    llm = _get_llm()
    system = (
        "You are a helpful assistant that answers strictly based on the given context. "
        "If the context is insufficient, say you don't know. Be concise, and cite items by DocType and ID where helpful."
    )
    user = (
        f"Question:\n{q}\n\n"
        f"Context (multiple independent records, each starts with 'DocType:' and 'ID:'):\n\n"
        f"{context_text}\n\n"
        f"Instructions:\n"
        f"- Use only the information in the context to answer.\n"
        f"- If you cannot find enough evidence, respond with: \"I don't know based on the provided context.\"\n"
    )
    resp = llm.invoke([{"role": "system", "content": system}, {"role": "user", "content": user}])
    answer = getattr(resp, "content", None) or str(resp)

    out = {
        "question": q,
        "answer": answer.strip(),
        "success": True,
        "engine": "rag-pinecone",
        "execution_time": time.time() - t0,
        "metadata": {
            "routed_doctypes": routed.get("routed_doctypes"),
            "matches": [
                {
                    "id": m.get("id"),
                    "score": m.get("score"),
                    "namespace": m.get("namespace"),
                    "doctype": (m.get("metadata") or {}).get("doctype") or m.get("namespace"),
                    "count": (m.get("metadata") or {}).get("count"),
                }
                for m in matches
            ],
            "records_loaded": len(sources),
        },
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return out

# -------- Bench CLI --------
def cli(q: str, k: int = 8, route_top_n: int = 4):
    """
    Bench:
      bench execute tap_lms.services.rag_answerer.cli --kwargs "{'q':'recommend activities for 9th graders','k':8,'route_top_n':4}"
    """
    return answer_from_pinecone(q=q, k=k, route_top_n=route_top_n)

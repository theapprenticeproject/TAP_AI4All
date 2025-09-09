# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_lms.infra.config import get_config
from tap_lms.services.pinecone_store import (
    search_auto_namespaces,
    get_db_columns_for_doctype,
)

# --- A local version of the text formatter is needed here ---
def _to_plain(v: Any) -> str:
    """Make values JSON-safe for text conversion."""
    if v is None: return ""
    if isinstance(v, (str, int, float, bool)): return str(v)
    if hasattr(v, 'isoformat'): return v.isoformat()
    return str(v)

def _record_to_text(doctype: str, row: Dict[str, Any]) -> str:
    """
    Flattens a record to a text block, giving weight to the title field.
    """
    parts = []
    meta = frappe.get_meta(doctype)
    
    title_field = None
    title_value = None

    official_title_field = meta.title_field
    if official_title_field and official_title_field in row and row[official_title_field]:
        title_field = official_title_field
        title_value = row[title_field]
    else:
        fallback_fields = [
            'title', 'name1', 'video_name', 'assignment_name', 'project_name', 
            'quiz_name', 'objective_name', 'unit_name', 'comp_name', 'note_name'
        ]
        for field in fallback_fields:
            if field in row and row[field]:
                title_field = field
                title_value = row[field]
                break

    if title_field and title_value:
        title_label = meta.get_field(title_field).label or title_field.replace("_", " ").title()
        parts.append(f"{title_label}: {title_value}")

    parts.append(f"DocType: {doctype}")
    parts.append(f"ID: {row.get('name','')}")
    
    for k, v in row.items():
        if k in ('name', title_field) or v in (None, ""):
            continue
        parts.append(f"{k}: {_to_plain(v)}")
        
    return "\n".join(parts)


def _get_llm(model: str = "gpt-4o-mini", temperature: float = 0.2) -> ChatOpenAI:
    api_key = get_config("openai_api_key")
    if not api_key: raise RuntimeError("Missing openai_api_key")
    return ChatOpenAI(model_name=model, openai_api_key=api_key, temperature=temperature, max_tokens=1500)

def _build_context_from_hits(hits: List[Dict[str, Any]], max_chars: int = 12000) -> Dict[str, Any]:
    """
    Builds context by fetching full records from Frappe DB based on Pinecone hit metadata.
    This is the robust "two-step fetch" method.
    """
    context_chunks: List[str] = []
    sources: List[Dict[str, Any]] = []
    used_chars = 0
    
    # This import is now needed here
    from tap_lms.services.pinecone_store import _record_to_text, get_db_columns_for_doctype

    for h in hits:
        meta = h.get("metadata") or {}
        doctype = meta.get("doctype")
        record_ids = meta.get("record_ids", [])
        
        if not doctype or not record_ids: continue

        try:
            fields = get_db_columns_for_doctype(doctype)
            rows = frappe.get_all(doctype, filters={"name": ("in", record_ids)}, fields=fields) or []
            
            for row in rows:
                text_chunk = _record_to_text(doctype, row)
                if used_chars + len(text_chunk) > max_chars:
                    break
                
                context_chunks.append(text_chunk)
                sources.append({"doctype": doctype, "id": row.get("name"), "score": h.get("score")})
                used_chars += len(text_chunk)
        
        except Exception as e:
            frappe.log_error(f"Failed to fetch records for context building: {e}")

        if used_chars >= max_chars: break
            
    return {"context_text": "\n\n---\n\n".join(context_chunks), "sources": sources}

# --- Contextual Query Refiner ---

REFINER_PROMPT = """Given a chat history and a follow-up question, rewrite the follow-up question to be a standalone question that a search engine can understand, incorporating the necessary context from the history.

Return ONLY the refined, standalone question. Do not answer the question.
"""

def _refine_query_with_history(query: str, chat_history: List[Dict[str, str]]) -> str:
    """Uses an LLM to make a follow-up question self-contained."""
    if not chat_history:
        return query

    llm = _get_llm(temperature=0.0) # Use zero temperature for predictable refinement
    history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in chat_history])
    prompt = (
        f"CHAT HISTORY:\n{history_str}\n\n"
        f"FOLLOW-UP QUESTION:\n{query}\n\n"
        f"REFINED STANDALONE QUESTION:"
    )
    
    try:
        resp = llm.invoke([("system", REFINER_PROMPT), ("user", prompt)])
        refined_query = getattr(resp, "content", query).strip()
        print(f"> Refined Query for Search: {refined_query}")
        return refined_query
    except Exception as e:
        frappe.log_error(f"Query refinement failed: {e}")
        return query

# --- Main Answer Function ---

def answer_from_pinecone(
    q: str,
    k: int = 8,
    route_top_n: int = 4,
    chat_history: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    t0 = time.time()
    chat_history = chat_history or []

    # 1. Refine the query with history before searching
    refined_query = _refine_query_with_history(q, chat_history)

    # 2. Route & search using the refined query (NO METADATA FILTERS)
    routed = search_auto_namespaces(q=refined_query, k=k, route_top_n=route_top_n)
    matches = routed.get("matches") or []

    # 3. Build context from DB hits (two-step fetch)
    ctx = _build_context_from_hits(matches)
    context_text = ctx["context_text"]
    sources = ctx["sources"]

    if not context_text.strip():
        return {
            "question": q,
            "answer": "I don't have enough context in the knowledge base to answer that.",
            "success": True,
            "engine": "rag-pinecone",
            "execution_time": time.time() - t0,
            "metadata": {"refined_query_for_search": refined_query, "routed_doctypes": routed.get("routed_doctypes"), "sources": []}
        }

    # 4. LLM synthesis with chat history and ORIGINAL question
    llm = _get_llm()
    system_prompt = (
        "You are a helpful assistant. Answer the user's final question using ONLY the provided CONTEXT and CHAT HISTORY. "
        #"If the context is insufficient, say you don't know. Be concise."
    )
    
    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(chat_history)
    messages.append({
        "role": "user",
        "content": f"CONTEXT:\n{context_text}\n\nBased on the context, answer this question:\n{q}"
    })

    resp = llm.invoke(messages)
    answer = getattr(resp, "content", None) or str(resp)

    # Final currency symbol fix on the answer string
    if isinstance(answer, str):
        answer = answer.replace('\\u20b9', '₹')

    out = {
        "question": q,
        "answer": answer.strip(),
        "success": True,
        "engine": "rag-pinecone",
        "execution_time": time.time() - t0,
        "metadata": {
            "refined_query_for_search": refined_query,
            "routed_doctypes": routed.get("routed_doctypes"),
            "sources": sources
        },
    }
    return out


# -------- Bench CLI --------
def cli(q: str, k: int = 8, route_top_n: int = 4):
    """
    Bench command to test the RAG pipeline.

    bench execute tap_lms.services.rag_answerer.cli --kwargs "{'q':'Find a video about financial literacy and goal setting and summarize its key points'}"
    bench execute tap_lms.services.rag_answerer.cli --kwargs "{'q':'Can you provide a summary of the video titled Needs First, Wants Later (2024)'}"
    """
    return answer_from_pinecone(q=q, k=k, route_top_n=route_top_n)

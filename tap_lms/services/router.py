# tap_lms/services/router.py
import frappe
from tap_lms.services.lang_refiner import refine_query
from tap_lms.services.doctype_selector import pick_doctypes
from tap_lms.services.graph_rag import answer_graph
from tap_lms.services.sql_agent import answer_sql
from tap_lms.services.rag_answerer import answer_from_pinecone
from tap_lms.infra.config import get_config

def answer(q: str) -> dict:
    # Step 1: refine language & intent
    ref = refine_query(q)
    refined = ref["refined_query"]
    
    # Step 2: decide doctypes once
    doctypes = pick_doctypes(refined, top_n=4)

    # Step 3: choose primary engine
    neo_ready = bool(get_config("neo4j_uri")) and get_config("enable_neo4j")
    if neo_ready:
        # Try GRAPH first
        g = answer_graph(refined, doctypes=doctypes)
        if not _is_failure(g):
            return _with_meta(g, refined, doctypes, primary="graph", fallback=False)
        # Fallback to Pinecone
        p = answer_from_pinecone(refined, doctypes=doctypes, k=8, route_top_n=4)
        return _with_meta(p, refined, doctypes, primary="graph", fallback=True)
    else:
        # Try SQL first
        s = answer_sql(refined, doctypes=doctypes)
        if not _is_failure(s):
            return _with_meta(s, refined, doctypes, primary="sql", fallback=False)
        # Fallback to Pinecone
        p = answer_from_pinecone(refined, doctypes=doctypes, k=8, route_top_n=4)
        return _with_meta(p, refined, doctypes, primary="sql", fallback=True)

def _is_failure(res: dict) -> bool:
    """Heuristic failure detector (no brittle hard-coding)."""
    if not res or not res.get("success"):
        return True
    text = (res.get("answer") or "").strip().lower()
    # soft signals that appear across libs/models:
    bad_phrases = ("i don't know", "unable to", "cannot", "no answer", "failed", "error", "iteration limit", "time limit", "")
    if any(p in text for p in bad_phrases):
        return True
    # also consider engine-specific metadata:
    meta = res.get("metadata") or {}
    if meta.get("no_results") is True:
        return True
    if meta.get("vector_context_items") == 0 and meta.get("vector_used") is True and not text:
        return True
    return False

def _with_meta(res: dict, refined: str, doctypes: list[str], primary: str, fallback: bool) -> dict:
    res.setdefault("metadata", {})
    res["metadata"].update({
        "refined_query": refined,
        "doctypes_used": doctypes,
        "primary_engine": primary,
        "fallback_used": fallback,
    })
    return res

# Bench CLI
def cli(q: str):
    '''
    bench execute tap_lms.services.router.cli --kwargs "{'q':'list all the activities present'}"
    '''
    out = answer(q)
    print(frappe.as_json(out, indent=2))
    return out

# tap_lms/api/query.py
import frappe
from tap_lms.services.router import answer as route_query
from tap_lms.services.ratelimit import check_rate_limit
from tap_lms.infra.ai_logging import log_query_event, now_ms


@frappe.whitelist(methods=["GET", "POST"], allow_guest=False)
def query(q: str = None, limit: int = 50):
    """
    Public API: Unified Router (Graph → SQL → Pinecone fallback).
    Supports both GET and POST.
    """
    auth = frappe.get_request_header("Authorization") or ""
    api_key = None
    if auth.lower().startswith("token "):
        try:
            api_key = auth.split()[1].split(":")[0]
        except Exception:
            api_key = None

    ok, remaining, reset = check_rate_limit(api_key, scope="router_query", limit=60, window_sec=60)
    if not ok:
        frappe.throw(
            f"Rate limit exceeded. Try again after {reset}. Remaining: {remaining}",
            frappe.TooManyRequestsError,
        )

    # If it's a POST and q is not passed as arg, try extracting from JSON body
    if not q and frappe.request and frappe.request.method == "POST":
        data = frappe.local.form_dict or {}
        q = data.get("q")

    if not q:
        frappe.throw("Missing required param: q")

    t0 = now_ms()
    out = {}
    success = False
    try:
        out = route_query(q)  # Graph → SQL → Pinecone
        success = out.get("success", True)
        return out
    finally:
        log_query_event({
            "question": q,
            "engine": out.get("engine") if out else "router",
            "success": 1 if success else 0,
            "execution_ms": now_ms() - t0,
            "api_key": api_key,
            "response_preview": str(out)[:300] if out else "error",
            "answer": out.get("answer") if out else "error",
            "metadata": out.get("metadata") if out else {},
        })
# tap_lms/api/query.py
import frappe
from tap_lms.services.sql_agent import answer_sql

@frappe.whitelist(methods=["GET"], allow_guest=False)
def query(q: str, engine: str = "sql", limit: int = 50):
    """Public API: routed Q&A. Auth required (API key/secret or session)."""
    if not q:
        frappe.throw("Missing required param: q")
    if engine != "sql":
        frappe.throw("Only 'sql' engine is enabled on this endpoint")

    out = answer_sql(q)
    # Optional: respect limit if your agent supports it
    return out  # Frappe will JSON-serialize dicts

@frappe.whitelist(allow_guest=False, methods=["GET"])
def explain(q: str):
    """
    Explain what SQL the agent intends to run (no execution).
    Usage:
      curl -s -G "http://localhost:8000/api/method/tap_lms.api.query.explain" \
        -H "Authorization: token KEY:SECRET" \
        --data-urlencode "q=list students in grade 9 with school"
    """
    from tap_lms.services.sql_agent import explain_sql
    out = explain_sql(q)
    return out
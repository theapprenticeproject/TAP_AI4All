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

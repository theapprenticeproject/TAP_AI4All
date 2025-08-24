# File: tap_lms/services/metrics.py
from typing import Any, Dict
import json
import time
import frappe

def log_query_event(event: Dict[str, Any]) -> None:
    """
    Tries to store an audit row into DocType 'AI Query Log' if present,
    otherwise prints to server log. Non-blocking best effort.
    Expected fields in `event`:
      question, engine, success, execution_ms, candidate_sql, tables, allowlist_ok,
      user, api_key_prefix, ip, status, error
    """
    try:
        # Redact secrets; keep only api_key prefix if provided
        event = dict(event)
        if "api_key" in event and event["api_key"]:
            event["api_key_prefix"] = event["api_key"][:6]
            event.pop("api_key", None)

        # If a DocType exists, insert a row (optional)
        if frappe.db.exists("DocType", "AI Query Log"):
            doc = frappe.new_doc("AI Query Log")
            doc.query_text = event.get("question")
            doc.engine = event.get("engine")
            doc.success = 1 if event.get("success") else 0
            doc.execution_ms = int(event.get("execution_ms") or 0)
            doc.candidate_sql = event.get("candidate_sql")
            doc.tables = ", ".join(event.get("tables") or [])
            doc.allowlist_ok = 1 if event.get("allowlist_ok") else 0
            doc.meta_json = json.dumps(event, ensure_ascii=False)
            doc.insert(ignore_permissions=True)
        else:
            frappe.logger("tap_lms.audit").info("[AI-QUERY] %s", json.dumps(event, ensure_ascii=False))
    except Exception:
        # Never break user requests for logging problems
        pass

def now_ms() -> int:
    return int(time.time() * 1000)

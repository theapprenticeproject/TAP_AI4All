# tap_lms/infra/ai_logging.py
import json, frappe
import time

def now_ms() -> int:
    return int(time.time() * 1000)

SAFE_PREVIEW_LEN = 500

def log_query_event(payload: dict):
    """
    payload keys you can pass:
      question, engine, success, execution_ms, candidate_sql,
      tables (list or csv or json-str), allowlist_ok, user, api_key, ip, status,
      response_preview, error, metadata (dict)
    """
    # sanitize
    api_key_last4 = None
    api_key = payload.get("api_key") or ""
    if isinstance(api_key, str):
        api_key_last4 = api_key[-4:] if len(api_key) >= 4 else api_key

    tables = payload.get("tables")
    if isinstance(tables, (list, tuple)):
        tables = json.dumps(list(tables))

    meta = payload.get("metadata")
    if isinstance(meta, dict):
        meta = json.dumps(meta)

    preview = (payload.get("answer") or "")[:SAFE_PREVIEW_LEN]

    doc = frappe.get_doc({
        "doctype": "AI Query Log",
        "occurred_on": frappe.utils.now_datetime(),
        "user": payload.get("user") or (frappe.session.user if frappe.session else "guest"),
        "engine": payload.get("engine") or "sql",
        "question": payload.get("question") or "",
        "status": payload.get("status") or ("ok" if payload.get("success") else "error"),
        "success": 1 if payload.get("success") else 0,
        "execution_ms": int(payload.get("execution_ms") or 0),
        "ip": payload.get("ip"),
        "api_key_last4": api_key_last4,
        "candidate_sql": payload.get("candidate_sql"),
        "tables": tables,
        "allowlist_ok": 1 if payload.get("allowlist_ok") else 0,
        "response_preview": preview,
        "error": payload.get("error"),
        "metadata": meta,
    })
    doc.insert(ignore_permissions=True)
    frappe.db.commit()

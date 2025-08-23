# File: tap_lms/api/schema.py
import os
import json
import frappe

@frappe.whitelist(allow_guest=False, methods=["GET", "POST"])
def refresh():
    """
    Regenerate tap_lms_schema.json (allowlist + joins) and hot-reload it.
    Usage:
      curl -s -H "Authorization: token KEY:SECRET" \
        http://localhost:8000/api/method/tap_lms.api.schema.refresh
    """
    # 1) Generate schema (expects your generator to return the dict)
    try:
        from tap_lms.schema.generate_schema import generate  # your generator
        schema = generate()
    except Exception as e:
        frappe.throw(f"Schema generation failed: {e}")

    # 2) Write to JSON file
    try:
        base = frappe.get_app_path("tap_lms", "schema")
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, "tap_lms_schema.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
    except Exception as e:
        frappe.throw(f"Failed to write schema file: {e}")

    # 3) Hot-reload into the SQL catalog
    try:
        from tap_lms.infra.sql_catalog import reload_schema
        reload_schema()
    except Exception as e:
        frappe.throw(f"Schema file written, but hot-reload failed: {e}")

    return {
        "ok": True,
        "wrote": path,
        "tables": len(schema.get("allowlist", [])),
        "joins": len(schema.get("allowed_joins", [])),
    }

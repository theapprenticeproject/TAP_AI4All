import os, json, re
from typing import Dict, List, Any, Tuple

# Adjust if your path differs
APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCTYPE_DIR = os.path.join(APP_ROOT, "tap_lms", "doctype")  # tap_lms/tap_lms/doctype
OUT_PATH = os.path.join(os.path.dirname(__file__), "tap_lms_schema.json")

SYSTEM_DTYPES_PREFIXES = ("__","_")  # quick filter for non-business doctypes

def snake_to_title(s: str) -> str:
    return re.sub(r"[_\-]+", " ", s).title()

def load_doctype(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return json.load(f)

def discover() -> Tuple[Dict, List[Dict], Dict, Dict]:
    """
    Returns:
      tables: {table_name: {doctype, display_field, pk, columns, description}}
      joins:  [{"left_table","left_key","right_table","right_key","why"}]
      aliases: suggested column aliases for nicer prompts
      allowlist: {table_name: True}
    """
    tables: Dict[str, Dict] = {}
    joins: List[Dict] = []
    aliases: Dict[str, List[str]] = {}
    allowlist: Dict[str, bool] = {}

    for root, _, files in os.walk(DOCTYPE_DIR):
        for file in files:
            if not file.endswith(".json"): continue
            path = os.path.join(root, file)
            try:
                doc = load_doctype(path)
            except Exception:
                continue
            if doc.get("doctype") != "DocType":
                continue

            doctype = doc.get("name")
            if not doctype or doctype.startswith(SYSTEM_DTYPES_PREFIXES):
                continue

            # Frappe table name convention
            table_name = f"tab{doctype}"
            allowlist[table_name] = True

            # Collect fields/columns
            fields = doc.get("fields", [])
            columns = []
            display_field = None
            for f in fields:
                fname = f.get("fieldname")
                if not fname: continue
                columns.append(fname)
                # pick display field preference: name1 > title > label-ish
                if display_field is None and f.get("fieldtype") in ("Data","Small Text","Text","Read Only"):
                    if fname == "name1": display_field = "name1"
                # fallbacks if no name1 later

            if display_field is None:
                if "name1" in columns: display_field = "name1"
                elif "title" in columns: display_field = "title"
                else: display_field = None  # agent will handle

            # Table description heuristic
            desc = doc.get("module") or snake_to_title(doctype)
            human_desc = f"{snake_to_title(doctype)} records. Key columns: name (PK)"
            if display_field: human_desc += f", {display_field} (display)"
            human_desc += "."

            tables[table_name] = {
                "doctype": doctype,
                "pk": "name",
                "display_field": display_field,
                "columns": sorted(set(columns + ["name"])),
                "description": human_desc
            }

            # Build joins from Link fields: source.field (FK) -> target.name
            for f in fields:
                if f.get("fieldtype") == "Link" and f.get("options"):
                    left_key = f.get("fieldname")
                    right_doctype = f.get("options")
                    right_table = f"tab{right_doctype}"
                    why = f"{doctype}.{left_key} links to {right_doctype}.name"
                    joins.append({
                        "left_table": table_name,
                        "left_key": left_key,
                        "right_table": right_table,
                        "right_key": "name",
                        "why": why
                    })

            # Suggested aliases (nice to have)
            if display_field:
                aliases[f"{doctype.lower()}_name"] = [table_name, display_field]
            aliases[f"{doctype.lower()}_id"] = [table_name, "name"]

    return tables, joins, aliases, allowlist

def write_schema(payload: Dict[str, Any]):
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(f"✅ Wrote schema: {OUT_PATH}")

def main():
    tables, joins, aliases, allowlist = discover()

    # Optional: injected guardrails for agent prompts
    guardrails = [
        "Use ONLY tables listed in allowlist.",
        "Use ONLY joins defined in allowed_joins.",
        "PK is always 'name'. Prefer display_field when showing names.",
        "Always include LIMIT for raw listings.",
    ]

    payload = {
        "tables": tables,
        "allowed_joins": joins,
        "aliases": aliases,
        "allowlist": sorted([t for t in allowlist.keys()]),
        "guardrails": guardrails,
        # space for canned views if you want to add later
        "safe_views": {}
    }
    write_schema(payload)

if __name__ == "__main__":
    main()

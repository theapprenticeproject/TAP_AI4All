# tap_lms/infra/sql_catalog.py
import json, os
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schema", "tap_lms_schema.json")

def load_schema():
    with open(SCHEMA_PATH, "r") as f:
        return json.load(f)

# File: ~/frappe-bench/apps/tap_lms/tap_lms/infra/db.py

from __future__ import annotations
import logging
from typing import Iterable, List, Optional

import frappe
from sqlalchemy import create_engine, text
from langchain_community.utilities import SQLDatabase

from tap_lms.infra.config import get_config, get_neo4j_config

logger = logging.getLogger(__name__)


def _get_mariadb_uri() -> str:
    """Build a SQLAlchemy URI from Frappe's site_config."""
    site = frappe.get_site_config()
    db = site.get("db_name")
    user = site.get("db_user", db)
    pwd = site.get("db_password")
    host = site.get("db_host", "127.0.0.1")
    port = site.get("db_port", 3306)

    if not (db and user and pwd):
        raise RuntimeError("Missing DB credentials in site_config.json")

    return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"

# build allow-list from DocTypes in a given Frappe module/app
def get_allowlisted_tables(
    module_name: Optional[str] = "TAP LMS",  # change if module name differs
    extra_doctypes: Optional[Iterable[str]] = None,
    exclude_doctypes: Optional[Iterable[str]] = None,
) -> List[str]:
    """
    Return ['tabDoctypeA', 'tabDoctypeB', ...] limited to your app/module.
    - module_name: DocType.module value to filter (e.g., "TAP LMS")
    - extra_doctypes: iterable of DocType names to force-include
    - exclude_doctypes: iterable of DocType names to exclude (even if module matches)
    """
    include = set()
    try:
        filters = {}
        if module_name:
            filters["module"] = module_name
        names = frappe.get_all("DocType", filters=filters, pluck="name")

        include.update(names)
        if extra_doctypes:
            include.update(extra_doctypes)
        if exclude_doctypes:
            include.difference_update(exclude_doctypes)

        # convert DocType -> table name
        tables = [f"tab{n}" for n in sorted(include)]
        # hard exclude obvious system/core tables if somehow present
        system_prefixes = {"__", "tab_"}  # __Auth, tab__...
        tables = [t for t in tables if not any(t.startswith(p) for p in system_prefixes)]
        return tables
    except Exception as e:
        # fallback to a conservative minimal set if discovery fails
        logger.warning("Allowlist discovery failed: %s", e)
        return []

def get_sqldb(
    include_tables: Optional[Iterable[str]] = None,
    sample_rows_in_table_info: int = 3,
) -> SQLDatabase:
    """
    Return a LangChain SQLDatabase bound to MariaDB (Frappe).
    If include_tables is None, we auto-allowlist by module.
    """
    uri = _get_mariadb_uri()
    if include_tables is None:
        include_tables = get_allowlisted_tables(module_name="TAP LMS")
        if not include_tables:
            # absolute fallback: expose only the most relevant app tables you know you'll need
            include_tables = [
                # put a safe minimal subset here if needed
                # e.g. 'tabStudent', 'tabSchool', 'tabEnrollment', ...
            ]
    return SQLDatabase.from_uri(
        uri,
        include_tables=list(include_tables) if include_tables else None,
        sample_rows_in_table_info=sample_rows_in_table_info,
    )


def self_test() -> bool:
    """
    Bench-executable smoke test.
    Usage:
      bench execute tap_lms.infra.db.self_test
    """
    print("🔧 tap_lms.infra.db.self_test starting...")
    uri = _get_mariadb_uri()
    safe_uri_tail = uri.split("@")[-1]
    print(f"🔗 MariaDB: @{safe_uri_tail}")

    eng = create_engine(uri)
    with eng.connect() as conn:
        ok = conn.execute(text("SELECT 1 AS ok")).scalar()
        print(f"✅ SQL ping: {ok}")

    # NEW: show allowlisted tables
    allowlist = get_allowlisted_tables(module_name="TAP LMS")
    print(f"✅ Allowlisted DocTypes (count={len(allowlist)}): {', '.join(allowlist[:20])}{' ...' if len(allowlist) > 20 else ''}")

    db = get_sqldb(sample_rows_in_table_info=1)  # now uses allowlist by default
    tables = sorted(db.get_usable_table_names())
    print(f"📋 Tables visible to agent (count={len(tables)}): {', '.join(tables[:20])}{' ...' if len(tables) > 20 else ''}")

    # Optional: Neo4j quick check if configured
    neo = get_neo4j_config()
    if neo.get("uri"):
        try:
            from neo4j import GraphDatabase

            drv = GraphDatabase.driver(neo["uri"], auth=(neo["user"], neo["password"]))
            with drv.session(database=neo.get("database", "neo4j")) as s:
                msg = s.run("RETURN 'ok' AS status").single()["status"]
                print(f"✅ Neo4j ping: {msg}")
            drv.close()
        except Exception as e:
            print(f"⚠️ Neo4j check skipped/failed: {e}")

    print("🎉 self_test completed.")
    return True

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

def _get_existing_tables(uri: str) -> Set[str]:
    """Return the set of actual table names present in this MariaDB schema."""
    eng = create_engine(uri)
    with eng.connect() as conn:
        # Works on MariaDB/MySQL
        rows = conn.execute(
            text("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
        ).fetchall()
    return {r[0] for r in rows}

# build allow-list from DocTypes in a given Frappe module/app
def get_allowlisted_tables(
    module_name: Optional[str] = "TAP LMS",
    extra_doctypes: Optional[Iterable[str]] = None,
    exclude_doctypes: Optional[Iterable[str]] = None,
) -> List[str]:
    """
    Build ['tabX', 'tabY', ...] for DocTypes in your app/module that actually have backing tables.
    - Excludes Single DocTypes (issingle=1) and Virtual DocTypes.
    - Intersects with information_schema to avoid missing-table errors.
    """
    include: Set[str] = set()

    # 1) Pull DocTypes from Frappe, excluding Singles/Virtual
    filters = {"issingle": 0, "is_virtual": 0}
    if module_name:
        filters["module"] = module_name

    names = frappe.get_all("DocType", filters=filters, pluck="name")
    include.update(names)

    if extra_doctypes:
        include.update(extra_doctypes)
    if exclude_doctypes:
        include.difference_update(exclude_doctypes)

    # Convert DocType -> table name
    candidate_tables = {f"tab{n}" for n in include}

    # 2) Keep only tables that actually exist in the DB
    uri = _get_mariadb_uri()
    existing = _get_existing_tables(uri)
    allowlisted = sorted(candidate_tables & existing)

    # Log any that were dropped
    dropped = sorted(candidate_tables - existing)
    if dropped:
        logger.info("Skipping non-existent tables (Singles/virtual or not in schema): %s", ", ".join(dropped))

    # 3) Hard exclude obvious system patterns just in case
    system_prefixes = {"__", "tab_"}  # e.g., __Auth, tab__something
    allowlisted = [t for t in allowlisted if not any(t.startswith(p) for p in system_prefixes)]

    return allowlisted

def get_sqldb(
    include_tables: Optional[Iterable[str]] = None,
    sample_rows_in_table_info: int = 3,
) -> SQLDatabase:
    """
    Return a LangChain SQLDatabase bound to MariaDB (Frappe).
    If include_tables is None, auto-allowlist by module & real tables.
    """
    uri = _get_mariadb_uri()
    if include_tables is None:
        include_tables = get_allowlisted_tables(module_name="TAP LMS")
        if not include_tables:
            logger.warning("Allowlist is empty; falling back to no include_tables (exposes all tables).")
            include_tables = None

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

    from sqlalchemy import create_engine, text
    eng = create_engine(uri)
    with eng.connect() as conn:
        ok = conn.execute(text("SELECT 1 AS ok")).scalar()
        print(f"✅ SQL ping: {ok}")

    allowlist = get_allowlisted_tables(module_name="TAP LMS")
    print(f"✅ Allowlisted tables (count={len(allowlist)}): {', '.join(allowlist[:20])}{' ...' if len(allowlist) > 20 else ''}")

    db = get_sqldb(sample_rows_in_table_info=1)
    tables = sorted(db.get_usable_table_names())
    print(f"📋 Tables visible to agent (count={len(tables)}): {', '.join(tables[:20])}{' ...' if len(tables) > 20 else ''}")
    return True

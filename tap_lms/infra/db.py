# File: ~/frappe-bench/apps/tap_lms/tap_lms/infra/db.py

from __future__ import annotations
import logging
from typing import Iterable, Optional

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


def get_sqldb(
    include_tables: Optional[Iterable[str]] = None,
    sample_rows_in_table_info: int = 3,
) -> SQLDatabase:
    """
    Return a LangChain SQLDatabase bound to MariaDB (Frappe).
    """
    uri = _get_mariadb_uri()
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
    safe_uri_tail = uri.split("@")[-1]  # don't print credentials
    print(f"🔗 MariaDB: @{safe_uri_tail}")

    # Raw SQLAlchemy ping
    eng = create_engine(uri)
    with eng.connect() as conn:
        ok = conn.execute(text("SELECT 1 AS ok")).scalar()
        print(f"✅ SQL ping: {ok}")

    # LangChain SQLDatabase basic info
    db = get_sqldb(sample_rows_in_table_info=1)
    tables = sorted(db.get_usable_table_names())
    shown = ", ".join(tables[:15])
    more = f" (+{len(tables)-15} more)" if len(tables) > 15 else ""
    print(f"📋 Tables visible to agent: {shown}{more}")

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

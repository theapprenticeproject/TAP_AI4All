# File: tap_lms/infra/neo4j_migrator.py

from __future__ import annotations
import math
import time
import logging
import re
from typing import Dict, List, Any, Iterable, Optional, Tuple

import frappe
from neo4j import GraphDatabase, basic_auth
from langchain_openai import OpenAIEmbeddings

from tap_lms.infra.config import get_config, get_neo4j_config
from tap_lms.infra.sql_catalog import load_schema

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# -------------------------------
# Utilities
# -------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)

def _chunked(seq: Iterable[Any], n: int) -> Iterable[List[Any]]:
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def _infer_emb_dim(model: str) -> int:
    # Common OpenAI dims
    if "text-embedding-3-large" in model:
        return 3072
    # default
    return 1536

# List of text-ish keys to prefer when composing embedding text
PREFERRED_TEXT_KEYS = [
    "display_name", "name1", "title", "subject", "description", "note",
    "question", "content", "body", "text", "prompt", "learning_objective",
    "sel_skill", "content_skill", "rigor", "level", "language",
]

# -------------------------------
# Migrator
# -------------------------------

class LMSNeo4jMigrator:
    """
    Schema-driven migrator for tap_lms:
      - Nodes from allowlisted DocTypes
      - Relationships from allowed_joins
      - OpenAI embeddings on 'comprehensive_embedding'
      - Vector indexes per label
    """

    def __init__(self):
        self.schema: Dict[str, Any] = load_schema()
        self.allowlist: List[str] = self.schema.get("allowlist", [])
        self.joins: List[Dict[str, Any]] = self.schema.get("allowed_joins", [])
        self.aliases: Dict[str, str] = self.schema.get("aliases", {})
        self.tables: Dict[str, Any] = self.schema.get("tables", {})

        # Neo4j
        neo = get_neo4j_config()
        self.neo_uri = neo["uri"]
        self.neo_user = neo["user"]
        self.neo_pwd = neo["password"]
        self.neo_db = neo.get("database", "neo4j")
        self._driver = GraphDatabase.driver(self.neo_uri, auth=basic_auth(self.neo_user, self.neo_pwd))

        # Embeddings
        self.embed_model = get_config("embedding_model") or "text-embedding-3-small"
        self.emb_dim = _infer_emb_dim(self.embed_model)
        api_key = get_config("openai_api_key")
        if not api_key:
            logger.warning("OpenAI API key missing; embedding features will fail.")
        self._embedder = OpenAIEmbeddings(model=self.embed_model, api_key=api_key)

        # Performance knobs
        self.batch_size = int(get_config("batch_size") or 100)
        self.max_context_len = int(get_config("max_context_length") or 2048)

    # --------- Core steps

    def _safe_label(self, doctype: str) -> str:
       """Map Frappe DocType to a valid Neo4j label (no spaces/symbols/leading digits)."""
       label = re.sub(r"\W+", "_", doctype).strip("_") or "Doc"
       if re.match(r"^\d", label):
           label = f"_{label}"
       return label

    def create_constraints_and_indexes(self):
        """
        Create idempotent constraints and helpful indexes for all discovered doctypes.
        - UNIQUE(name) per label
        - INDEX(display_name) when property exists on any node
        """
        print("🔧 Creating Neo4j constraints & indexes…")
        db = self.neo4j_config.get("database", "neo4j")
        total_constraints = 0
        total_indexes = 0
    
        if not getattr(self, "discovered_doctypes", None):
            print("⚠️  No discovered doctypes found on the migrator; run discovery first.")
            return {"constraints": 0, "indexes": 0}
    
        with self.driver.session(database=db) as session:
            for doctype, info in self.discovered_doctypes.items():
                label = self._safe_label(doctype)
                # ---- UNIQUE(name) ----
                constraint_name = f"{label.lower()}_name_unique"
                cql_constraint = f"""
                CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
                FOR (n:{label}) REQUIRE n.name IS UNIQUE
                """
                try:
                    session.run(cql_constraint)
                    print(f"  ✅ Constraint ensured: {constraint_name} on :{label}(name)")
                    total_constraints += 1
                except Exception as e:
                    print(f"  ⚠️ Constraint error for {label}: {e}")
    
                # ---- INDEX(display_name) if property is present on any node of this label ----
                # cheap existence probe; avoids creating useless indexes
                try:
                    has_display = session.run(
                        f"MATCH (n:{label}) WHERE exists(n.display_name) RETURN 1 AS ok LIMIT 1"
                    ).single()
                    if has_display:
                        index_name = f"{label.lower()}_display_name_idx"
                        cql_index = f"""
                        CREATE INDEX {index_name} IF NOT EXISTS
                        FOR (n:{label}) ON (n.display_name)
                        """
                        session.run(cql_index)
                        print(f"  ✅ Index ensured: {index_name} on :{label}(display_name)")
                        total_indexes += 1
                except Exception as e:
                    print(f"  ⚠️ Index check/create error for {label}: {e}")
    
        print(f"✅ Constraint/Index pass complete — created/ensured {total_constraints} constraints, {total_indexes} indexes.")
        return {"constraints": total_constraints, "indexes": total_indexes}

    def create_constraints(self) -> None:
        with self._driver.session(database=self.neo_db) as s:
            for table in self.allowlist:
                label = self._label_from_table(table)
                constraint_name = f"{label.lower()}_name_unique"
                index_name = f"{label.lower()}_display_name_idx"
    
                s.run(
                    f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
                    f"FOR (n:{label}) REQUIRE n.name IS UNIQUE"
                )
                s.run(
                    f"CREATE INDEX {index_name} IF NOT EXISTS "
                    f"FOR (n:{label}) ON (n.display_name)"
                )
                logger.info("Constraints & indexes ensured for %s", label)


    def migrate_nodes(self, labels: Optional[list[str]] = None, batch_size: Optional[int] = None) -> Dict[str, int]:
        """
        Pull rows from MariaDB and MERGE nodes in Neo4j.
        """
        counts: Dict[str, int] = {}
        tables = [t for t in self.allowlist if (labels is None or self._label_from_table(t) in labels)]
        bs = batch_size or self.batch_size
    
        for table in self.allowlist:
            doctype = table[3:] if table.startswith("tab") else table
            label = self._label_from_table(table)
            try:
                total = frappe.db.count(doctype)
            except Exception as e:
                logger.warning("Skipping %s – count failed: %s", doctype, e)
                counts[label] = 0
                continue
          
            if total == 0:
                counts[label] = 0
                continue

            logger.info("Migrating %s (%d rows)...", doctype, total)
            migrated = 0
    
            for offset in range(0, total, bs):
                rows = frappe.get_all(doctype, fields="*", start=offset, page_length=bs)
                with self._driver.session(database=self.neo_db) as s:
                    for row in rows:
                        props = self._prepare_node_props(row, doctype)
                        if not props:
                            continue
                        s.run(f"MERGE (n:{label} {{name:$name}}) SET n += $props",
                              {"name": props["name"], "props": props})
                        migrated += 1
    
            counts[label] = migrated
            logger.info("✓ %s: %d/%d migrated", label, migrated, total)
    
        return counts


    def create_relationships(self) -> Dict[str, int]:
        rel_counts: Dict[str, int] = {}
        with self._driver.session(database=self.neo_db) as s:
            for j in self.joins:
                lt, lk, rt, rk = j["left_table"], j["left_key"], j["right_table"], j["right_key"]
                rel = j.get("rel") or self._rel_name_from_tables(lt, rt, lk, rk)
                rel = self._safe_rel(rel)  # <<< enforce safety even if provided in schema
                llabel, rlabel = self._label_from_table(lt), self._label_from_table(rt)
                
    
                cypher = f"""
                MATCH (l:{llabel}), (r:{rlabel})
                WHERE l.{lk} IS NOT NULL AND r.{rk} IS NOT NULL AND l.{lk} = r.{rk}
                WITH l, r LIMIT 500000
                MERGE (l)-[:{rel}]->(r)
                """
                try:
                    summary = s.run(cypher).consume()
                    created = summary.counters.relationships_created
                    rel_counts[f"{llabel}-{rel}->{rlabel}"] = created
                    if created:
                        logger.info("Rel %s %s->%s: +%d", rel, llabel, rlabel, created)
                except Exception as e:
                    logger.warning("Rel creation failed for %s.%s -> %s.%s: %s", lt, lk, rt, rk, e)
        return rel_counts


    def build_embeddings_for_all_labels(self, labels: Optional[list[str]] = None, batch_size: Optional[int] = None, resume: bool = True) -> Dict[str, int]:
        out: Dict[str, int] = {}
        target_labels = labels or [self._label_from_table(t) for t in self.allowlist]
        for label in target_labels:
            n = self._embed_missing_for_label(label, limit_per_label=None)  # you can wire batch_size if you want
            out[label] = n
            logger.info("Embeddings: %s -> %d nodes updated", label, n)
        return out
    
    def create_vector_indexes(self, labels: Optional[list[str]] = None) -> Dict[str, str]:
        results = {}
        with self._driver.session(database=self.neo_db) as s:
            target_labels = labels or [self._label_from_table(t) for t in self.allowlist]
            for label in target_labels:
                index_name = f"{label.lower()}_comprehensive_embeddings"
                try:
                    s.run("""
                        CALL db.index.vector.createNodeIndex($name, $label, $prop, $dim, $sim)
                    """, {
                        "name": index_name,
                        "label": label,
                        "prop": "comprehensive_embedding",
                        "dim": self.emb_dim,
                        "sim": "cosine",
                    })
                    results[label] = index_name
                    logger.info("Vector index ensured: %s on %s.comprehensive_embedding", index_name, label)
                except Exception as e:
                    logger.warning("Index create warn for %s: %s", index_name, e)
        return results

    # --------- Helpers

    def _label_from_table(self, table: str) -> str:
        # "tabStudent" -> "Student"
        raw = table[3:] if table.startswith("tab") else table
        alias = self.aliases.get(table, raw)
        return self._safe_label(alias)   # <<< ensure sanitized
    
    def _safe_rel(self, rel: str) -> str:
        # Uppercase, replace non-word characters with underscore, cannot start with digit
        rel = re.sub(r"\W+", "_", rel).strip("_").upper()
        if re.match(r"^\d", rel):
            rel = f"R_{rel}"
        return rel
    
    def _rel_name_from_tables(self, lt: str, rt: str, lk: str, rk: str) -> str:
        # e.g. tabIssue Tracker.school -> tabSchool.name
        left  = (lt[3:] if lt.startswith("tab") else lt)
        right = (rt[3:] if rt.startswith("tab") else rt)
        base  = f"{left}_{lk}_TO_{right}_{rk}"
        return self._safe_rel(base)


    def _prepare_node_props(self, row: Dict[str, Any], doctype: str | None = None) -> Dict[str, Any]:
        if "name" not in row:
            return {}
        props: Dict[str, Any] = {
            "name": row["name"],
            "_doctype": (doctype or "").strip()      
        }
        if "name1" in row and row["name1"]:
            props["display_name"] = str(row["name1"]).strip()
        for k, v in row.items():
            if v in (None, "", "None"):
                continue
            if isinstance(v, (str, int, float, bool)) and k not in ("name", "display_name"):
                props[k] = v if not isinstance(v, str) else v.strip()
        return props

    def _compose_text_from_props(self, props: Dict[str, Any]) -> str:
        """
        Build an embedding text from selected fields. Prefers PREFERRED_TEXT_KEYS,
        then falls back to any string fields.
        """
        pieces: List[str] = []
        for key in PREFERRED_TEXT_KEYS:
            val = props.get(key)
            if isinstance(val, str) and val.strip():
                pieces.append(val.strip())

        if not pieces:
            # fallback: any other string props
            for k, v in props.items():
                if isinstance(v, str) and v.strip() and k not in ("name", "_doctype"):
                    pieces.append(v.strip())

        text = " | ".join(pieces)[: self.max_context_len]
        return text if text else props.get("display_name") or props.get("name") or ""

    def _embed_missing_for_label(self, label: str, limit_per_label: int | None = None) -> int:
        """
        Pull nodes without 'comprehensive_embedding', embed, and write back in batches.
        """
        updated = 0
        with self._driver.session(database=self.neo_db) as s:
            # Collect candidates
            records = s.run(f"""
                MATCH (n:{label})
                WHERE n.comprehensive_embedding IS NULL
                RETURN n.name AS name, n AS node
                LIMIT $lim
            """, {"lim": limit_per_label or 100000})
            rows = list(records)

        if not rows:
            return 0

        for chunk in _chunked(rows, 100):
            texts: List[str] = []
            names: List[str] = []
            for rec in chunk:
                node = rec["node"]
                props = dict(node.items())
                text = self._compose_text_from_props(props)
                names.append(rec["name"])
                texts.append(text)

            try:
                vecs = self._embedder.embed_documents(texts)
            except Exception as e:
                logger.error("Embedding batch failed for %s: %s", label, e)
                continue

            with self._driver.session(database=self.neo_db) as s:
                for name, vec in zip(names, vecs):
                    s.run(f"""
                        MATCH (n:{label} {{name:$name}})
                        SET n.comprehensive_embedding = $vec
                    """, {"name": name, "vec": vec})
                    updated += 1

        return updated

    def close(self):
        try:
            self._driver.close()
        except:
            pass

# -------------------------------
# Bench-friendly wrappers
# -------------------------------

def run_all(clear_db: bool = False) -> dict:
    """
    Full pipeline:
      0) (optional) clear DB
      1) constraints
      2) nodes
      3) relationships
      4) embeddings
      5) vector indexes

      bench execute tap_lms.infra.neo4j_migrator.run_all --kwargs "{'clear_db': True}"

    """
    m = LMSNeo4jMigrator()
    t0 = _now_ms()
    try:
        if clear_db:
            with m._driver.session(database=m.neo_db) as s:
                s.run("MATCH (n) DETACH DELETE n")

        m.create_constraints()
        nodes = m.migrate_nodes()
        rels = m.create_relationships()
        emb = m.build_embeddings_for_all_labels()
        idx = m.create_vector_indexes()

        out = {
            "cleared": clear_db,
            "nodes_migrated": nodes,
            "relationships_created": rels,
            "embeddings_updated": emb,
            "vector_indexes": idx,
            "ms": _now_ms() - t0,
        }
        print(out)
        return out
    finally:
        m.close()


def run_constraints_only() -> dict:
    m = LMSNeo4jMigrator()
    try:
        m.create_constraints()
        return {"ok": True, "step": "constraints"}
    finally:
        m.close()


def run_nodes_only(labels: Optional[list[str]] = None, batch_size: int = 500) -> dict:
    m = LMSNeo4jMigrator()
    try:
        n = m.migrate_nodes(labels=labels, batch_size=batch_size)
        return {"ok": True, "step": "nodes", "labels": labels or "ALL", "migrated": n}
    finally:
        m.close()


def run_relationships_only() -> dict:
    m = LMSNeo4jMigrator()
    try:
        r = m.create_relationships()
        return {"ok": True, "step": "relationships", "created": r}
    finally:
        m.close()


def run_embeddings_only(
    labels: Optional[list[str]] = None, batch_size: int = 500, resume: bool = True
) -> dict:
    m = LMSNeo4jMigrator()
    try:
        e = m.build_embeddings_for_all_labels(labels=labels, batch_size=batch_size, resume=resume)
        return {"ok": True, "step": "embeddings", "labels": labels or "ALL", "updated": e}
    finally:
        m.close()


def run_indexes_only(labels: Optional[list[str]] = None) -> dict:
    m = LMSNeo4jMigrator()
    try:
        idx = m.create_vector_indexes(labels=labels)
        return {"ok": True, "step": "indexes", "labels": labels or "ALL", "created": idx}
    finally:
        m.close()


def check_counts():
    m = LMSNeo4jMigrator()
    try:
        out = {}
        with m._driver.session(database=m.neo_db) as s:
            total_nodes = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            total_rels = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
            out["total_nodes"] = total_nodes
            out["total_relationships"] = total_rels
            # breakdown per label
            labels = set(m._label_from_table(t) for t in m.allowlist)
            for lab in sorted(labels):
                c = s.run(f"MATCH (n:{lab}) RETURN count(n) AS c").single()["c"]
                out[f"{lab}_nodes"] = c
        print(out)
        return out
    finally:
        m.close()

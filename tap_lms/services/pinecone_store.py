# tap_lms/services/pinecone_store.py
from __future__ import annotations

import time
import json
import decimal
from datetime import date, datetime, time as dtime
from typing import Dict, List, Optional, Iterable, Any, Tuple
from tap_lms.services.doctype_selector import pick_doctypes


import frappe
from pinecone import Pinecone

from langchain_openai import OpenAIEmbeddings

from tap_lms.infra.config import get_config
from tap_lms.infra.sql_catalog import load_schema

# --------- helpers ---------

def _pc() -> Pinecone:
    api_key = get_config("pinecone_api_key")
    if not api_key:
        raise RuntimeError("Missing pinecone_api_key in site_config.json")
    return Pinecone(api_key=api_key)

def _index():
    pc = _pc()
    name = get_config("pinecone_index") or "tap-lms-byo"
    return pc.Index(name)

def _emb() -> OpenAIEmbeddings:
    api_key = get_config("openai_api_key")
    model = get_config("embedding_model") or "text-embedding-3-small"  # 1536-d
    if not api_key:
        raise RuntimeError("Missing openai_api_key in site_config.json")
    return OpenAIEmbeddings(model=model, api_key=api_key)

def _to_plain(v: Any) -> Any:
    """Make values Pinecone-metadata safe."""
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime, date, dtime)):
        return v.isoformat()
    # lists / tuples -> list
    if isinstance(v, (list, tuple)):
        return [_to_plain(x) for x in v]
    # dict -> shallow clean
    if isinstance(v, dict):
        return {str(k): _to_plain(v) for k, v in v.items()}
    # fallback to string
    return str(v)


def _record_to_text(doctype: str, row: Dict[str, Any]) -> str:
    """
    Flatten a single record to a readable text block.
    We include important links and numeric/text fields.
    """
    parts = [f"DocType: {doctype}", f"ID: {row.get('name','')}"]
    for k, v in row.items():
        if k == "name":
            continue
        if v is None or v == "":
            continue
        parts.append(f"{k}: {_to_plain(v)}")
    return "\n".join(parts)

# ---- REAL DB COLUMNS ONLY ----
NON_DB_FIELD_TYPES = {
    "Table", "Table MultiSelect", "Attach", "Attach Image",
    "Section Break", "Column Break", "HTML", "Button", "Fold",
    "Image", "Barcode", "Geolocation"
}

def get_db_columns_for_doctype(doctype: str) -> list[str]:
    """Return only actual DB columns for the DocType's table."""
    table = f"tab{doctype}"
    try:
        # Frappe gives you the definitive list from the DB
        cols = frappe.db.get_table_columns(table) or []
        return cols
    except Exception:
        # Fallback via DESCRIBE if needed
        desc = frappe.db.sql(f"DESC `{table}`", as_dict=True)
        return [d["Field"] for d in desc]

def get_safe_select_columns(doctype: str) -> list[str]:
    """
    Intersect real DB columns with meaningful fields based on meta,
    excluding non-DB fieldtypes.
    """
    db_cols = set(get_db_columns_for_doctype(doctype))
    meta = frappe.get_meta(doctype)

    # start with standard system columns (always exist)
    keep = {"name", "modified", "owner", "creation", "docstatus"}

    # then add real fields that map to DB columns and are not non-DB types
    for f in meta.fields:
        if f.fieldtype in NON_DB_FIELD_TYPES:
            continue
        if f.fieldname and f.fieldname in db_cols:
            keep.add(f.fieldname)

    # ensure we don’t accidentally select huge text blobs unless you want them
    # (optional) if you want to skip the very large *_html fields, you can:
    keep = {c for c in keep if not c.endswith("_html")}

    # final order: stable & readable
    ordered = [c for c in [
        "name", "name1", "grade", "school_id", "status", "language",
        "level", "rigour", "city", "district", "state", "country",
        "owner", "creation", "modified", "docstatus"
    ] if c in keep]
    # plus anything else left
    ordered += sorted(list(keep - set(ordered)))
    return ordered


# --------- upsert pipeline ---------

def upsert_doctype(
    doctype: str,
    since: Optional[str] = None,
    group_records: int = 20,
    embed_batch: int = 64,
) -> Dict[str, Any]:
    """
    Incremental BYO upsert for one DocType.
    - Selects ONLY real SQL columns (skips Table/Section Break/etc.)
    - Groups N records into one vector (to avoid "top_k misses long lists")
    - Metadata contains doctype, record_ids, and a small schema summary
    """
    idx = _index()
    emb = _emb()
    schema = load_schema()  # optional enrichment

    ns = doctype  # namespace per doctype
    total_records = 0
    total_vectors = 0

    # --- discover real columns for this parent table ---
    table = f"tab{doctype}"
    try:
        select_cols = frappe.db.get_table_columns(table) or []
    except Exception:
        # Fallback to DESCRIBE if the helper isn't available
        desc = frappe.db.sql(f"DESCRIBE `{table}`", as_dict=True)
        select_cols = [d["Field"] for d in desc]

    # Safety: always include 'name' (primary key) first if present
    if "name" in select_cols:
        select_cols = ["name"] + [c for c in select_cols if c != "name"]

    # Build WHERE for incremental sync
    where_parts = ["docstatus < 2"]
    params: List[Any] = []
    if since:
        where_parts.append("modified >= %s")
        params.append(since)
    where_sql = " AND ".join(where_parts)

    # Paging
    page_size = 1000
    offset = 0

    # Buffers for grouped docs
    buffer_texts: List[str] = []
    buffer_ids: List[str] = []
    buffer_meta: List[Dict[str, Any]] = []

    def _json_safe(v: Any) -> Any:
        # mirror your serializer used elsewhere
        import datetime, decimal
        if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
            return v.isoformat()
        if isinstance(v, decimal.Decimal):
            return float(v)
        return v

    def flush():
        nonlocal total_vectors
        if not buffer_texts:
            return
        vectors: List[Dict[str, Any]] = []

        # embed
        vectors_values = emb.embed_documents(buffer_texts)
        for i, vals in enumerate(vectors_values):
            vectors.append({
                "id": buffer_ids[i],
                "values": vals,
                "metadata": buffer_meta[i],
            })

        # upsert
        idx.upsert(vectors=vectors, namespace=ns)
        total_vectors += len(vectors)

        buffer_texts.clear()
        buffer_ids.clear()
        buffer_meta.clear()

    group: List[Tuple[str, Dict[str, Any]]] = []

    # Iterate pages
    while True:
        cols_csv = ", ".join(f"`{c}`" for c in select_cols)
        rows = frappe.db.sql(
            f"SELECT {cols_csv} FROM `{table}` WHERE {where_sql} "
            f"ORDER BY modified ASC LIMIT {page_size} OFFSET {offset}",
            params,
            as_list=True  # we’ll map to dict manually
        )
        if not rows:
            break

        for tup in rows:
            row = dict(zip(select_cols, tup))
            total_records += 1
            rid = str(row.get("name"))
            text = _record_to_text(doctype, row)  # your existing canonical record->text
            group.append((rid, {"raw": row, "text": text}))

            if len(group) >= group_records:
                combined_text = "\n\n--- END OF RECORD ---\n\n".join([g[1]["text"] for g in group])
                record_ids = [g[0] for g in group]

                buffer_texts.append(combined_text)
                buffer_ids.append(f"{doctype}:{record_ids[0]}:+{len(record_ids)}")
                # metadata: keep it small, JSON-safe
                buffer_meta.append({
                    "doctype": doctype,
                    "record_ids": record_ids,
                    "count": len(record_ids),
                    "schema_keys": [k for k in group[0][1]["raw"].keys()],
                })

                group = []

                if len(buffer_texts) >= embed_batch:
                    # JSON-safe conversion of meta (dates/decimals)
                    for m in buffer_meta:
                        for k, v in list(m.items()):
                            if isinstance(v, list):
                                m[k] = [_json_safe(x) for x in v]
                            else:
                                m[k] = _json_safe(v)
                    flush()

        offset += page_size

    # last partial group
    if group:
        combined_text = "\n\n--- END OF RECORD ---\n\n".join([g[1]["text"] for g in group])
        record_ids = [g[0] for g in group]
        buffer_texts.append(combined_text)
        buffer_ids.append(f"{doctype}:{record_ids[0]}:+{len(record_ids)}")
        buffer_meta.append({
            "doctype": doctype,
            "record_ids": record_ids,
            "count": len(record_ids),
            "schema_keys": [k for k in group[0][1]["raw"].keys()],
        })

    # final JSON-safe pass for metadata and flush
    if buffer_meta:
        for m in buffer_meta:
            for k, v in list(m.items()):
                if isinstance(v, list):
                    m[k] = [_json_safe(x) for x in v]
                else:
                    m[k] = _json_safe(v)
    flush()

    return {
        "doctype": doctype,
        "namespace": ns,
        "records_seen": total_records,
        "vectors_upserted": total_vectors,
        "group_records": group_records,
    }

def upsert_all(
    doctypes: Optional[List[str]] = None,
    since: Optional[str] = None,
    group_records: int = 20,
    embed_batch: int = 64,
) -> Dict[str, Any]:
    """
    Upsert multiple doctypes. If doctypes is None, take a small curated set to start.
    """
    if doctypes is None:
        schema = load_schema()
        doctypes = [t[3:] if t.startswith("tab") else t for t in schema.get("allowlist", [])] # seed list; extend later

    out: Dict[str, Any] = {}
    for dt in doctypes:
        try:
            out[dt] = upsert_doctype(dt, since=since, group_records=group_records, embed_batch=embed_batch)
        except Exception as e:
            out[dt] = {"error": str(e)}
    return out

# --------- search (BYO) ---------

def semantic_search(
    q: str,
    k: int = 10,
    namespace: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Query Pinecone with BYO embeddings.
    """
    idx = _index()
    emb = _emb()

    q_vec = emb.embed_query(q)
    res = idx.query(
        vector=q_vec,
        namespace=namespace,  # None => all namespaces
        top_k=k,
        include_values=False,
        include_metadata=True,
    )
    # normalize response to python dict
    matches = []
    for m in getattr(res, "matches", []):
        matches.append({
            "id": m.id,
            "score": float(m.score),
            "metadata": m.metadata,
            "namespace": namespace,
        })
    return {"q": q, "k": k, "namespace": namespace, "matches": matches}

def list_namespaces() -> list[str]:
    """
    Discover all namespaces that currently exist in the Pinecone index.
    """
    idx = _index()
    # v6 SDK: describe_index_stats returns namespaces map
    stats = idx.describe_index_stats()
    # {'namespaces': {'Student': {'record_count': ...}, 'School': {...}, ...}}
    ns = list((stats or {}).get("namespaces", {}).keys())
    # Always include default if present
    if "__default__" in ns:
        ns.remove("__default__")
        ns = ["__default__"] + ns
    return ns


def _fetch_rows_for_hit(doctype: str, record_ids: list[str], fields: list[str] | None) -> list[dict]:
    """Fetch human-readable rows for a single hit."""
    if not doctype or not record_ids:
        return []
    try:
        return frappe.db.get_all(
            doctype,
            filters={"name": ["in", record_ids]},
            fields=fields or ["name", "name1", "description", "grade", "status"],
        )
    except Exception as e:
        return [{"error": str(e), "doctype": doctype, "record_ids": record_ids}]




def search_auto_namespaces(
    q: str,
    k: int = 8,
    route_top_n: int = 4,
    include_metadata: bool = True,
) -> Dict[str, Any]:
    """
    1) Ask the LLM (schema-aware) which DocTypes matter for the query.
    2) Query Pinecone only in those namespaces (DocType == namespace).
    3) Merge and re-rank by score.
    """
    idx = _index()
    emb = _emb()

    doctypes = pick_doctypes(q, top_n=route_top_n)
    if not doctypes:
        doctypes = ["Student"]  # fallback tiny default

    qvec = emb.embed_query(q)

    matches: List[Dict[str, Any]] = []
    for ns in doctypes:
        try:
            res = idx.query(
                namespace=ns,
                vector=qvec,
                top_k=k,
                include_values=False,
                include_metadata=include_metadata,
            )
            for m in res.get("matches", []):
                m["namespace"] = ns
            matches.extend(res.get("matches", []))
        except Exception as e:
            frappe.log_error(f"Pinecone query failed for ns={ns}: {e}")

    # sort & trim
    matches.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return {
        "q": q,
        "routed_doctypes": doctypes,
        "k": k,
        "matches": matches[:k],
    }


# --- add this helper ---
def _serialize_match(m):
    """
    Normalize Pinecone search hits into JSON-safe dicts.
    Works for both dict and SDK 'ScoredVector' objects.
    """
    # v6 dict-like response
    if isinstance(m, dict):
        return {
            "id": m.get("id"),
            "score": m.get("score"),
            "namespace": m.get("namespace"),  # may be None depending on op
            "metadata": m.get("metadata"),
        }
    # object response
    # hasattr checks keep this safe across SDK variants
    return {
        "id": getattr(m, "id", None),
        "score": getattr(m, "score", None),
        "namespace": getattr(m, "namespace", None),
        "metadata": getattr(m, "metadata", None),
    }



# --------- bench CLIs ---------

def cli_upsert_all(doctypes: Optional[List[str]] = None, since: Optional[str] = None, group_records: int = 20):
    """
    Bench:
      bench execute tap_lms.services.pinecone_store.cli_upsert_all \
        --kwargs "{'doctypes':['Student','School','Activities'],'group_records':20}"
    """
    out = upsert_all(doctypes=doctypes, since=since, group_records=group_records)
    print(frappe.as_json(out))
    return out

def cli_search(q: str, k: int = 10, namespace: Optional[str] = None):
    """
    Bench:
      bench execute tap_lms.services.pinecone_store.cli_search \
        --kwargs "{'q':'recommend activities for 9th graders','k':5,'namespace':'Activities'}"
    """
    out = semantic_search(q=q, k=k, namespace=namespace)
    print(frappe.as_json(out))
    return out


def cli_search_auto(q: str, k: int = 8, route_top_n: int = 4):
    """
    Bench:
      bench execute tap_lms.services.pinecone_store.cli_search_auto \
        --kwargs "{'q':'recommend activities for 9th graders','k':8,'route_top_n':4}"
    """
    t0 = time.time()
    # 1) pick doctypes via the selector
    from tap_lms.services.doctype_selector import pick_doctypes
    candidates = pick_doctypes(q, top_n=route_top_n)

    # 2) run pinecone search across those namespaces
    pc = _pc()
    idx = _index()
    emb = _emb()

    q_vec = emb.embed_query(q)
    all_matches = []
    scanned = []

    for ns in candidates:
        scanned.append(ns)
        res = idx.query(
            namespace=ns,
            vector=q_vec,
            top_k=k,
            include_metadata=True,
            include_values=False,
        )
        # Pinecone responses can be dict or object; normalize safely
        matches = res.get("matches", []) if isinstance(res, dict) else getattr(res, "matches", [])
        for m in matches or []:
            all_matches.append(_serialize_match(m))

    out = {
        "q": q,
        "k": k,
        "namespaces_scanned": scanned,
        "matches": all_matches,
        "execution_time": time.time() - t0,
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return out




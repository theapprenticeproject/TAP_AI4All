# tap_lms/services/pinecone_index.py
from __future__ import annotations

import time
import frappe
from typing import Optional
from pinecone import Pinecone, ServerlessSpec

from tap_lms.infra.config import get_config


def _pc() -> Pinecone:
    api_key = get_config("pinecone_api_key")
    if not api_key:
        raise RuntimeError("Missing pinecone_api_key in site_config.json")
    return Pinecone(api_key=api_key)


def ensure_index(
    index_name: Optional[str] = None,
    dimension: Optional[int] = None,
    cloud: str = "aws",
    region: str = "us-east-1",
    metric: str = "cosine",
) -> dict:
    """
    Create the Pinecone index if it does not exist (BYO embeddings).
    Dimension must match your embedding model (e.g. OpenAI text-embedding-3-small = 1536).
    """
    pc = _pc()
    name = index_name or get_config("pinecone_index") or "tap-lms-byo"
    dim = int(dimension or get_config("embedding_dimension") or 1536)

    if not pc.has_index(name):
        pc.create_index(
            name=name,
            dimension=dim,
            metric=metric,
            spec=ServerlessSpec(cloud=cloud, region=region),
        )

    # simple wait until “Ready”
    while True:
        status = pc.describe_index(name)
        if getattr(status, "status", {}).get("ready"):
            break
        time.sleep(1)

    return {"index": name, "dimension": dim, "metric": metric, "ready": True}


# ---- bench CLI ----
def cli_ensure_index(index_name: Optional[str] = None, dimension: Optional[int] = None):
    """
    Bench:
      bench execute tap_lms.services.pinecone_index.cli_ensure_index \
        --kwargs "{'index_name':'tap-lms-byo','dimension':1536}"
    """
    out = ensure_index(index_name=index_name, dimension=dimension)
    print(frappe.as_json(out))
    return out

# TAP LMS – RAG Microservice Extension

This extension adds a microservice-style API layer on top of TAP LMS (Frappe app) to enable LLM-powered question answering with:

- **Graph RAG (Neo4j)** – preferred when enabled
- **SQL Agent (MariaDB)**
- **Vector RAG (Pinecone)** – universal fallback

A single REST endpoint exposes the router so external systems (e.g., Griffin webhooks) can call it securely.

## 🔀 Routing (current behavior)

- If Neo4j is enabled, route to Graph RAG; on failure, fallback to Pinecone.
- If Neo4j is disabled, route to SQL Agent; on failure, fallback to Pinecone.

Your public API wires into this router and supports GET & POST (Frappe whitelisted), and logs each call with `log_query_event`.

## 📦 Installation

Ensure TAP LMS is installed on your site:

```bash
bench get-app tap_lms <org-repo-url>
bench --site <yoursite> install-app tap_lms
```

Pull this extension code into the same app (adds `infra/`, `services/` and `api/`).

Rebuild & restart:

```bash
bench build
bench restart
```

## ⚙️ Configuration

Put these keys in your site's `site_config.json` (examples):

```json
{
  "openai_api_key": "sk-xxxx",
  "primary_llm_model": "gpt-4o-mini",
  "embedding_model": "text-embedding-3-small",

  "pinecone_api_key": "pcn-xxxx",
  "pinecone_index": "tap-lms-byo",

  "neo4j_uri": "neo4j+s://xxxx.databases.neo4j.io",
  "neo4j_user": "neo4j",
  "neo4j_password": "xxxx",
  "neo4j_database": "neo4j",
  "enable_neo4j": true
}
```

- The router endpoint invokes `route_query(q)` under the hood.
- Run `tap_lms/schema/generate_schema.py` file once to get **TAP LMS** doctypes entire schema in json. 
- Schema is read from `tap_lms_schema.json` through `load_schema()` (used across services).

## 🧭 Required one-time setup (in order)

This is the minimal working sequence to bring all 3 engines online.
Run any step you actually need (e.g., skip Neo4j if disabled).

### 1) Ensure schema JSON exists

Place your curated schema at: `tap_lms/schema/tap_lms_schema.json`.
(It is consumed by multiple modules via `load_schema()`)

### 2) (Optional) Neo4j migration

Populate the graph from your DB based on the allow-list & joins in the schema:

```bash
bench execute tap_lms.infra.neo4j_migrator.run_all --kwargs "{'clear_db': False}"
```

The migrator is schema-driven and builds nodes/relationships accordingly (plus vector fields).

### 3) Pinecone: create/ensure index

```bash
bench execute tap_lms.services.pinecone_index.cli_ensure_index
```

### 3) Pinecone: ingest vectors (BYO embeddings)

All doctypes (recommended):

```bash
bench execute tap_lms.services.pinecone_store.cli_upsert_all --kwargs "{'group_records': 120}"
```

Or single doctype:

```bash
bench execute tap_lms.services.pinecone_store.cli_upsert_doctype --kwargs "{'doctype':'Student','group_records':100}"
```

## 🧪 Sanity checks (CLIs)

### Pinecone search (vector)

```bash
bench execute tap_lms.services.pinecone_store.cli_search_auto --kwargs "{'q':'recommend activities for 9th graders','k':8,'route_top_n':4}"
```

This will embed the query and return normalized matches list.

### Graph RAG (Neo4j)

```bash
bench execute tap_lms.services.graph_rag.cli --kwargs "{'q':'List out distribution of students school wise'}"
```

The graph engine loads schema, selects candidate doctypes, generates/sanitizes Cypher, executes, and returns rows.

### SQL Agent (MariaDB)

```bash
bench execute tap_lms.services.sql_agent.cli --kwargs "{'q':'How many students are in grade 9?'}"
```

The agent is built with the allow-list and produces an answer + (when available) intermediate SQL for visibility.

### Router (end-to-end)

```bash
bench execute tap_lms.services.router.cli --kwargs "{'q':'9th ke students k liye activities suggest karo'}"
```

The router refines the language (Hinglish → English), picks doctypes once, then follows the routing policy described above.

## 🌐 REST API

### Endpoint

```
/api/method/tap_lms.api.query.query
```

### Auth

```
Authorization: token <api_key>:<api_secret>
```

### GET

```bash
curl -G "http://localhost:8000/api/method/tap_lms.api.query.query" \
  -H "Authorization: token <api_key>:<api_secret>" \
  --data-urlencode "q=recommend activities for 9th graders"
```

### POST

```bash
curl -X POST "http://localhost:8000/api/method/tap_lms.api.query.query" \
  -H "Authorization: token <api_key>:<api_secret>" \
  -H "Content-Type: application/json" \
  -d '{"q":"recommend activities for 9th graders"}'
```

This endpoint supports GET & POST and extracts `q` from URL or JSON body.
Each call is rate-limited and logged via `log_query_event`.

## 🔍 What each core file does

- **`tap_lms/api/query.py`** – Frappe REST API (GET/POST) → calls the router; rate limits & logs usage.

- **`tap_lms/services/router.py`** – Orchestrates Graph→Pinecone or SQL→Pinecone fallback based on `enable_neo4j`.

- **`tap_lms/services/sql_agent.py`** – Builds a safe SQL agent using the allow-list in schema; returns answer + candidate SQL (when available).

- **`tap_lms/services/graph_rag.py`** – Graph-only RAG: selects doctypes, prompts LLM for Cypher, sanitizes, runs on Neo4j, returns rows.

- **`tap_lms/services/pinecone_store.py`** – Pinecone utilities: ensure index, upsert all/doctype, and query; uses BYO OpenAI embeddings by default; index name via config.

- **`tap_lms/services/rag_answerer.py`** – Pulls Pinecone matches → loads raw rows → synthesizes final answer with LLM; CLI for quick runs.

- **`tap_lms/infra/neo4j_migrator.py`** – Schema-driven graph migration, constraints/indexes, and optional vector fields; runs via `run_all`.

- **`tap_lms/infra/sql_catalog.py`** – Loads the declarative schema JSON used by SQL Agent / Graph RAG / Router.

- **`tap_lms/infra/ai_logging.py`** – Helper used by the API to log each query (already hooked in `query.py`).

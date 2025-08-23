# TAP LMS – RAG Microservice Extension

This extension adds a **microservice-style API layer** on top of TAP LMS (Frappe app) to enable **LLM-powered question answering** using SQL, Graph, or Hybrid retrieval.

It exposes endpoints via Frappe's `/api/method/...` system, making it easy to integrate with **Griffin webhooks** or other external systems.

---

## 🚀 Features

- **SQL Agent**: Robust question answering over TAP LMS structured data in MariaDB  
- **Graph RAG (Neo4j)**: Relationship and pattern-based queries *(planned)*  
- **Hybrid Mode**: Combine SQL + Graph + Vector embeddings *(planned)*  
- **Microservice API**: Frappe whitelisted endpoints for secure external integration  

---

## 📦 Installation

1. **Ensure you have TAP LMS installed on your site:**
   ```bash
   bench get-app tap_lms <org-repo-url>
   bench --site <yoursite> install-app tap_lms
   ```

2. **Pull this extension code** into the same TAP LMS app (adds `infra/` and `services/` modules).

3. **Run migrations:**
   ```bash
   bench build
   bench restart
   ```

---

## ⚙️ Configuration

We rely on Frappe `site_config.json` for settings. **Example:**

```json
{
  "db_name": "yourdbname",
  "db_password": "yourdbpassword",
  "db_host": "127.0.0.1",
  "db_port": 3306,
  "openai_api_key": "sk-xxxxxx",
  "neo4j_uri": "neo4j+s://xxxx.databases.neo4j.io",
  "neo4j_user": "neo4j",
  "neo4j_password": "yourpassword",
  "neo4j_database": "neo4j",
  "enable_neo4j": true,
  "enable_redis": true
}
```

All values are loaded automatically by `tap_lms.infra.config`.

---

## 🔑 Authentication

**Two options:**

### API Key & Secret (recommended for integration)
- Create a user in Frappe
- Generate API Key and Secret in **User → API Access**
- Example header:
  ```ruby
  Authorization: token <api_key>:<api_secret>
  ```

### Session ID (for logged-in browser sessions)
- Copy `sid` cookie and pass it in requests.

---

## 📡 Usage

### 1. Ping (sanity check)
```bash
curl -s -X GET "http://localhost:8000/api/method/ping"
# {"message":"pong"}
```

### 2. Run SQL Agent
```bash
curl -s -G "http://localhost:8000/api/method/tap_lms.api.query.query" \
  -H "Authorization: token <api_key>:<api_secret>" \
  --data-urlencode "q=how many students in grade 9" \
  --data-urlencode "engine=sql"
```

**Response:**
```json
{
  "question": "how many students in grade 9",
  "answer": "There are 5 students in grade 9.",
  "success": true,
  "engine": "sql",
  "execution_time": 11.0,
  "metadata": {
    "visible_tables": 87
  }
}
```

### 3. Planned endpoints
- `engine=graph` → use Neo4j graph QA
- `engine=hybrid` → combine SQL + graph + vector

---

## 🛠️ Development Notes

- **SQL agent** uses allowlisted tables from `tap_lms/schema/tap_lms_schema.json`
- Run `generate_schema.py` after modifying Doctypes to refresh schema + join rules
- **Graph RAG** uses Neo4j relationships discovered during migration

---

## 📌 Next Steps

- [ ] Add Graph RAG endpoint
- [ ] Add Hybrid router that selects best engine automatically  
- [ ] Define Griffin webhook integration guide

---

## 👥 Contributing

- Update schema if new Doctypes are added
- Extend `services/` with additional RAG engines
- Write tests under `tap_lms/tests/`
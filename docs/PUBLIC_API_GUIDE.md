# Public API Integration Guide

This guide explains how external services can use the Content Atlas public API to explore dataset metadata, run large language model (LLM) powered queries, and stream results back into their own applications. It is intended for teams bootstrapping a new integration and captures the expected request flow, conversation mechanics, and response formats.

---

## Capabilities Overview
- Authenticate with an issued API key using the `X-API-Key` header.
- Discover available reporting tables and their row counts.
- Inspect the column-level schema for a specific table.
- Submit natural language prompts that are translated to SQL by the platform's LLM agent.
- Maintain conversational context across calls by reusing a `thread_id`.
- Retrieve query outputs as CSV data, with support for result sets up to 10 000 rows per request.

All public endpoints share the `/api/v1` prefix (e.g., `https://{host}/api/v1/query`).

---

## Authentication and Safeguards
- Each request must include `X-API-Key: <your_key>`. Keys can be provisioned via the internal admin console or the API key management endpoints.
- Keys are stored hashed. If a key is revoked or expires, requests return `401 Invalid or expired API key`.
- Rate limiting is enforced per key (`rate_limit_per_minute` column). Coordinate with the platform team if your integration requires higher throughput.
- API access is read-only. The query agent blocks destructive SQL (`INSERT`, `UPDATE`, `DELETE`, etc.) and access to protected system tables such as `users`, `api_keys`, `file_imports`, `import_history`, `import_duplicates`, `import_jobs`, `mapping_errors`, `table_metadata`, `uploaded_files`, `query_messages`, and `query_threads`.

---

## Recommended Integration Flow
1. **Acquire an API key** and store it securely (e.g., secret manager). Never embed plain keys in client bundles.
2. **Enumerate tables** using `GET /api/v1/tables` to understand available datasets and row volumes.
3. **Inspect schema** with `GET /api/v1/tables/{table_name}/schema` before issuing analytical prompts.
4. **Run a query** via `POST /api/v1/query`, passing:
   - `prompt`: the user’s natural language question.
   - `thread_id`: a stable identifier (e.g., customer ID) to preserve conversation context.
   - `max_rows`: desired row ceiling (see “Large Result Sets” below).
5. **Parse the response**:
   - `response`: conversational answer produced by the LLM agent.
   - `executed_sql`: SQL text executed on the database (if any).
   - `data_csv`: CSV string containing the result set.
   - `rows_returned`: number of records included in `data_csv`.
6. **Handle follow-up prompts** by reusing the same `thread_id`, allowing the agent to refer to prior conversation state.

---

## Endpoint Reference

### `GET /api/v1/tables`
Lists user-facing tables along with row counts.

**Sample request**
```bash
curl https://{host}/api/v1/tables \
  -H "X-API-Key: $ATLAS_KEY"
```

**Sample response**
```json
{
  "success": true,
  "tables": [
    { "table_name": "ad_performance", "row_count": 28451 },
    { "table_name": "campaigns", "row_count": 1200 }
  ]
}
```

### `GET /api/v1/tables/{table_name}/schema`
Returns column-level metadata so integrations can validate prompts or enforce downstream typing.

**Sample response**
```json
{
  "success": true,
  "table_name": "ad_performance",
  "columns": [
    { "name": "date", "type": "date", "nullable": false },
    { "name": "spend", "type": "numeric", "nullable": false }
  ]
}
```

### `POST /api/v1/query`
Translates a natural language prompt into SQL, executes it, and returns both a conversational summary and CSV data. Requests without a `thread_id` are associated with a default thread.

**Request body**
```json
{
  "prompt": "Show total spend by campaign for Q1 2024 ordered by spend descending.",
  "max_rows": 10000,
  "thread_id": "client-abc"
}
```

**Response body**
```json
{
  "success": true,
  "response": "Here is the Q1 2024 spend by campaign (top 50 shown).",
  "executed_sql": "SELECT ... LIMIT 10000;",
  "data_csv": "campaign,spend\nBrand Awareness,185000.25\n...",
  "execution_time_seconds": 3.42,
  "rows_returned": 284,
  "error": null
}
```

**Conversation behavior**
- The agent automatically injects schema context and remembers previous turns scoped to the supplied `thread_id`.
- If the user prompt clearly asks for specific data (keywords such as “list”, “count”, “top”), the agent forces at least one `SELECT` query; otherwise it may provide strategic guidance without executing SQL.
- Follow-up prompts like “limit to California” or “what was the previous total?” should reuse the same `thread_id` to keep the context.

---

## Handling Large Result Sets (10 000 Row Requirement)

- `max_rows` accepts integers from 1 to 10 000. Integrations should set this value explicitly when requesting large exports.
- The backend agent shapes queries with an explicit `LIMIT` that honors `max_rows`. The `rows_returned` field indicates how many records were actually delivered.
- For exports larger than 10 000 rows, plan for request batching (e.g., iterate by date ranges) or bespoke data pipelines—public API calls intentionally cap at 10 000 rows to protect performance.
- When building UI experiences, consider splitting very large `data_csv` payloads into streaming downloads or background jobs to avoid blocking user interactions.

> **Implementation note:** Ensure the underlying SQL execution layer fetches at least `max_rows` rows. Earlier builds truncated batches at 1 000; confirm your deployment is running the updated configuration before relying on 10 000-row pulls.

---

## Response Fields and Error Handling

- `success` is `true` when the request completed end-to-end; network or unexpected server failures surface as HTTP `500` with a descriptive `detail`.
- `error` contains a human-readable message when something prevented a query from executing (e.g., referencing a protected table). The field can be populated even when `success` is `true`, so check it before processing results.
- Common HTTP statuses:
  - `401`: Missing or invalid API key.
  - `404`: Requested table not found in `/tables/{table_name}/schema`.
  - `500`: Unhandled server-side error; retry with backoff and alert the platform team.
- Log both the `executed_sql` and `thread_id` values for traceability when escalating issues.

---

## Implementation Checklist
- [ ] Request and securely store the API key.
- [ ] Configure an HTTP client that always passes the `X-API-Key` header.
- [ ] Decide how you will generate and persist `thread_id` values per end-user session.
- [ ] Use `/tables` and `/tables/{table}/schema` during bootstrap or as part of your prompt construction workflow.
- [ ] Validate that the deployment supports the 10 000-row cap before exposing large exports to users.
- [ ] Parse `data_csv` using a streaming CSV reader for memory efficiency.
- [ ] Monitor error responses and alert on sustained `500` or `401` trends.

With these practices in place, new projects can consistently leverage the Content Atlas public API to power conversational data exploration and large result exports.

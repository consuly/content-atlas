# API Integration Guide

This guide provides technical details, code examples, and best practices for integrating external applications with the Content Atlas Public API. It explains how to explore dataset metadata, run large language model (LLM) powered queries, and stream results back into your own applications.

## Table of Contents

- [Overview](#overview)
- [Authentication](#authentication)
- [Recommended Integration Flow](#recommended-integration-flow)
- [Endpoint Reference](#endpoint-reference)
- [SDK Examples](#sdk-examples)
  - [Python SDK Example](#python-sdk-example)
  - [Node.js SDK Example](#nodejs-sdk-example)
- [Handling Large Result Sets](#handling-large-result-sets)
- [Monitoring & Health](#monitoring--health)
- [Common Workflows](#common-workflows)
- [Response Fields & Error Handling](#response-fields--error-handling)

---

## Overview

The Content Atlas Public API allows external systems to:
1.  **Discover Data**: List available tables and inspect schemas.
2.  **Query Data**: Execute natural language queries that are automatically translated to SQL.
3.  **Generate SQL**: Convert natural language to SQL for preview or validation.
4.  **Retrieve Results**: Get query outputs as CSV data, with support for result sets up to 10,000 rows per request.

**Base URL**: `https://{host}/api/v1`

---

## Authentication

All public API endpoints require an API Key passed in the `X-API-Key` header. Keys can be provisioned via the internal admin console or the API key management endpoints.

**Example Request:**
```http
GET /api/v1/tables HTTP/1.1
Host: your-domain.com
X-API-Key: your_api_key_here
```

**Safeguards:**
- Keys are stored hashed. If a key is revoked or expires, requests return `401 Invalid or expired API key`.
- Rate limiting is enforced per key (`rate_limit_per_minute` column).
- API access is read-only. The query agent blocks destructive SQL (`INSERT`, `UPDATE`, `DELETE`, etc.) and access to protected system tables.

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

### `POST /api/v1/generate-sql`
Lightweight endpoint that converts natural language to SQL without executing it. Designed for the probe phase of large export workflows or validation.

**Request body**
```json
{
  "prompt": "Get top 10000 clients with email and company",
  "table_hints": ["clients-list"]
}
```

**Response body**
```json
{
  "success": true,
  "sql_query": "SELECT email, company_name FROM \"clients-list\" LIMIT 10000",
  "tables_referenced": ["clients-list"],
  "explanation": "Selecting email and company columns from clients table",
  "error": null
}
```

---

## SDK Examples

### Python SDK Example

Since there is no official PyPI package yet, you can use this reference implementation with the standard `requests` library.

**Prerequisites**: `pip install requests`

```python
import requests
from typing import Dict, Any, List, Optional

class ContentAtlasClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json"
        }

    def list_tables(self) -> List[Dict[str, Any]]:
        """List all available reporting tables."""
        response = requests.get(f"{self.base_url}/tables", headers=self.headers)
        response.raise_for_status()
        return response.json().get("tables", [])

    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """Get column definitions for a specific table."""
        response = requests.get(f"{self.base_url}/tables/{table_name}/schema", headers=self.headers)
        response.raise_for_status()
        return response.json()

    def query(self, prompt: str, thread_id: Optional[str] = None) -> Dict[str, Any]:
        """Run a natural language query."""
        payload = {
            "prompt": prompt,
            "thread_id": thread_id
        }
        response = requests.post(f"{self.base_url}/query", json=payload, headers=self.headers)
        response.raise_for_status()
        return response.json()

# --- Usage Example ---
if __name__ == "__main__":
    client = ContentAtlasClient(
        base_url="https://api.contentatlas.com/api/v1",
        api_key="your_api_key_here"
    )

    # 1. List Tables
    tables = client.list_tables()
    print(f"Found {len(tables)} tables.")

    # 2. Run a Query
    result = client.query("Show me the top 5 campaigns by spend")
    
    print("\nAI Response:", result.get("response"))
    print("\nSQL Executed:", result.get("executed_sql"))
```

### Node.js SDK Example

Reference implementation using `axios`.

**Prerequisites**: `npm install axios`

```javascript
const axios = require('axios');

class ContentAtlasClient {
    constructor(baseUrl, apiKey) {
        this.client = axios.create({
            baseURL: baseUrl,
            headers: {
                'X-API-Key': apiKey,
                'Content-Type': 'application/json'
            }
        });
    }

    async listTables() {
        const response = await this.client.get('/tables');
        return response.data.tables;
    }

    async getSchema(tableName) {
        const response = await this.client.get(`/tables/${tableName}/schema`);
        return response.data;
    }

    async query(prompt, threadId = null) {
        const response = await this.client.post('/query', {
            prompt,
            thread_id: threadId
        });
        return response.data;
    }
}

// --- Usage Example ---
(async () => {
    const client = new ContentAtlasClient(
        'https://api.contentatlas.com/api/v1',
        'your_api_key_here'
    );

    try {
        // 1. List Tables
        const tables = await client.listTables();
        console.log('Tables:', tables.map(t => t.table_name));

        // 2. Run Query
        const result = await client.query('Count active users by region');
        console.log('\nAI Response:', result.response);
        console.log('Rows Returned:', result.rows_returned);
    } catch (error) {
        console.error('Error:', error.response ? error.response.data : error.message);
    }
})();
```

---

## Handling Large Result Sets

- `max_rows` accepts integers from 1 to 10,000. Integrations should set this value explicitly when requesting large exports.
- The backend agent shapes queries with an explicit `LIMIT` that honors `max_rows`. The `rows_returned` field indicates how many records were actually delivered.
- For exports larger than 10,000 rows, plan for request batching or bespoke data pipelines.
- When building UI experiences, consider splitting very large `data_csv` payloads into streaming downloads or background jobs.

---

## Monitoring & Health

Integrate these endpoints into your monitoring system.

### Health Check
**Endpoint**: `GET /health` (Root API, not `/api/v1`)
**Response**: `{"status": "healthy", ...}`

### Rate Limiting
The API enforces rate limits per API key. Headers to monitor:
- `X-RateLimit-Limit`: Total requests allowed per window.
- `X-RateLimit-Remaining`: Requests remaining in current window.
- `X-RateLimit-Reset`: Time until the limit resets.

---

## Common Workflows

### 1. Exploratory Data Analysis
Use the schema endpoint to understand table structures before asking complex questions.
1. `GET /tables` -> Identify target table (e.g., `sales_data`).
2. `GET /tables/sales_data/schema` -> See columns (e.g., `amount`, `date`).
3. `POST /query` -> "Sum amount by date for the last 30 days".

### 2. Dashboard Integration
You can use the `data_csv` field in the query response to populate charts in your own dashboard.
1. Send prompt: "Daily active users for the last week"
2. Parse `data_csv` from JSON response.
3. Render chart using library of choice (Chart.js, D3, etc.).

---

## Response Fields & Error Handling

- `success` is `true` when the request completed end-to-end.
- `error` contains a human-readable message when something prevented a query from executing.
- **Common HTTP statuses**:
  - `401`: Missing or invalid API key.
  - `404`: Requested table not found.
  - `429`: Rate limit exceeded.
  - `500`: Unhandled server-side error.

Log both the `executed_sql` and `thread_id` values for traceability when escalating issues.

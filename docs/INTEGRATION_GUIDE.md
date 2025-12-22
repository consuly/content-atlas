# Integration Guide

This guide provides technical details, code examples, and best practices for integrating external applications with the Content Atlas Public API.

## Table of Contents

- [Overview](#overview)
- [Authentication](#authentication)
- [Python SDK Example](#python-sdk-example)
- [Node.js SDK Example](#nodejs-sdk-example)
- [Monitoring & Health](#monitoring--health)
- [Common Workflows](#common-workflows)

---

## Overview

The Content Atlas Public API allows external systems to:
1.  **Discover Data**: List available tables and inspect schemas.
2.  **Query Data**: Execute natural language queries that are automatically translated to SQL.
3.  **Generate SQL**: Convert natural language to SQL for preview or validation.

**Base URL**: `https://your-domain.com/api/v1`

---

## Authentication

All public API endpoints require an API Key passed in the `X-API-Key` header.

```http
GET /api/v1/tables HTTP/1.1
Host: your-domain.com
X-API-Key: your_api_key_here
```

**obtaining an API Key**:
API keys are generated in the Admin Console under **Settings > API Keys**.

---

## Python SDK Example

Since there is no official PyPI package yet, you can use this reference implementation with the standard `requests` library.

### Prerequisites

```bash
pip install requests
```

### Client Implementation

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
    # print("\nData:", result.get("data_csv"))
```

---

## Node.js SDK Example

Reference implementation using `axios`.

### Prerequisites

```bash
npm install axios
```

### Client Implementation

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

## Monitoring & Health

Integrate these endpoints into your monitoring system (e.g., Datadog, Prometheus, Uptime Robot).

### Health Check

**Endpoint**: `GET /health` (Note: This is on the root API, not `/api/v1`)

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2023-10-27T10:00:00.000000",
  "service": "data-mapper-api"
}
```

**Alerting Rules**:
- Alert if status code is not `200`.
- Alert if response time > 500ms.

### Rate Limiting

The API enforces rate limits per API key. If you exceed the limit, you will receive a `429 Too Many Requests` response.

**Headers to Monitor**:
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

### Note on Webhooks & Ingestion
*   **Webhooks**: Real-time webhook notifications are not currently supported. Please use the `/tasks/{id}` endpoint (internal API) or polling if you are waiting for long-running processes.
*   **Ingestion**: Programmatic data ingestion via API Key is currently in development. Currently, data import is supported via the Web Console or the internal authenticated API.

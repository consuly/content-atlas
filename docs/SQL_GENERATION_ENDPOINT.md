# SQL Generation Endpoint Documentation

## Overview

The SQL Generation Endpoint (`/api/v1/generate-sql`) provides a fast, lightweight way to convert natural language prompts into SQL queries **without executing them**. This endpoint is optimized for the probe phase of large export workflows.

## Why Use the SQL Generation Endpoint?

The natural language query agent (`/api/v1/query`) is powerful but slow for large exports:
- **Multiple LLM calls**: Discovery → Schema analysis → Query execution
- **Execution overhead**: Actually runs the query and returns results
- **Conversation state**: Maintains thread history
- **Typical duration**: 60-120 seconds for complex queries

The SQL generation endpoint is designed for speed:
- **Single LLM call**: Direct prompt → SQL conversion
- **No execution**: Returns SQL only, no database query
- **Stateless**: No conversation memory overhead
- **Typical duration**: 5-15 seconds

## Use Cases

### 1. Probe Phase for Large Exports (Primary Use Case)

For large data exports (>1000 rows), use a two-phase approach:

**Phase 1 (Probe):** Convert NL to SQL using `/api/v1/generate-sql`
```bash
POST /api/v1/generate-sql
{
  "prompt": "Get top 10000 clients with email, name and company"
}

Response (5-15 seconds):
{
  "success": true,
  "sql_query": "SELECT \"email\", \"contact_full_name\", \"company_name\" FROM \"clients-list\" LIMIT 10000",
  "explanation": "..."
}
```

**Phase 2 (Execute):** Use the generated SQL with `/api/export/query`
```bash
POST /api/export/query
{
  "sql_query": "SELECT \"email\", \"contact_full_name\", \"company_name\" FROM \"clients-list\" LIMIT 10000",
  "filename": "clients_export.csv"
}

Response (streaming CSV download)
```

### 2. SQL Preview/Validation

Generate SQL to preview what will be executed before committing:
```bash
POST /api/v1/generate-sql
{
  "prompt": "Show me revenue by advertiser for Q4 2024",
  "table_hints": ["advertisers", "revenue"]
}
```

### 3. Batch SQL Generation

Generate multiple SQL queries efficiently without execution overhead:
```javascript
const prompts = [
  "Top 100 customers by revenue",
  "All orders from last month", 
  "Products with low inventory"
];

for (const prompt of prompts) {
  const response = await fetch('/api/v1/generate-sql', {
    method: 'POST',
    headers: {
      'X-API-Key': apiKey,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ prompt })
  });
  
  const result = await response.json();
  console.log(`SQL for "${prompt}":`, result.sql_query);
}
```

## Endpoint Details

### URL
```
POST /api/v1/generate-sql
```

### Authentication
Requires API key authentication via the `X-API-Key` header.

### Request Body

```json
{
  "prompt": "Natural language description of desired query",
  "table_hints": ["optional", "list", "of", "table", "names"]
}
```

**Parameters:**
- `prompt` (required, string): Natural language description of the query
- `table_hints` (optional, array): List of table names to focus on. When provided, only these tables' schemas are sent to the LLM, which speeds up generation and improves accuracy.

### Response

**Success Response (200 OK):**
```json
{
  "success": true,
  "sql_query": "SELECT \"email\", \"company_name\" FROM \"clients-list\" LIMIT 10000",
  "tables_referenced": ["clients-list"],
  "explanation": "Selecting email and company columns from clients table with 10000 row limit"
}
```

**Error Response (400/500):**
```json
{
  "success": false,
  "error": "Could not generate SQL: ambiguous table reference"
}
```

## Performance Comparison

| Feature | `/api/v1/query` (Agent) | `/api/v1/generate-sql` |
|---------|-------------------------|------------------------|
| LLM Calls | 3-10+ | 1 |
| Database Execution | Yes | No |
| Conversation State | Yes | No |
| Typical Duration | 60-120s | 5-15s |
| Returns Data | Yes | No (SQL only) |
| Use Case | Interactive queries | SQL generation for exports |

## Example Usage

### Using cURL

```bash
curl -X POST https://your-domain.com/api/v1/generate-sql \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key-here" \
  -d '{
    "prompt": "Find all clients from California with active subscriptions",
    "table_hints": ["clients", "subscriptions"]
  }'
```

### Using Python

```python
import requests

url = "https://your-domain.com/api/v1/generate-sql"
headers = {
    "X-API-Key": "your-api-key-here",
    "Content-Type": "application/json"
}
payload = {
    "prompt": "Get top 5000 advertisers by total spend in 2024",
    "table_hints": ["advertisers", "campaigns"]
}

response = requests.post(url, json=payload, headers=headers)
result = response.json()

if result["success"]:
    print(f"Generated SQL:\n{result['sql_query']}")
    print(f"\nExplanation: {result['explanation']}")
    print(f"Tables used: {', '.join(result['tables_referenced'])}")
else:
    print(f"Error: {result['error']}")
```

### Using JavaScript/TypeScript

```typescript
async function generateSQL(prompt: string, tableHints?: string[]) {
  const response = await fetch('https://your-domain.com/api/v1/generate-sql', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': 'your-api-key-here'
    },
    body: JSON.stringify({
      prompt,
      table_hints: tableHints
    })
  });

  const result = await response.json();
  
  if (result.success) {
    console.log('Generated SQL:', result.sql_query);
    console.log('Tables:', result.tables_referenced);
    return result.sql_query;
  } else {
    throw new Error(result.error);
  }
}

// Usage
const sql = await generateSQL(
  'Get all customers who placed orders in the last 30 days',
  ['customers', 'orders']
);
```

## Complete Workflow: NL Prompt → Large Export

Here's a complete example showing the recommended two-phase workflow:

```typescript
async function exportLargeData(prompt: string, filename: string) {
  // Phase 1: Generate SQL (fast, 5-15s)
  console.log('Generating SQL from prompt...');
  const generateResponse = await fetch('/api/v1/generate-sql', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY
    },
    body: JSON.stringify({ prompt })
  });
  
  const generated = await generateResponse.json();
  
  if (!generated.success) {
    throw new Error(`SQL generation failed: ${generated.error}`);
  }
  
  console.log('SQL generated:', generated.sql_query);
  
  // Phase 2: Execute export with generated SQL (up to 120s, streams response)
  console.log('Executing export...');
  const exportResponse = await fetch('/api/export/query', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY
    },
    body: JSON.stringify({
      sql_query: generated.sql_query,
      filename: filename
    })
  });
  
  if (exportResponse.ok) {
    const blob = await exportResponse.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    
    console.log('Export completed successfully');
    console.log('Rows:', exportResponse.headers.get('X-Row-Count'));
    console.log('Time:', exportResponse.headers.get('X-Execution-Time'));
  } else {
    throw new Error(`Export failed: ${await exportResponse.text()}`);
  }
}

// Usage
await exportLargeData(
  'Get top 10000 clients with email, company, and contact info',
  'clients_export.csv'
);
```

## Security

The SQL generation endpoint enforces the same security rules as other query endpoints:

- **Only SELECT queries** - No INSERT, UPDATE, DELETE, DROP, etc.
- **No system table access** - Protected tables (users, api_keys, etc.) are blocked
- **SQL validation** - Generated SQL is validated before being returned
- **No dangerous patterns** - Comments, multiple statements, etc. are blocked

## Optimization Tips

### 1. Use Table Hints for Better Performance

When you know which tables are relevant, provide them via `table_hints`:

```json
{
  "prompt": "Show revenue by campaign",
  "table_hints": ["campaigns", "revenue"]
}
```

This reduces the schema context sent to the LLM, making generation faster and more accurate.

### 2. Be Specific in Prompts

More specific prompts generate better SQL:

❌ **Vague:** "Get some clients"
✅ **Specific:** "Get top 1000 clients with email, company name, and phone number"

### 3. Handle Errors Gracefully

Always check the `success` field and handle errors:

```javascript
const result = await generateSQL(prompt);

if (!result.success) {
  // Retry with more specific prompt or table hints
  console.error('Generation failed:', result.error);
  
  // Example: retry with table hints
  const retryResult = await generateSQL(prompt, ['known-table-name']);
}
```

## Troubleshooting

### "Could not extract SQL from LLM response"

The LLM didn't return SQL in the expected format. Try:
- Making your prompt more specific
- Adding table hints
- Simplifying the request

### "Cannot access protected system table: users"

The generated SQL tried to query a system table. The system blocks this automatically.

### "Generated query is not a SELECT statement"

The LLM generated a non-SELECT query (INSERT/UPDATE/etc.). This is blocked for security.
- Rephrase your prompt to be read-only
- Ensure you're asking for data retrieval, not modification

### Slow Generation (>30s)

If generation is taking longer than expected:
- Add `table_hints` to narrow the schema context
- Check your database has the Anthropic API key configured
- Monitor your Anthropic API rate limits

## Related Documentation

- [Export Endpoint](./EXPORT_ENDPOINT.md) - For executing SQL and downloading large CSV exports
- [Public API Guide](./PUBLIC_API_GUIDE.md) - API authentication and usage
- [Query Agent](./CONSOLE.md) - Full natural language query interface with execution

## Configuration

No additional configuration needed beyond the standard API setup. The endpoint uses:
- `ANTHROPIC_API_KEY` - For LLM access (same as other query endpoints)
- Database connection settings (for schema introspection only, no query execution)

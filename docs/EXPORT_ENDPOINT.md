# Export Endpoint Documentation

## Overview

The Export Endpoint (`/api/export/query`) allows direct SQL execution for exporting large datasets as CSV files, bypassing the LLM agent for maximum performance and higher row limits.

## Why Use the Export Endpoint?

The natural language query agent has limits designed for interactive use:
- **Row Limit**: 2,500 rows (configurable via `QUERY_ROW_LIMIT`)
- **Timeout**: 60 seconds (configurable via `QUERY_TIMEOUT_SECONDS`)
- **Purpose**: Quick data exploration and analysis

The export endpoint is designed for large data exports:
- **Row Limit**: 100,000 rows (configurable via `EXPORT_ROW_LIMIT`)
- **Timeout**: 120 seconds (configurable via `EXPORT_TIMEOUT_SECONDS`)
- **Purpose**: Bulk data extraction and reporting

## Endpoint Details

### URL
```
POST /api/export/query
```

### Authentication
Requires API key authentication via the `X-API-Key` header.

### Request Body

```json
{
  "sql_query": "SELECT * FROM customers LIMIT 50000",
  "filename": "customers_export.csv"
}
```

**Parameters:**
- `sql_query` (required): SQL SELECT query to execute
- `filename` (optional): Name of the downloaded CSV file (defaults to "export.csv")

### Response

Returns a streaming CSV file download with the following headers:
- `Content-Type`: `text/csv`
- `Content-Disposition`: `attachment; filename="your_filename.csv"`
- `X-Row-Count`: Number of rows returned
- `X-Execution-Time`: Query execution time in seconds

### Security

The endpoint enforces the same security rules as the agent:
- **Only SELECT queries** - No INSERT, UPDATE, DELETE, DROP, etc.
- **No system table access** - Protected tables like `users`, `api_keys`, etc. are blocked
- **SQL injection prevention** - Dangerous patterns are blocked
- **Query timeout** - Configurable timeout prevents runaway queries

### Example Usage

#### Using cURL

```bash
curl -X POST http://localhost:8000/api/export/query \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key-here" \
  -d '{
    "sql_query": "(SELECT contact_full_name, email, company_name FROM \"clients-list\" LIMIT 25000) UNION ALL (SELECT contact_full_name, email, company_name FROM \"competitors-list\" LIMIT 25000)",
    "filename": "combined_contacts.csv"
  }' \
  --output combined_contacts.csv
```

#### Using Python

```python
import requests

url = "http://localhost:8000/api/export/query"
headers = {
    "X-API-Key": "your-api-key-here",
    "Content-Type": "application/json"
}
payload = {
    "sql_query": "SELECT * FROM customers WHERE country = 'USA' LIMIT 50000",
    "filename": "usa_customers.csv"
}

response = requests.post(url, json=payload, headers=headers)

if response.status_code == 200:
    with open("usa_customers.csv", "wb") as f:
        f.write(response.content)
    print(f"Downloaded {response.headers.get('X-Row-Count')} rows")
    print(f"Execution time: {response.headers.get('X-Execution-Time')}")
else:
    print(f"Error: {response.status_code} - {response.text}")
```

#### Using JavaScript/TypeScript

```typescript
const exportQuery = async (sqlQuery: string, filename: string) => {
  const response = await fetch('http://localhost:8000/api/export/query', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': 'your-api-key-here'
    },
    body: JSON.stringify({
      sql_query: sqlQuery,
      filename: filename
    })
  });

  if (response.ok) {
    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    
    console.log(`Rows: ${response.headers.get('X-Row-Count')}`);
    console.log(`Time: ${response.headers.get('X-Execution-Time')}`);
  } else {
    console.error('Export failed:', await response.text());
  }
};

// Usage
exportQuery(
  'SELECT * FROM orders WHERE year = 2024 LIMIT 75000',
  'orders_2024.csv'
);
```

## Configuration

Configure the export endpoint behavior via environment variables:

```bash
# Maximum rows returned per export
EXPORT_ROW_LIMIT=100000

# Query timeout in seconds
EXPORT_TIMEOUT_SECONDS=120
```

## Use Cases

### 1. Combining Multiple Tables with UNION

Export 50,000 records (25K from each table):

```sql
(SELECT contact_full_name, first_name, last_name, email, title, company_name 
 FROM "clients-list" 
 WHERE email IS NOT NULL 
 LIMIT 25000)
UNION ALL
(SELECT contact_full_name, first_name, last_name, email, title, company_name 
 FROM "competitors-list" 
 WHERE email IS NOT NULL 
 LIMIT 25000)
```

### 2. Filtered Large Dataset Export

Export all B2B prospects from multiple industries:

```sql
SELECT contact_full_name, email, company_name, company_industry, title, seniority
FROM "clients-list"
WHERE company_industry IN ('Marketing', 'Advertising', 'Software', 'Technology')
  AND email IS NOT NULL
LIMIT 80000
```

### 3. Aggregated Reports

Export summary data:

```sql
SELECT company_name, 
       COUNT(*) as contact_count,
       STRING_AGG(DISTINCT department, ', ') as departments
FROM "clients-list"
GROUP BY company_name
HAVING COUNT(*) > 5
ORDER BY contact_count DESC
LIMIT 10000
```

## Health Check

Check the export service status:

```bash
GET /api/export/health
```

Response:
```json
{
  "status": "healthy",
  "service": "export",
  "max_rows": 100000,
  "timeout_seconds": 120
}
```

## Error Handling

The endpoint returns HTTP error codes for common issues:

- **400 Bad Request**: Invalid SQL query (non-SELECT, system table access, dangerous operations)
- **404 Not Found**: Query executed successfully but returned no results
- **500 Internal Server Error**: Database connection or execution error

Example error response:
```json
{
  "detail": "Access to system table 'users' is not allowed."
}
```

## Best Practices

1. **Use LIMIT clauses** to avoid hitting the row limit unexpectedly
2. **Index columns** used in WHERE clauses for better performance
3. **Test queries** with the agent endpoint first (smaller limits) before exporting
4. **Use UNION ALL** instead of UNION when you don't need duplicate removal (faster)
5. **Break very large exports** into multiple requests if needed
6. **Monitor timeout settings** and adjust based on query complexity

## Differences from Agent Endpoint

| Feature | Agent Endpoint | Export Endpoint |
|---------|----------------|-----------------|
| Row Limit | 2,500 | 100,000 |
| Timeout | 60s | 120s |
| Natural Language | ✅ Yes | ❌ No (SQL only) |
| LLM Processing | ✅ Yes | ❌ No |
| Response Format | JSON + CSV | CSV download |
| Use Case | Interactive queries | Bulk exports |
| Chart Suggestions | ✅ Yes | ❌ No |

## Troubleshooting

### Query Timeout

If your query times out, try:
- Add more specific WHERE conditions to reduce data volume
- Add appropriate indexes to speed up filtering
- Increase `EXPORT_TIMEOUT_SECONDS` in environment variables

### Hit Row Limit

If you need more than 100,000 rows:
- Increase `EXPORT_ROW_LIMIT` in environment variables
- Break the export into multiple requests with pagination (OFFSET/LIMIT)
- Export filtered subsets and combine locally

### Memory Issues

For very large result sets:
- The endpoint uses streaming to minimize memory usage
- Consider splitting into smaller exports
- Ensure your database has adequate resources

## Related Documentation

- [API Reference](./API_REFERENCE.md) - Full API documentation
- [Public API Guide](./PUBLIC_API_GUIDE.md) - API authentication and usage
- [Query Agent](./CONSOLE.md) - Natural language query interface

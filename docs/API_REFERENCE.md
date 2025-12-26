# API Reference

Complete reference for all ContentAtlas API endpoints.

## Table of Contents

- [Data Mapping Endpoints](#data-mapping-endpoints)
  - [POST /map-data](#post-map-data)
  - [POST /map-b2-data](#post-map-b2-data)
  - [POST /map-b2-data-async](#post-map-b2-data-async)
- [Data Analysis Endpoints](#data-analysis-endpoints)
  - [POST /extract-b2-excel-csv](#post-extract-b2-excel-csv)
  - [POST /detect-b2-mapping](#post-detect-b2-mapping)
- [Natural Language Query Endpoints](#natural-language-query-endpoints)
  - [POST /query-database](#post-query-database)
- [Export Endpoints](#export-endpoints)
  - [POST /api/export/query](#post-apiexportquery)
- [SQL Generation Endpoints](#sql-generation-endpoints)
  - [POST /api/v1/generate-sql](#post-apiv1generate-sql)
- [Table Management Endpoints](#table-management-endpoints)
  - [GET /tables](#get-tables)
  - [GET /tables/{table_name}](#get-tablestable_name)
  - [GET /tables/{table_name}/schema](#get-tablestable_nameschema)
  - [GET /tables/{table_name}/stats](#get-tablestable_namestats)
- [Task Management Endpoints](#task-management-endpoints)
  - [GET /tasks/{task_id}](#get-taskstask_id)

---

## Data Mapping Endpoints

### POST /map-data

Upload a file and mapping configuration to map data to the database.

**Parameters:**
- `file`: The data file (CSV, Excel, JSON, or XML)
- `mapping_json`: JSON string containing the mapping configuration

**Mapping JSON Format:**
```json
{
  "table_name": "customers",
  "db_schema": {
    "id": "INTEGER",
    "name": "VARCHAR(255)",
    "email": "VARCHAR(255)"
  },
  "mappings": {
    "name": "customer_name",
    "email": "contact_email"
  },
  "rules": {
    "transformations": [
      {"type": "uppercase", "field": "name"}
    ]
  }
}
```

**Response:**
```json
{
  "success": true,
  "message": "Data mapped and inserted successfully",
  "records_processed": 10,
  "table_name": "customers"
}
```

**Error Responses:**
- `400 Bad Request`: Invalid file format or mapping configuration
- `409 Conflict`: Duplicate data detected (see [Duplicate Detection](DUPLICATE_DETECTION.md))
- `500 Internal Server Error`: Database or processing error

---

### POST /map-b2-data

Download a file from Backblaze B2 and map data to the database.

**Request Body:**
```json
{
  "file_name": "data/customers.csv",
  "mapping": {
    "table_name": "customers",
    "db_schema": {
      "id": "INTEGER",
      "name": "VARCHAR(255)",
      "email": "VARCHAR(255)"
    },
    "mappings": {
      "name": "customer_name",
      "email": "contact_email"
    },
    "rules": {
      "transformations": [
        {"type": "uppercase", "field": "name"}
      ]
    }
  }
}
```

**Environment Variables Required:**
- `STORAGE_ACCESS_KEY_ID`: Your storage access key ID (B2 Application Key ID, AWS Access Key, etc.)
- `STORAGE_SECRET_ACCESS_KEY`: Your storage secret access key (B2 Application Key, AWS Secret Key, etc.)
- `STORAGE_BUCKET_NAME`: The name of your storage bucket
- `STORAGE_ENDPOINT_URL`: The storage endpoint URL
- `STORAGE_PROVIDER`: The storage provider type (e.g., "b2", "s3", "minio")

**Response:**
```json
{
  "success": true,
  "message": "B2 data mapped and inserted successfully",
  "records_processed": 10,
  "table_name": "customers"
}
```

**Notes:**
- Files larger than 50MB are automatically processed using chunked processing
- See [Scalability and Performance](SCALABILITY_AND_PERFORMANCE.md) for details on large file handling

---

### POST /map-b2-data-async

Start asynchronous processing of large files from Backblaze B2.

**Request Body:**
```json
{
  "file_name": "data/large_file.xlsx",
  "mapping": {
    "table_name": "large_dataset",
    "db_schema": {
      "id": "INTEGER",
      "name": "VARCHAR(255)",
      "value": "DECIMAL"
    },
    "mappings": {
      "name": "Name",
      "value": "Value"
    }
  }
}
```

**Response:**
```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Task queued for processing"
}
```

**Use Cases:**
- Files that take longer than 30 seconds to process
- Very large datasets (>100,000 records)
- When you need to track processing progress

**See Also:**
- [GET /tasks/{task_id}](#get-taskstask_id) to check task status

---

## Data Analysis Endpoints

### POST /extract-b2-excel-csv

Extract top N rows from each sheet in an Excel file from Backblaze B2 and return as CSV strings.

**Request Body:**
```json
{
  "file_name": "data/workbook.xlsx",
  "rows": 100
}
```

**Parameters:**
- `file_name`: The name/key of the Excel file in the B2 bucket
- `rows`: Number of rows to extract from each sheet (optional, defaults to 100)

**Environment Variables Required:**
- `B2_APPLICATION_KEY_ID`: Your B2 application key ID
- `B2_APPLICATION_KEY`: Your B2 application key
- `B2_BUCKET_NAME`: The name of your B2 bucket

**Response:**
```json
{
  "success": true,
  "sheets": {
    "Sheet1": "col1,col2\nrow1val1,row1val2\nrow2val1,row2val2\n...",
    "Sheet2": "col1,col2\nrow1val1,row1val2\nrow2val1,row2val2\n..."
  }
}
```

**Notes:**
- Extracts the specified number of rows from each sheet in the Excel file
- Returns CSV formatted strings for each sheet
- If a sheet has fewer rows than requested, returns all available rows
- Supports both .xlsx and .xls file formats

**Use Cases:**
- Preview Excel file contents before full import
- Analyze data structure and format
- Generate sample data for testing mappings

---

### POST /detect-b2-mapping

Analyze a CSV or Excel file from Backblaze B2 and return the auto-detected mapping configuration.

**Request Body:**
```json
{
  "file_name": "data/customers.csv"
}
```

**Parameters:**
- `file_name`: The name/key of the CSV or Excel file in the B2 bucket

**Response:**
```json
{
  "success": true,
  "file_type": "csv",
  "detected_mapping": {
    "table_name": "customers",
    "db_schema": {
      "id": "INTEGER",
      "name": "VARCHAR(255)",
      "email": "VARCHAR(255)"
    },
    "mappings": {
      "id": "Customer ID",
      "name": "Customer Name",
      "email": "Email Address"
    },
    "rules": {}
  },
  "columns_found": ["Customer ID", "Customer Name", "Email Address"],
  "rows_sampled": 100
}
```

**Notes:**
- Only supports CSV and Excel files (.csv, .xlsx, .xls)
- Automatically detects column data types:
  - `INTEGER`: Whole numbers
  - `DECIMAL`: Numbers with decimal points
  - `TIMESTAMP`: Date/time values
  - `VARCHAR(255)`: Text values
- Generates SQL-safe table and column names
- Table name is derived from the filename
- Returns the complete mapping configuration that can be used with `/map-b2-data`

**Use Cases:**
- Quick setup for new data sources
- Validate data structure before import
- Generate initial mapping configuration for manual refinement

---

## Natural Language Query Endpoints

### POST /query-database

Execute natural language queries against the database using AI-powered SQL generation with conversation memory.

**Request Body:**
```json
{
  "prompt": "Show me all customers from California",
  "max_rows": 1000,
  "thread_id": "user-session-123"
}
```

**Parameters:**
- `prompt` (required): Natural language query to execute
- `max_rows` (optional): Maximum number of rows to return (default: 1000, max: 10000)
- `thread_id` (optional): Conversation thread ID for memory continuity

**Response:**
```json
{
  "success": true,
  "response": "I found 45 customers from California. Here are the results:",
  "executed_sql": "SELECT * FROM customers WHERE state = 'California' LIMIT 1000;",
  "data_csv": "id,name,email,state\n1,John Doe,john@example.com,California\n...",
  "execution_time_seconds": 0.15,
  "rows_returned": 45,
  "chart_suggestion": {
    "should_display": true,
    "reason": "Detected a categorical breakdown with numeric values.",
    "spec": {
      "type": "bar",
      "labels": ["California", "Nevada", "Oregon"],
      "datasets": [
        { "label": "customers", "data": [45, 22, 12] }
      ]
    }
  },
  "error": null
}
```

---

## Export Endpoints

### POST /api/export/query

The Export Endpoint allows direct SQL execution for exporting large datasets as CSV files, bypassing the LLM agent for maximum performance and higher row limits.

**Request Body:**
```json
{
  "sql_query": "SELECT * FROM customers LIMIT 50000",
  "filename": "customers_export.csv"
}
```

**Parameters:**
- `sql_query` (required): SQL SELECT query to execute
- `filename` (optional): Name of the downloaded CSV file (defaults to "export.csv")

**Response:**
Returns a streaming CSV file download.

**Limits:**
- Row Limit: 100,000 rows (configurable via `EXPORT_ROW_LIMIT`)
- Timeout: 120 seconds (configurable via `EXPORT_TIMEOUT_SECONDS`)

**Security:**
- Only SELECT queries allowed
- No system table access
- SQL injection prevention

---

## SQL Generation Endpoints

### POST /api/v1/generate-sql

Provides a fast, lightweight way to convert natural language prompts into SQL queries **without executing them**. Optimized for the probe phase of large export workflows.

**Request Body:**
```json
{
  "prompt": "Get top 10000 clients with email, name and company",
  "table_hints": ["clients-list"]
}
```

**Parameters:**
- `prompt` (required): Natural language description of the query
- `table_hints` (optional): List of table names to focus on to speed up generation.

**Response:**
```json
{
  "success": true,
  "sql_query": "SELECT \"email\", \"contact_full_name\", \"company_name\" FROM \"clients-list\" LIMIT 10000",
  "tables_referenced": ["clients-list"],
  "explanation": "Selecting email and company columns from clients table with 10000 row limit"
}
```

**Use Case:**
Use this endpoint to generate SQL, then pass the SQL to `/api/export/query` for large data exports.

---

## Table Management Endpoints

### GET /tables

List all dynamically created tables in the database.

**Response:**
```json
{
  "success": true,
  "tables": [
    {
      "table_name": "customers",
      "row_count": 1500
    },
    {
      "table_name": "products",
      "row_count": 250
    }
  ]
}
```

---

### GET /tables/{table_name}

Query data from a specific table with pagination.

**Parameters:**
- `limit`: Number of records to return (default: 100, max: 1000)
- `offset`: Number of records to skip (default: 0)

**Response:**
```json
{
  "success": true,
  "table_name": "customers",
  "data": [
    {"id": 1, "name": "John Doe", "email": "john@example.com"}
  ],
  "total_rows": 1500,
  "limit": 50,
  "offset": 100
}
```

---

### GET /tables/{table_name}/schema

Get the column schema and metadata for a table.

**Response:**
```json
{
  "success": true,
  "table_name": "customers",
  "columns": [
    {"name": "id", "type": "integer", "nullable": false},
    {"name": "name", "type": "character varying", "nullable": true}
  ]
}
```

---

### GET /tables/{table_name}/stats

Get basic statistics for a table.

**Response:**
```json
{
  "success": true,
  "table_name": "customers",
  "total_rows": 1500,
  "columns_count": 3,
  "data_types": {
    "name": "character varying",
    "created_at": "timestamp without time zone"
  }
}
```

---

## Task Management Endpoints

### GET /tasks/{task_id}

Check the status of an async processing task.

**Response:**
```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "progress": 60,
  "message": "Mapping data...",
  "result": null
}
```

**Status Values:**
- `pending`: Task is queued but not yet started
- `processing`: Task is currently being processed
- `completed`: Task completed successfully
- `failed`: Task failed with an error

---

## Common Response Codes

- `200 OK`: Request successful
- `400 Bad Request`: Invalid request parameters or body
- `404 Not Found`: Resource not found (table, task, etc.)
- `409 Conflict`: Duplicate data detected
- `500 Internal Server Error`: Server-side error

## Related Documentation

- [Duplicate Detection](DUPLICATE_DETECTION.md) - Understanding duplicate detection
- [Scalability and Performance](SCALABILITY_AND_PERFORMANCE.md) - Large file processing
- [Setup Guide](SETUP.md) - Environment configuration
- [Testing Guide](TESTING.md) - Testing API endpoints

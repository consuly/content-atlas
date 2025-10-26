# API Reference

Complete reference for all Data Mapper API endpoints.

## Table of Contents

- [Data Mapping Endpoints](#data-mapping-endpoints)
  - [POST /map-data](#post-map-data)
  - [POST /map-b2-data](#post-map-b2-data)
  - [POST /map-b2-data-async](#post-map-b2-data-async)
- [Data Analysis Endpoints](#data-analysis-endpoints)
  - [POST /extract-b2-excel-csv](#post-extract-b2-excel-csv)
  - [POST /detect-b2-mapping](#post-detect-b2-mapping)
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
- `B2_APPLICATION_KEY_ID`: Your B2 application key ID
- `B2_APPLICATION_KEY`: Your B2 application key
- `B2_BUCKET_NAME`: The name of your B2 bucket

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
- See [Parallel Processing](PARALLEL_PROCESSING.md) for details on large file handling

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

**Use Cases:**
- Display available tables in a frontend UI
- Monitor database contents
- Verify successful data imports

---

### GET /tables/{table_name}

Query data from a specific table with pagination.

**Parameters:**
- `limit`: Number of records to return (default: 100, max: 1000)
- `offset`: Number of records to skip (default: 0)

**Example Request:**
```
GET /tables/customers?limit=50&offset=100
```

**Response:**
```json
{
  "success": true,
  "table_name": "customers",
  "data": [
    {"id": 1, "name": "John Doe", "email": "john@example.com"},
    {"id": 2, "name": "Jane Smith", "email": "jane@example.com"}
  ],
  "total_rows": 1500,
  "limit": 50,
  "offset": 100
}
```

**Use Cases:**
- Display table data in a frontend grid/table
- Export data for analysis
- Verify data import results

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
    {"name": "name", "type": "character varying", "nullable": true},
    {"name": "email", "type": "character varying", "nullable": true}
  ]
}
```

**Use Cases:**
- Display table structure in a frontend UI
- Validate data types before import
- Generate documentation

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
    "email": "character varying",
    "created_at": "timestamp without time zone"
  }
}
```

**Use Cases:**
- Display table overview in a dashboard
- Monitor data growth
- Validate import completeness

---

## Task Management Endpoints

### GET /tasks/{task_id}

Check the status of an async processing task.

**Response (In Progress):**
```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "progress": 60,
  "message": "Mapping data...",
  "result": null
}
```

**Response (Completed):**
```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "progress": 100,
  "message": "Processing completed successfully",
  "result": {
    "success": true,
    "message": "B2 data mapped and inserted successfully",
    "records_processed": 50000,
    "table_name": "large_dataset"
  }
}
```

**Response (Failed):**
```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "failed",
  "progress": 45,
  "message": "Error processing file: Invalid data format",
  "result": null
}
```

**Status Values:**
- `pending`: Task is queued but not yet started
- `processing`: Task is currently being processed
- `completed`: Task completed successfully
- `failed`: Task failed with an error

**Use Cases:**
- Poll for task completion in a frontend UI
- Display progress bars for long-running imports
- Handle errors gracefully

---

## Common Response Codes

- `200 OK`: Request successful
- `400 Bad Request`: Invalid request parameters or body
- `404 Not Found`: Resource not found (table, task, etc.)
- `409 Conflict`: Duplicate data detected
- `500 Internal Server Error`: Server-side error

## Related Documentation

- [Duplicate Detection](DUPLICATE_DETECTION.md) - Understanding duplicate detection and configuration
- [Parallel Processing](PARALLEL_PROCESSING.md) - How large files are processed efficiently
- [Setup Guide](SETUP.md) - Environment configuration and setup
- [Testing Guide](TESTING.md) - Testing API endpoints

# Data Mapper API

A Python FastAPI application that accepts documents (XML, JSON, CSV, Excel) with mapping configurations and maps the data to a PostgreSQL database according to the specified schema.

## Features

- Support for CSV, Excel, JSON, and XML file formats
- Dynamic database table creation based on schema
- Flexible data mapping with transformation rules
- RESTful API with automatic documentation

## Setup

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Start PostgreSQL: `docker-compose up -d db`
4. Run the application: `uvicorn app.main:app --reload`

**Note:** Ensure PostgreSQL is running before starting the application. The API will fail to start if it cannot connect to the database.

## API Usage

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
    "Sheet1": "col1,col2,row1val1,row1val2\nrow2val1,row2val2\n...",
    "Sheet2": "col1,col2,row1val1,row1val2\nrow2val1,row2val2\n..."
  }
}
```

**Notes:**
- Extracts the specified number of rows from each sheet in the Excel file
- Returns CSV formatted strings for each sheet
- If a sheet has fewer rows than requested, returns all available rows
- Supports both .xlsx and .xls file formats

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
- Automatically detects column data types (INTEGER, DECIMAL, TIMESTAMP, VARCHAR)
- Generates SQL-safe table and column names
- Table name is derived from the filename
- Returns the complete mapping configuration that can be used with `/map-b2-data`

## Frontend Integration Endpoints

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
    {"id": 1, "name": "John Doe", "email": "john@example.com"},
    {"id": 2, "name": "Jane Smith", "email": "jane@example.com"}
  ],
  "total_rows": 1500,
  "limit": 100,
  "offset": 0
}
```

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

## Async Processing

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

**Completed Response:**
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

## Complete Testing Strategy

### Environment Setup

1. **Start the complete stack:**
```bash
docker-compose up -d
```

2. **Verify services are running:**
```bash
docker-compose ps
```

### Data Processing Pipeline Testing

1. **Test schema detection:**
```bash
POST /detect-b2-mapping
{
  "file_name": "test_data/sample.csv"
}
```

2. **Test data preview:**
```bash
POST /extract-b2-excel-csv
{
  "file_name": "test_data/sample.xlsx",
  "rows": 100
}
```

3. **Test full processing:**
```bash
POST /map-b2-data
{
  "file_name": "test_data/sample.xlsx",
  "mapping": {...}
}
```

### Frontend Integration Testing

4. **Test table listing:**
```bash
GET /tables
```

5. **Test data queries:**
```bash
GET /tables/customers?limit=1000
GET /tables/customers/schema
GET /tables/customers/stats
```

### Large File Testing

6. **Test chunked processing (>50MB files):**
```bash
POST /map-b2-data
# Upload a file >50MB to test chunked processing
```

7. **Test async processing:**
```bash
POST /map-b2-data-async
{
  "file_name": "large_data/huge_file.xlsx",
  "mapping": {...}
}

# Check progress
GET /tasks/{task_id}
```

### Performance Validation

- Files >50MB should use chunked processing automatically
- Async processing should handle long-running tasks without timeouts
- Database queries should support pagination for large datasets
- Memory usage should remain stable during processing

## Development

- API documentation available at `http://localhost:8000/docs`
- Run tests: `pytest`

## Docker

Build and run with Docker:
```bash
docker build -t data-mapper .
docker run -p 8000:8000 data-mapper
```

**Complete Stack with Docker Compose:**
```bash
# Start everything
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

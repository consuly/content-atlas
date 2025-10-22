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

## Development

- API documentation available at `http://localhost:8000/docs`
- Run tests: `pytest`

## Docker

Build and run with Docker:
```bash
docker build -t data-mapper .
docker run -p 8000:8000 data-mapper

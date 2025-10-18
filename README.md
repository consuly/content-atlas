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
  "schema": {
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

## Development

- API documentation available at `http://localhost:8000/docs`
- Run tests: `pytest`

## Docker

Build and run with Docker:
```bash
docker build -t data-mapper .
docker run -p 8000:8000 data-mapper

# Testing Guide

Comprehensive guide for testing the ContentAtlas API application.

## Table of Contents

- [Overview](#overview)
- [Test Environment Setup](#test-environment-setup)
- [Running Tests](#running-tests)
- [Testing Workflows](#testing-workflows)
- [Manual Testing](#manual-testing)
- [Performance Testing](#performance-testing)
- [Test Data](#test-data)

---

## Overview

The application includes both automated tests and manual testing procedures to ensure reliability and performance.

### Test Coverage

- **Unit Tests**: Core functionality and data processing
- **Integration Tests**: API endpoints and database operations
- **Performance Tests**: Large file processing and chunking
- **Duplicate Detection Tests**: File-level and row-level duplicate checking

---

## Test Environment Setup

### 1. Start the Complete Stack

Ensure all services are running:

```bash
docker-compose up -d
```

This starts:
- PostgreSQL database
- API application (if configured in docker-compose)

### 2. Verify Services

Check that all services are running:

```bash
docker-compose ps
```

Expected output:
```
NAME                COMMAND                  SERVICE             STATUS
content-atlas-db-1  "docker-entrypoint.s…"   db                  Up
```

### 3. Install Test Dependencies

Ensure pytest and related packages are installed:

```bash
pip install pytest pytest-asyncio httpx
```

---

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test Files

```bash
# API tests
pytest tests/test_api.py

# LLM/Console tests
pytest tests/test_llm.py
```

### Run Tests with Verbose Output

```bash
pytest -v
```

### Run Tests with Coverage

```bash
pytest --cov=app --cov-report=html
```

### Run Specific Test Functions

```bash
# Run duplicate detection tests
pytest tests/test_api.py -k duplicate -v

# LLM-ready row-level duplicate handling (checks existing_row in preview)
python -m pytest tests/test_api.py::test_duplicate_detection_row_level -q

# Run a specific test
pytest tests/test_api.py::test_map_data_csv -v
```

---

## Testing Workflows

### Complete Data Processing Pipeline

This workflow tests the entire data import process from detection to insertion.

#### 1. Schema Detection

Test automatic schema detection:

```bash
curl -X POST "http://localhost:8000/detect-b2-mapping" \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "test_data/sample.csv"
  }'
```

**Expected Response:**
```json
{
  "success": true,
  "file_type": "csv",
  "detected_mapping": {
    "table_name": "sample",
    "db_schema": {...},
    "mappings": {...}
  },
  "columns_found": ["col1", "col2", "col3"],
  "rows_sampled": 100
}
```

#### 2. Data Preview

Extract and preview data before full import:

```bash
curl -X POST "http://localhost:8000/extract-b2-excel-csv" \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "test_data/sample.xlsx",
    "rows": 100
  }'
```

**Expected Response:**
```json
{
  "success": true,
  "sheets": {
    "Sheet1": "col1,col2\nval1,val2\n..."
  }
}
```

#### 3. Full Data Import

Import the complete dataset:

```bash
curl -X POST "http://localhost:8000/map-b2-data" \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "test_data/sample.csv",
    "mapping": {
      "table_name": "test_table",
      "db_schema": {
        "id": "INTEGER",
        "name": "VARCHAR(255)"
      },
      "mappings": {
        "id": "id",
        "name": "name"
      }
    }
  }'
```

**Expected Response:**
```json
{
  "success": true,
  "message": "B2 data mapped and inserted successfully",
  "records_processed": 100,
  "table_name": "test_table"
}
```

### Frontend Integration Testing

Test endpoints used by frontend applications.

#### 4. List All Tables

```bash
curl http://localhost:8000/tables
```

**Expected Response:**
```json
{
  "success": true,
  "tables": [
    {
      "table_name": "test_table",
      "row_count": 100
    }
  ]
}
```

#### 5. Query Table Data

```bash
curl "http://localhost:8000/tables/test_table?limit=10&offset=0"
```

**Expected Response:**
```json
{
  "success": true,
  "table_name": "test_table",
  "data": [...],
  "total_rows": 100,
  "limit": 10,
  "offset": 0
}
```

#### 6. Get Table Schema

```bash
curl http://localhost:8000/tables/test_table/schema
```

**Expected Response:**
```json
{
  "success": true,
  "table_name": "test_table",
  "columns": [
    {"name": "id", "type": "integer", "nullable": false},
    {"name": "name", "type": "character varying", "nullable": true}
  ]
}
```

#### 7. Get Table Statistics

```bash
curl http://localhost:8000/tables/test_table/stats
```

**Expected Response:**
```json
{
  "success": true,
  "table_name": "test_table",
  "total_rows": 100,
  "columns_count": 2,
  "data_types": {
    "id": "integer",
    "name": "character varying"
  }
}
```

---

## Manual Testing

### Testing File Upload (Local Files)

Create a test CSV file:

```csv
name,email,age
John Doe,john@example.com,30
Jane Smith,jane@example.com,25
```

Upload using curl:

```bash
curl -X POST "http://localhost:8000/map-data" \
  -F "file=@test.csv" \
  -F 'mapping_json={
    "table_name": "users",
    "db_schema": {
      "name": "VARCHAR(255)",
      "email": "VARCHAR(255)",
      "age": "INTEGER"
    },
    "mappings": {
      "name": "name",
      "email": "email",
      "age": "age"
    }
  }'
```

### Testing Duplicate Detection

#### Test 1: File-Level Duplicate Detection

Upload the same file twice:

```bash
# First upload (should succeed)
curl -X POST "http://localhost:8000/map-data" \
  -F "file=@test.csv" \
  -F 'mapping_json={
    "table_name": "users",
    "db_schema": {"name": "VARCHAR(255)"},
    "mappings": {"name": "name"},
    "duplicate_check": {
      "enabled": true,
      "check_file_level": true
    }
  }'

# Second upload (should fail with 409 Conflict)
curl -X POST "http://localhost:8000/map-data" \
  -F "file=@test.csv" \
  -F 'mapping_json={
    "table_name": "users",
    "db_schema": {"name": "VARCHAR(255)"},
    "mappings": {"name": "name"},
    "duplicate_check": {
      "enabled": true,
      "check_file_level": true
    }
  }'
```

#### Test 2: Row-Level Duplicate Detection

Create two files with overlapping data:

**file1.csv:**
```csv
email
john@example.com
jane@example.com
```

**file2.csv:**
```csv
email
john@example.com
bob@example.com
```

Upload both:

```bash
# First file (should succeed)
curl -X POST "http://localhost:8000/map-data" \
  -F "file=@file1.csv" \
  -F 'mapping_json={
    "table_name": "emails",
    "db_schema": {"email": "VARCHAR(255)"},
    "mappings": {"email": "email"},
    "duplicate_check": {
      "enabled": true,
      "check_file_level": false,
      "uniqueness_columns": ["email"]
    }
  }'

# Second file (should fail - john@example.com is duplicate)
curl -X POST "http://localhost:8000/map-data" \
  -F "file=@file2.csv" \
  -F 'mapping_json={
    "table_name": "emails",
    "db_schema": {"email": "VARCHAR(255)"},
    "mappings": {"email": "email"},
    "duplicate_check": {
      "enabled": true,
      "check_file_level": false,
      "uniqueness_columns": ["email"]
    }
  }'
```

### Testing Async Processing

For large files that require async processing:

```bash
# Start async task
TASK_ID=$(curl -X POST "http://localhost:8000/map-b2-data-async" \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "large_data/huge_file.xlsx",
    "mapping": {...}
  }' | jq -r '.task_id')

# Check status
curl "http://localhost:8000/tasks/$TASK_ID"

# Poll until complete
while true; do
  STATUS=$(curl -s "http://localhost:8000/tasks/$TASK_ID" | jq -r '.status')
  echo "Status: $STATUS"
  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "failed" ]; then
    break
  fi
  sleep 2
done
```

---

## Performance Testing

### Large File Testing (>50MB)

Test chunked processing with large files:

```bash
# Create a large test file (100,000 rows)
python -c "
import csv
with open('large_test.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['id', 'name', 'value'])
    for i in range(100000):
        writer.writerow([i, f'Name_{i}', i * 1.5])
"

# Upload and time the operation
time curl -X POST "http://localhost:8000/map-data" \
  -F "file=@large_test.csv" \
  -F 'mapping_json={
    "table_name": "large_test",
    "db_schema": {
      "id": "INTEGER",
      "name": "VARCHAR(255)",
      "value": "DECIMAL"
    },
    "mappings": {
      "id": "id",
      "name": "name",
      "value": "value"
    }
  }'
```

### Performance Validation Checklist

- [ ] Files >50MB use chunked processing automatically
- [ ] Async processing handles long-running tasks without timeouts
- [ ] Database queries support pagination for large datasets
- [ ] Memory usage remains stable during processing
- [ ] Parallel duplicate checking speeds up large imports

### Expected Performance Metrics

| File Size | Records | Processing Time | Method |
|-----------|---------|-----------------|--------|
| <1MB | <1,000 | <2 seconds | Standard |
| 1-10MB | 1,000-10,000 | 2-10 seconds | Standard |
| 10-50MB | 10,000-50,000 | 10-30 seconds | Chunked |
| >50MB | >50,000 | 30+ seconds | Chunked + Parallel |

---

## Test Data

### Sample CSV Files

**Simple CSV (test_data_small.csv):**
```csv
id,name,email
1,John Doe,john@example.com
2,Jane Smith,jane@example.com
3,Bob Johnson,bob@example.com
```

**CSV with Various Data Types:**
```csv
id,name,price,created_at,active
1,Product A,19.99,2024-01-01 10:00:00,true
2,Product B,29.99,2024-01-02 11:30:00,false
3,Product C,39.99,2024-01-03 14:15:00,true
```

### Sample Excel Files

Create test Excel files with multiple sheets for testing:

```python
import pandas as pd

# Create multi-sheet Excel file
with pd.ExcelWriter('test_workbook.xlsx') as writer:
    df1 = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C']
    })
    df2 = pd.DataFrame({
        'product': ['X', 'Y', 'Z'],
        'price': [10.0, 20.0, 30.0]
    })
    df1.to_excel(writer, sheet_name='Sheet1', index=False)
    df2.to_excel(writer, sheet_name='Sheet2', index=False)
```

---

## Automated Test Examples

### Example Test: CSV Upload

```python
def test_map_data_csv(client, test_csv_file):
    """Test uploading and mapping CSV data"""
    mapping = {
        "table_name": "test_users",
        "db_schema": {
            "name": "VARCHAR(255)",
            "email": "VARCHAR(255)"
        },
        "mappings": {
            "name": "name",
            "email": "email"
        }
    }
    
    response = client.post(
        "/map-data",
        files={"file": test_csv_file},
        data={"mapping_json": json.dumps(mapping)}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["records_processed"] > 0
```

### Example Test: Duplicate Detection

```python
def test_duplicate_detection(client, test_csv_file):
    """Test file-level duplicate detection"""
    mapping = {
        "table_name": "test_duplicates",
        "db_schema": {"name": "VARCHAR(255)"},
        "mappings": {"name": "name"},
        "duplicate_check": {
            "enabled": True,
            "check_file_level": True
        }
    }
    
    # First upload should succeed
    response1 = client.post(
        "/map-data",
        files={"file": test_csv_file},
        data={"mapping_json": json.dumps(mapping)}
    )
    assert response1.status_code == 200
    
    # Second upload should fail
    response2 = client.post(
        "/map-data",
        files={"file": test_csv_file},
        data={"mapping_json": json.dumps(mapping)}
    )
    assert response2.status_code == 409
```

---

## Related Documentation

- [API Reference](API_REFERENCE.md) - Complete API endpoint documentation
- [Setup Guide](SETUP.md) - Environment setup and configuration
- [Duplicate Detection](DUPLICATE_DETECTION.md) - Duplicate detection system details
- [Parallel Processing](PARALLEL_PROCESSING.md) - Large file processing details

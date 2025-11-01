# Import Tracking and Data Lineage

Comprehensive documentation for the import history tracking system in Content Atlas.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Database Schema](#database-schema)
- [API Endpoints](#api-endpoints)
- [Usage Examples](#usage-examples)
- [Integration Guide](#integration-guide)
- [Best Practices](#best-practices)

---

## Overview

The import tracking system provides comprehensive auditing and lineage tracking for all data imports in Content Atlas. Every file upload and data import operation is tracked with detailed metadata, enabling:

- **Full Traceability**: Track where data came from and when it was imported
- **Performance Monitoring**: Analyze import performance and identify bottlenecks
- **Error Analysis**: Review failed imports and understand what went wrong
- **Data Lineage**: Trace which imports contributed data to each table
- **Compliance**: Maintain audit trails for regulatory requirements

## Features

### Automatic Tracking

All import operations are automatically tracked without requiring any changes to existing code. The system captures:

- **Source Information**: File name, size, type, hash, and source location (local upload vs B2 storage)
- **Destination**: Target table name and import strategy used
- **Configuration**: Complete mapping configuration used for the import
- **Outcome**: Success/failure status with detailed error messages
- **Statistics**: Row counts, duplicates found, validation errors
- **Performance Metrics**: Parsing time, duplicate check time, insert time, total duration
- **User Context**: User ID and email (when available)

### Import History Table

The `import_history` table stores all import metadata with the following structure:

```sql
CREATE TABLE import_history (
    import_id UUID PRIMARY KEY,
    import_timestamp TIMESTAMP DEFAULT NOW(),
    
    -- User/Actor Information
    user_id VARCHAR(255),
    user_email VARCHAR(255),
    
    -- Source Information
    source_type VARCHAR(50) NOT NULL,  -- 'local_upload', 'b2_storage', 'api_direct'
    source_path TEXT,
    file_name VARCHAR(500),
    file_size_bytes BIGINT,
    file_type VARCHAR(50),  -- 'csv', 'excel', 'json', 'xml'
    file_hash VARCHAR(64),  -- SHA-256 hash
    
    -- Destination Information
    table_name VARCHAR(255) NOT NULL,
    import_strategy VARCHAR(50),
    
    -- Configuration
    mapping_config JSONB,
    duplicate_check_enabled BOOLEAN DEFAULT TRUE,
    
    -- Import Outcome
    status VARCHAR(50) NOT NULL,  -- 'success', 'failed', 'partial'
    error_message TEXT,
    warnings TEXT[],
    
    -- Statistics
    total_rows_in_file INTEGER,
    rows_processed INTEGER,
    rows_inserted INTEGER,
    rows_skipped INTEGER,
    duplicates_found INTEGER,
    validation_errors INTEGER,
    
    -- Performance Metrics
    duration_seconds DECIMAL(10, 3),
    parsing_time_seconds DECIMAL(10, 3),
    duplicate_check_time_seconds DECIMAL(10, 3),
    insert_time_seconds DECIMAL(10, 3),
    
    -- Additional Context
    analysis_id UUID,
    task_id UUID,
    metadata JSONB,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Indexed Queries

The system includes optimized indexes for common query patterns:

- `idx_import_history_timestamp`: Fast chronological queries
- `idx_import_history_table`: Quick lookups by table name
- `idx_import_history_status`: Filter by success/failure
- `idx_import_history_user`: User-specific queries
- `idx_import_history_file_hash`: Duplicate file detection

---

## Database Schema

### Import Status Values

- **`in_progress`**: Import is currently being processed
- **`success`**: Import completed successfully
- **`failed`**: Import failed completely
- **`partial`**: Import partially succeeded (some rows inserted, some failed)

### Source Type Values

- **`local_upload`**: File uploaded directly via API
- **`b2_storage`**: File downloaded from Backblaze B2
- **`api_direct`**: Data provided directly via API (no file)

### Import Strategy Values

- **`new_table`**: Created a new table
- **`merge_exact`**: Merged into existing table with exact schema match
- **`extend_table`**: Extended existing table with new columns
- **`adapt_data`**: Adapted data to fit existing schema

---

## API Endpoints

### List Import History

```http
GET /import-history
```

**Query Parameters:**
- `table_name` (optional): Filter by destination table
- `user_id` (optional): Filter by user
- `status` (optional): Filter by status ('success', 'failed', 'partial')
- `limit` (optional): Maximum records to return (default: 100)
- `offset` (optional): Pagination offset (default: 0)

**Response:**
```json
{
  "success": true,
  "imports": [
    {
      "import_id": "550e8400-e29b-41d4-a716-446655440000",
      "import_timestamp": "2025-10-28T12:30:00Z",
      "source_type": "local_upload",
      "file_name": "customers.csv",
      "file_size_bytes": 1048576,
      "file_type": "csv",
      "table_name": "customers",
      "status": "success",
      "rows_inserted": 1000,
      "duration_seconds": 2.5,
      ...
    }
  ],
  "total_count": 1,
  "limit": 100,
  "offset": 0
}
```

### Get Import Details

```http
GET /import-history/{import_id}
```

**Response:**
```json
{
  "success": true,
  "import_record": {
    "import_id": "550e8400-e29b-41d4-a716-446655440000",
    "import_timestamp": "2025-10-28T12:30:00Z",
    "user_id": "user123",
    "user_email": "user@example.com",
    "source_type": "local_upload",
    "source_path": null,
    "file_name": "customers.csv",
    "file_size_bytes": 1048576,
    "file_type": "csv",
    "file_hash": "abc123...",
    "table_name": "customers",
    "import_strategy": "new_table",
    "mapping_config": { ... },
    "duplicate_check_enabled": true,
    "status": "success",
    "error_message": null,
    "warnings": [],
    "total_rows_in_file": 1000,
    "rows_processed": 1000,
    "rows_inserted": 1000,
    "rows_skipped": 0,
    "duplicates_found": 0,
    "validation_errors": 0,
    "duration_seconds": 2.5,
    "parsing_time_seconds": 0.5,
    "duplicate_check_time_seconds": 0.8,
    "insert_time_seconds": 1.2,
    "analysis_id": null,
    "task_id": null,
    "metadata": {},
    "created_at": "2025-10-28T12:30:00Z",
    "updated_at": "2025-10-28T12:30:02Z"
  }
}
```

### Get Import Statistics

```http
GET /import-statistics
```

**Query Parameters:**
- `table_name` (optional): Filter by table
- `user_id` (optional): Filter by user
- `days` (optional): Look back period in days (default: 30)

**Response:**
```json
{
  "success": true,
  "total_imports": 150,
  "successful_imports": 145,
  "failed_imports": 5,
  "total_rows_inserted": 1500000,
  "total_duplicates_found": 250,
  "avg_duration_seconds": 3.2,
  "tables_affected": 12,
  "unique_users": 5,
  "period_days": 30
}
```

### Get Table Lineage

```http
GET /tables/{table_name}/lineage
```

**Response:**
```json
{
  "success": true,
  "table_name": "customers",
  "imports": [
    {
      "import_id": "...",
      "import_timestamp": "2025-10-28T12:30:00Z",
      "file_name": "customers_batch1.csv",
      "rows_inserted": 500,
      ...
    },
    {
      "import_id": "...",
      "import_timestamp": "2025-10-27T10:15:00Z",
      "file_name": "customers_batch2.csv",
      "rows_inserted": 300,
      ...
    }
  ],
  "total_imports": 2,
  "total_rows_contributed": 800
}
```

---

## Usage Examples

### Example 1: Track All Imports for a Table

```python
import requests

# Get all imports for the 'customers' table
response = requests.get(
    "http://localhost:8000/import-history",
    params={"table_name": "customers", "limit": 50}
)

imports = response.json()["imports"]
for imp in imports:
    print(f"{imp['import_timestamp']}: {imp['file_name']} - {imp['rows_inserted']} rows")
```

### Example 2: Analyze Failed Imports

```python
# Get all failed imports in the last 7 days
response = requests.get(
    "http://localhost:8000/import-history",
    params={"status": "failed", "limit": 100}
)

failed_imports = response.json()["imports"]
for imp in failed_imports:
    print(f"Failed: {imp['file_name']}")
    print(f"Error: {imp['error_message']}")
    print(f"Duplicates found: {imp['duplicates_found']}")
    print("---")
```

### Example 3: Monitor Import Performance

```python
# Get statistics for the last 30 days
response = requests.get(
    "http://localhost:8000/import-statistics",
    params={"days": 30}
)

stats = response.json()
print(f"Success Rate: {stats['successful_imports'] / stats['total_imports'] * 100:.1f}%")
print(f"Average Duration: {stats['avg_duration_seconds']:.2f}s")
print(f"Total Rows Imported: {stats['total_rows_inserted']:,}")
```

### Example 4: Trace Data Lineage

```python
# Find all sources that contributed to a table
response = requests.get(
    "http://localhost:8000/tables/customers/lineage"
)

lineage = response.json()
print(f"Table: {lineage['table_name']}")
print(f"Total Imports: {lineage['total_imports']}")
print(f"Total Rows: {lineage['total_rows_contributed']}")
print("\nSources:")
for imp in lineage['imports']:
    print(f"  - {imp['file_name']} ({imp['rows_inserted']} rows)")
```

---

## Integration Guide

### Automatic Integration

The import tracking system is automatically integrated into all import endpoints:

- `/map-data` - Local file uploads
- `/map-b2-data` - B2 storage imports
- `/map-b2-data-async` - Async B2 imports

No code changes are required to enable tracking.

### Manual Integration

If you're creating custom import logic, use the tracking functions:

```python
from app.domain.imports.history import start_import_tracking, complete_import_tracking
from app.models import calculate_file_hash
import time

# Start tracking
start_time = time.time()
import_id = start_import_tracking(
    source_type="local_upload",
    file_name="data.csv",
    table_name="my_table",
    file_size_bytes=len(file_content),
    file_type="csv",
    file_hash=calculate_file_hash(file_content),
    mapping_config=config,
    user_id="user123",
    user_email="user@example.com"
)

try:
    # Perform import...
    records_inserted = insert_records(...)
    
    # Complete tracking on success
    complete_import_tracking(
        import_id=import_id,
        status="success",
        total_rows_in_file=len(records),
        rows_processed=records_inserted,
        rows_inserted=records_inserted,
        duration_seconds=time.time() - start_time
    )
except Exception as e:
    # Complete tracking on failure
    complete_import_tracking(
        import_id=import_id,
        status="failed",
        total_rows_in_file=0,
        rows_processed=0,
        rows_inserted=0,
        duration_seconds=time.time() - start_time,
        error_message=str(e)
    )
    raise
```

---

## Best Practices

### 1. Regular Monitoring

Set up regular monitoring of import statistics:

```python
# Daily check for failed imports
failed_imports = get_import_history(status="failed", limit=100)
if len(failed_imports) > 10:
    send_alert("High number of failed imports detected")
```

### 2. Performance Analysis

Use performance metrics to identify bottlenecks:

```python
# Find slow imports
slow_imports = [
    imp for imp in get_import_history(limit=1000)
    if imp['duration_seconds'] > 30
]

# Analyze what's slow
for imp in slow_imports:
    print(f"File: {imp['file_name']}")
    print(f"  Parsing: {imp['parsing_time_seconds']}s")
    print(f"  Duplicate Check: {imp['duplicate_check_time_seconds']}s")
    print(f"  Insert: {imp['insert_time_seconds']}s")
```

### 3. Data Lineage Queries

Use lineage information for data governance:

```python
# Find all tables affected by a specific file
file_hash = "abc123..."
imports = get_import_history(limit=1000)
affected_tables = {
    imp['table_name'] 
    for imp in imports 
    if imp['file_hash'] == file_hash
}
```

### 4. Cleanup Old Records

Implement a retention policy for import history:

```python
# Archive imports older than 1 year
from datetime import datetime, timedelta
cutoff_date = datetime.now() - timedelta(days=365)

# Move to archive table or delete
# (Implementation depends on your requirements)
```

### 5. User Attribution

Always provide user context when available:

```python
# Extract from authentication
user_id = request.user.id
user_email = request.user.email

import_id = start_import_tracking(
    ...,
    user_id=user_id,
    user_email=user_email
)
```

---

## Future Enhancements

### Row-Level Lineage (Optional)

Add an `import_id` column to data tables to track which import created each row:

```sql
ALTER TABLE customers ADD COLUMN import_id UUID;
CREATE INDEX idx_customers_import_id ON customers(import_id);
```

This enables:
- Precise data lineage at the row level
- Rollback capability (delete all rows from a specific import)
- Impact analysis (which rows came from which source)

### Import Rollback

Implement rollback functionality:

```python
def rollback_import(import_id: str, table_name: str):
    """Delete all data from a specific import."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM "{table_name}"
            WHERE import_id = :import_id
        """), {"import_id": import_id})
```

### Persistent Task Storage

Move async task tracking from in-memory to database:

```sql
CREATE TABLE import_tasks (
    task_id UUID PRIMARY KEY,
    import_id UUID REFERENCES import_history(import_id),
    status VARCHAR(50),
    progress INTEGER,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

---

## Related Documentation

- [API Reference](API_REFERENCE.md) - Complete API documentation
- [Architecture](ARCHITECTURE.md) - System architecture overview
- [Duplicate Detection](DUPLICATE_DETECTION.md) - Duplicate detection system
- [Parallel Processing](PARALLEL_PROCESSING.md) - Large file processing

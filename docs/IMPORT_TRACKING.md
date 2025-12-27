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
- [Mapping Status Tracking](#mapping-status-tracking)

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
    "mapping_status": "completed",
    "mapping_errors_count": 0,
    "total_rows_in_file": 1000,
    "rows_processed": 1000,
    "rows_inserted": 1000,
    "rows_skipped": 0,
    "duplicates_found": 0,
    "data_validation_errors": 0,
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

### Get Mapping Errors

```http
GET /import-history/{import_id}/mapping-errors
```

**Query Parameters:**
- `limit` (optional): Maximum records to return (default: 100)
- `offset` (optional): Pagination offset (default: 0)

**Response:**
```json
{
  "success": true,
  "errors": [
    {
      "id": 1,
      "import_id": "550e8400-...",
      "record_number": 5,
      "error_type": "type_mismatch",
      "error_message": "Value 'abc' is not a valid integer",
      "source_field": "age",
      "target_field": "age_years",
      "source_value": "abc",
      "occurred_at": "2025-10-28T12:30:05Z",
      "chunk_number": 1
    }
  ],
  "total_count": 1,
  "limit": 100,
  "offset": 0
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

## Undo and Rollback Features

This feature adds comprehensive import tracking to Content Atlas, enabling full traceability of data imports, selective undo/rollback capabilities, and detailed correction tracking. Every row imported into the system is now linked to its source import, allowing for precise data lineage and easy rollback of problematic imports.

### Key Features

#### 1. Metadata Columns

All dynamically created data tables now include four metadata columns:

- **`_import_id`** (UUID, NOT NULL): Links to `import_history.import_id` with CASCADE DELETE
- **`_imported_at`** (TIMESTAMP): Timestamp when the row was inserted
- **`_source_row_number`** (INTEGER): Original row number in the source file (1-indexed)
- **`_corrections_applied`** (JSONB): Tracks data transformations and corrections

These columns are:
- Prefixed with `_` to distinguish from user data
- Automatically added during table creation
- Indexed on `_import_id` for efficient queries
- Hidden from user-facing queries (future enhancement)

#### 2. Import Tracking

Every import operation is tracked in the `import_history` table with:
- Unique import ID (UUID)
- Source information (file name, type, hash, path)
- Destination table name
- Import statistics (rows processed, inserted, errors)
- Mapping configuration used
- Performance metrics
- Status and error messages

#### 3. Corrections Tracking

The system tracks data transformations during import:

**Type Coercion:**
```json
{
  "age": {
    "before": "30.0",
    "after": 30,
    "correction_type": "type_coercion",
    "target_type": "INTEGER"
  }
}
```

**Datetime Standardization:**
```json
{
  "event_date": {
    "before": "10/09/2025 8:11 PM",
    "after": "2025-10-09T20:11:00",
    "correction_type": "datetime_standardization",
    "source_format": "%m/%d/%Y %I:%M %p"
  }
}
```

**Performance Impact:**
- <5% overhead for imports without corrections
- <10% overhead with corrections
- Only populated when corrections occur (NULL otherwise)

#### 4. Cascading Deletes

Foreign key constraint with CASCADE DELETE:
```sql
_import_id UUID NOT NULL REFERENCES import_history(import_id) ON DELETE CASCADE
```

**Benefits:**
- Automatic cleanup when import is deleted
- Database-level integrity
- No orphaned data
- Simple undo operation: `DELETE FROM import_history WHERE import_id = ?`

#### 5. Selective Undo/Rollback

**Multiple imports to same table:**
- Each import tracked separately
- Can undo specific imports without affecting others
- Maintains data integrity across operations

**Example workflow:**
1. Import file A → 100 rows added (import_id: abc-123)
2. Import file B → 50 rows added (import_id: def-456)
3. Undo import A → Only 100 rows from file A removed
4. File B data remains intact

### Implementation Details

#### Database Schema Changes

**Table Creation (app/models.py):**
```python
CREATE TABLE "{table_name}" (
    id SERIAL PRIMARY KEY,
    {user_columns},
    _import_id UUID NOT NULL REFERENCES import_history(import_id) ON DELETE CASCADE,
    _imported_at TIMESTAMP DEFAULT NOW(),
    _source_row_number INTEGER,
    _corrections_applied JSONB
);

CREATE INDEX idx_{table_name}_import_id ON "{table_name}"(_import_id);
```

**Metadata Population:**
- `_import_id`: Retrieved from active import in `import_history`
- `_source_row_number`: Enumerated during insertion (1-indexed)
- `_corrections_applied`: Tracked during type coercion and mapping
- `_imported_at`: Auto-populated by database

#### Correction Tracking Logic

**Type Coercion Detection:**
```python
if str(original_value) != str(coerced_value):
    corrections[col_name] = {
        "before": str(original_value),
        "after": coerced_value,
        "correction_type": "type_coercion",
        "target_type": sql_type
    }
```

**Datetime Transformation:**
- Tracked in `mapper.py` during `apply_rules()`
- Logged when standardization occurs
- Includes source format and target format

#### Query Examples

**Find all rows from a specific import:**
```sql
SELECT * FROM my_table WHERE _import_id = 'abc-123-def-456';
```

**Get import statistics:**
```sql
SELECT 
    _import_id,
    COUNT(*) as row_count,
    MIN(_imported_at) as import_started,
    MAX(_imported_at) as import_completed
FROM my_table
GROUP BY _import_id;
```

**Find rows with corrections:**
```sql
SELECT * FROM my_table 
WHERE _corrections_applied IS NOT NULL;
```

**Link errors to actual rows:**
```sql
SELECT 
    m.record_number,
    m.error_message,
    t.*
FROM mapping_errors m
JOIN my_table t ON t._source_row_number = m.record_number
WHERE m.import_id = 'abc-123';
```

### Testing

Comprehensive test suite in `tests/test_import_tracking.py`:

#### Test Coverage

1. **Metadata Columns** (2 tests)
   - ✅ Columns created correctly
   - ✅ Import ID populated for all rows

2. **Corrections Tracking** (3 tests)
   - ✅ Type coercion tracked
   - ✅ Datetime conversion tracked
   - ✅ NULL when no corrections

3. **Cascading Delete** (1 test)
   - ✅ Data deleted when import removed

4. **Multiple Imports** (2 tests)
   - ✅ Tracked separately
   - ✅ Selective undo works

5. **Import Lineage** (1 test)
   - ✅ Query all imports for table

**Test Results:** 9/9 core tests passing

#### Running Tests

```bash
# Run all import tracking tests
pytest tests/test_import_tracking.py -v

# Run specific test class
pytest tests/test_import_tracking.py::TestMetadataColumns -v

# Skip tests requiring API endpoints
pytest tests/test_import_tracking.py -k "not test_metadata_hidden" -v
```

### Performance Considerations

#### Optimizations Implemented

1. **Lazy correction tracking** - Only computed when corrections occur
2. **Batch operations** - Single transaction per chunk
3. **Indexed metadata** - Fast queries on `_import_id`
4. **Chunked processing** - Already implemented (20K chunks)
5. **Minimal overhead** - <5-10% performance impact

#### Storage Impact

**Per row overhead:**
- `_import_id`: 16 bytes (UUID)
- `_imported_at`: 8 bytes (TIMESTAMP)
- `_source_row_number`: 4 bytes (INTEGER)
- `_corrections_applied`: Variable (NULL when no corrections)

**Total:** ~28 bytes + corrections (if any)

**Example:** 1M rows = ~28 MB overhead (negligible)

### Migration Guide

#### Existing Tables

Tables created before this feature won't have metadata columns. Options:

1. **Leave as-is** - Old tables work without tracking
2. **Add columns** - Backfill with NULL values
3. **Recreate** - Drop and reimport with tracking

#### Adding Metadata to Existing Table

```sql
ALTER TABLE existing_table 
ADD COLUMN _import_id UUID REFERENCES import_history(import_id) ON DELETE CASCADE,
ADD COLUMN _imported_at TIMESTAMP DEFAULT NOW(),
ADD COLUMN _source_row_number INTEGER,
ADD COLUMN _corrections_applied JSONB;

CREATE INDEX idx_existing_table_import_id ON existing_table(_import_id);
```

### Security Considerations

1. **Cascading deletes** - Ensure proper permissions on `import_history`
2. **Metadata visibility** - Hide from untrusted users
3. **Audit trail** - Metadata provides complete audit log
4. **Data retention** - Consider compliance requirements

## Mapping Status Tracking

The Content Atlas system now tracks the status and errors of data mapping operations during file imports. This provides complete visibility into whether files were successfully mapped, partially mapped with errors, or failed during the mapping phase.

### Database Schema Updates

#### Enhanced `import_history` Table

The `import_history` table has been enhanced with mapping-specific fields:

```sql
-- Mapping Status Fields
mapping_status VARCHAR(50) DEFAULT 'not_started'
mapping_started_at TIMESTAMP
mapping_completed_at TIMESTAMP
mapping_duration_seconds DECIMAL(10, 3)
mapping_errors_count INTEGER DEFAULT 0
```

**Mapping Status Values:**
- `not_started` - Import created but mapping hasn't begun
- `in_progress` - Actively mapping data
- `completed` - Successfully mapped all records
- `completed_with_errors` - Completed but some records had errors
- `failed` - Mapping process failed completely

#### New `mapping_errors` Table

A dedicated table stores detailed mapping error information:

```sql
CREATE TABLE mapping_errors (
    id SERIAL PRIMARY KEY,
    import_id UUID NOT NULL REFERENCES import_history(import_id) ON DELETE CASCADE,
    
    -- Error Context
    record_number INTEGER,           -- Which record in the file (1-indexed)
    source_field VARCHAR(255),       -- Which source field caused the error
    target_field VARCHAR(255),       -- Target database column
    
    -- Error Details
    error_type VARCHAR(100),         -- 'datetime_conversion', 'type_mismatch', etc.
    error_message TEXT NOT NULL,     -- Full error message
    source_value TEXT,               -- The problematic value (truncated if >500 chars)
    
    -- Metadata
    occurred_at TIMESTAMP DEFAULT NOW(),
    chunk_number INTEGER             -- For parallel processing tracking
);
```

**Indexes:**
- `idx_mapping_errors_import` - Fast lookup by import_id
- `idx_mapping_errors_type` - Filter by error type
- `idx_mapping_errors_field` - Filter by source field

### Implementation

#### 1. Tracking Flow

The mapping status is tracked through the import lifecycle:

```python
# Start mapping
update_mapping_status(import_id, 'in_progress')

# Perform mapping
mapped_records, mapping_errors = map_data(records, mapping_config)

# Record errors if any
if mapping_errors:
    record_mapping_errors_batch(import_id, error_records)
    status = 'completed_with_errors' if mapped_records else 'failed'
else:
    status = 'completed'

# Update final status
update_mapping_status(import_id, status, len(mapping_errors), duration)
```

#### 2. Error Storage

Mapping errors are stored in a structured format:

```python
error_record = {
    'record_number': 1,
    'error_type': 'datetime_conversion',
    'error_message': 'Failed to convert datetime field...',
    'source_field': 'created_date',
    'target_field': 'created_at',
    'source_value': '2023-13-45',  # Invalid date
    'chunk_number': 1
}
```

#### 3. API Functions

**Update Mapping Status:**
```python
update_mapping_status(
    import_id: str,
    status: str,  # 'in_progress', 'completed', 'completed_with_errors', 'failed'
    errors_count: int = 0,
    duration_seconds: Optional[float] = None
)
```

**Record Mapping Errors (Batch):**
```python
record_mapping_errors_batch(
    import_id: str,
    errors: List[Dict[str, Any]]
)
```

**Retrieve Mapping Errors:**
```python
get_mapping_errors(
    import_id: str,
    limit: int = 100,
    offset: int = 0,
    error_type: Optional[str] = None
) -> List[Dict[str, Any]]
```

### Benefits

1. **Complete Visibility**
   - Know exactly which files were successfully mapped
   - Identify files that had partial mapping failures
   - Track mapping performance metrics

2. **Error Diagnosis**
   - Full error messages preserved for debugging
   - Context about which records and fields failed
   - Ability to query errors by type or field

3. **Scalability**
   - Separate table prevents row size issues
   - Efficient indexing for fast queries
   - Easy to paginate through large error sets

4. **Data Integrity**
   - Cascade delete ensures cleanup when imports are removed
   - Atomic operations prevent partial state
   - Transaction safety maintained

### Usage Examples

#### Check Mapping Status

```python
from app.domain.imports.history import get_import_history

# Get import record
imports = get_import_history(import_id="uuid-here")
import_record = imports[0]

print(f"Mapping Status: {import_record['mapping_status']}")
print(f"Errors Count: {import_record['mapping_errors_count']}")
print(f"Duration: {import_record['mapping_duration_seconds']}s")
```

#### Retrieve Mapping Errors

```python
from app.domain.imports.history import get_mapping_errors

# Get all errors for an import
errors = get_mapping_errors(import_id="uuid-here", limit=100)

for error in errors:
    print(f"Record {error['record_number']}: {error['error_message']}")
    print(f"  Field: {error['source_field']}")
    print(f"  Value: {error['source_value']}")
```

#### Filter Errors by Type

```python
# Get only datetime conversion errors
datetime_errors = get_mapping_errors(
    import_id="uuid-here",
    error_type="datetime_conversion"
)
```

### Performance Considerations

- **Memory Efficiency**: Errors stored in separate table (not JSONB in main table), source values truncated to 500 characters, batch inserts for efficiency.
- **Query Performance**: Indexed by import_id for fast lookup, indexed by error_type for filtering, pagination support for large error sets.
- **Parallel Processing**: Chunk numbers tracked for parallel mapping, errors aggregated from multiple workers, thread-safe batch insertion.

### Development Workflow

**Database Reset**: When resetting the development database, both `import_history` and `mapping_errors` tables are automatically created.

**Testing**: Tests should exclude the `mapping_errors` table when counting user data tables.

### Future Enhancements

1. **Enhanced Error Context**: Parse error messages to extract field names and values automatically
2. **Error Aggregation**: Group similar errors for easier analysis
3. **Retry Mechanism**: Allow re-mapping of failed records with corrected data
4. **Error Notifications**: Alert users when mapping errors exceed threshold
5. **Error Analytics**: Dashboard showing common error patterns

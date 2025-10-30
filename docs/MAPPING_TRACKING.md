# Mapping Status Tracking

## Overview

The Content Atlas system now tracks the status and errors of data mapping operations during file imports. This provides complete visibility into whether files were successfully mapped, partially mapped with errors, or failed during the mapping phase.

## Database Schema

### Enhanced `import_history` Table

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

### New `mapping_errors` Table

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

## Implementation

### 1. Tracking Flow

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

### 2. Error Storage

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

### 3. API Functions

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

## Benefits

### 1. Complete Visibility
- Know exactly which files were successfully mapped
- Identify files that had partial mapping failures
- Track mapping performance metrics

### 2. Error Diagnosis
- Full error messages preserved for debugging
- Context about which records and fields failed
- Ability to query errors by type or field

### 3. Scalability
- Separate table prevents row size issues
- Efficient indexing for fast queries
- Easy to paginate through large error sets

### 4. Data Integrity
- Cascade delete ensures cleanup when imports are removed
- Atomic operations prevent partial state
- Transaction safety maintained

## Usage Examples

### Check Mapping Status

```python
from app.import_history import get_import_history

# Get import record
imports = get_import_history(import_id="uuid-here")
import_record = imports[0]

print(f"Mapping Status: {import_record['mapping_status']}")
print(f"Errors Count: {import_record['mapping_errors_count']}")
print(f"Duration: {import_record['mapping_duration_seconds']}s")
```

### Retrieve Mapping Errors

```python
from app.import_history import get_mapping_errors

# Get all errors for an import
errors = get_mapping_errors(import_id="uuid-here", limit=100)

for error in errors:
    print(f"Record {error['record_number']}: {error['error_message']}")
    print(f"  Field: {error['source_field']}")
    print(f"  Value: {error['source_value']}")
```

### Filter Errors by Type

```python
# Get only datetime conversion errors
datetime_errors = get_mapping_errors(
    import_id="uuid-here",
    error_type="datetime_conversion"
)
```

## Performance Considerations

### Memory Efficiency
- Errors stored in separate table (not JSONB in main table)
- Source values truncated to 500 characters
- Batch inserts for efficiency

### Query Performance
- Indexed by import_id for fast lookup
- Indexed by error_type for filtering
- Pagination support for large error sets

### Parallel Processing
- Chunk numbers tracked for parallel mapping
- Errors aggregated from multiple workers
- Thread-safe batch insertion

## Development Workflow

### Database Reset
When resetting the development database, both tables are automatically created:

```python
from app.import_history import create_import_history_table

# Creates both import_history and mapping_errors tables
create_import_history_table()
```

### Testing
Tests should exclude the `mapping_errors` table when counting user data tables:

```python
user_tables = [
    t for t in all_tables
    if t['table_name'] not in [
        'file_imports',
        'table_metadata', 
        'import_history',
        'mapping_errors'  # System table
    ]
]
```

## Future Enhancements

### Potential Improvements
1. **Enhanced Error Context**: Parse error messages to extract field names and values automatically
2. **Error Aggregation**: Group similar errors for easier analysis
3. **Retry Mechanism**: Allow re-mapping of failed records with corrected data
4. **Error Notifications**: Alert users when mapping errors exceed threshold
5. **Error Analytics**: Dashboard showing common error patterns

### API Endpoints (Future)
```python
# Get mapping errors for an import
GET /import-history/{import_id}/mapping-errors

# Get mapping error summary
GET /import-history/{import_id}/mapping-summary

# Get error statistics
GET /mapping-errors/statistics?days=30
```

## Related Documentation

- [Import Tracking](IMPORT_TRACKING.md) - Overall import tracking system
- [Parallel Processing](PARALLEL_PROCESSING.md) - How mapping works with parallel processing
- [Architecture](ARCHITECTURE.md) - System architecture overview

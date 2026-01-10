# Temporary Tables

Temporary tables are a feature designed for data transformation workflows where users need to create intermediate tables that:
- Are hidden from LLM/RAG context by default
- Auto-expire after a configurable period
- Block additional data imports by default (configurable)
- Are automatically cleaned up when expired

## Table of Contents

- [Overview](#overview)
- [Use Cases](#use-cases)
- [How It Works](#how-it-works)
- [API Reference](#api-reference)
- [Configuration Options](#configuration-options)
- [LLM/RAG Behavior](#llmrag-behavior)
- [Examples and Workflows](#examples-and-workflows)
- [Import Protection](#import-protection)
- [Troubleshooting](#troubleshooting)

## Overview

Temporary tables provide a way to mark tables as temporary, preventing them from cluttering your database and LLM context. They're particularly useful for:

- Data transformation pipelines
- Testing data imports
- Temporary analysis workspaces
- One-off data processing tasks

### Key Features

1. **Automatic Expiration**: Tables expire after a configurable number of days (default: 7 days)
2. **Hidden from LLM**: Temporary tables are excluded from LLM context by default
3. **Import Protection**: Additional imports blocked by default (prevents accidental overwrites)
4. **Automatic Cleanup**: Expired tables are automatically deleted every 24 hours
5. **Explicit Access**: Can still be accessed when explicitly mentioned by name

## Use Cases

### 1. Data Transformation Workflows

```
1. Import raw data → temporary table
2. Transform/clean data using SQL queries
3. Export to permanent table
4. Temporary table auto-expires
```

### 2. Testing Data Imports

```
1. Test import → temporary table (marked as temporary)
2. Verify data quality
3. If good: convert to permanent
4. If bad: let it expire
```

### 3. Temporary Analysis

```
1. Create analysis workspace → temporary table
2. Join with existing data
3. Generate reports
4. Table expires after analysis complete
```

## How It Works

### Marking a Table as Temporary

When you mark a table as temporary:

1. An entry is created in the `temporary_tables` tracking table
2. The table gets an expiration date (default: 7 days from now)
3. Additional imports are blocked by default
4. The table is hidden from LLM queries

### LLM Hiding Mechanism

The LLM agent uses modified database context functions:

- **`list_tables_tool`**: Filters out temporary tables automatically
- **`get_table_schema_tool`**: Includes temporary tables when explicitly named

This means:
- ✅ "List all my tables" → excludes temporary tables
- ✅ "Show me the schema for temp_analysis" → includes temp_analysis if it's temporary
- ✅ "Join customers with temp_data" → works, temp_data is explicitly referenced

### Auto-Cleanup

A background task runs every 24 hours:

1. Queries `temporary_tables` for expired tables
2. Drops each expired table from the database
3. Removes the tracking record
4. Logs the cleanup results

## API Reference

### List Temporary Tables

```http
GET /tables/temporary
```

Returns all temporary tables for the current organization.

**Response:**
```json
{
  "success": true,
  "tables": [
    {
      "table_name": "temp_analysis",
      "created_at": "2026-01-01T00:00:00Z",
      "expires_at": "2026-01-08T00:00:00Z",
      "organization_id": 1,
      "allow_additional_imports": false,
      "purpose": "Data transformation workspace",
      "row_count": 1500
    }
  ],
  "total_count": 1
}
```

### Mark Table as Temporary

```http
POST /tables/{table_name}/mark-temporary
```

Marks an existing table as temporary with automatic expiration.

**Request Body:**
```json
{
  "expires_days": 7,
  "allow_additional_imports": false,
  "purpose": "Data transformation workspace"
}
```

**Parameters:**
- `expires_days` (integer, 1-365): Days until expiration
- `allow_additional_imports` (boolean): Whether to allow more data imports
- `purpose` (string, optional): Description of the table's purpose

**Response:**
```json
{
  "success": true,
  "message": "Table 'temp_analysis' marked as temporary",
  "table_info": {
    "table_name": "temp_analysis",
    "expires_at": "2026-01-08T00:00:00Z",
    "allow_additional_imports": false,
    "purpose": "Data transformation workspace",
    "row_count": 1500
  }
}
```

### Convert to Permanent Table

```http
DELETE /tables/{table_name}/mark-temporary
```

Removes temporary status, converting the table to permanent.

**Response:**
```json
{
  "success": true,
  "table_name": "temp_analysis",
  "message": "Table 'temp_analysis' converted to permanent"
}
```

### Extend Expiration

```http
POST /tables/{table_name}/extend-expiration
```

Extends the expiration date of a temporary table.

**Request Body:**
```json
{
  "additional_days": 14
}
```

**Parameters:**
- `additional_days` (integer, 1-365): Days to extend the expiration

**Response:**
```json
{
  "success": true,
  "table_name": "temp_analysis",
  "message": "Expiration extended by 14 days",
  "table_info": {
    "expires_at": "2026-01-22T00:00:00Z",
    ...
  }
}
```

### Manual Cleanup Trigger

```http
POST /tables/cleanup-expired
```

Manually triggers cleanup of expired temporary tables (admin endpoint).

**Response:**
```json
{
  "success": true,
  "deleted_count": 3,
  "failed_count": 0,
  "deleted_tables": ["temp_old_1", "temp_old_2", "temp_old_3"],
  "failed_tables": []
}
```

## Configuration Options

### During Import (MappingConfig)

You can mark a table as temporary during the import process:

```json
{
  "table_name": "temp_analysis",
  "db_schema": {...},
  "mappings": {...},
  "is_temporary": true,
  "temporary_expires_days": 7,
  "temporary_allow_imports": false,
  "temporary_purpose": "Data transformation workspace"
}
```

### After Creation

Mark an existing table as temporary:

```bash
curl -X POST "https://api.example.com/tables/my_table/mark-temporary" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "expires_days": 7,
    "allow_additional_imports": false,
    "purpose": "Testing new data source"
  }'
```

## LLM/RAG Behavior

### Default Behavior (Hidden)

Temporary tables are hidden from these LLM operations:

- **List tables**: "What tables do I have?"
- **Schema discovery**: "Show me all table schemas"
- **General queries**: "Query all customer data"

### Explicit Access (Visible)

Temporary tables ARE included when explicitly mentioned:

- **Specific table requests**: "Show me the schema for temp_analysis"
- **Join operations**: "Join customers with temp_staging ON..."
- **Direct queries**: "SELECT * FROM temp_data"

### Implementation Details

The filtering happens in `app/db/context.py`:

```python
def get_table_names(
    engine: Engine,
    explicit_tables: Optional[List[str]] = None,
    include_temporary: bool = False
) -> List[str]:
    """
    Get user table names, optionally filtering temporary tables.
    
    - If explicit_tables provided: includes those tables regardless of temp status
    - If include_temporary=True: includes all temp tables
    - Otherwise: excludes temp tables
    """
```

## Examples and Workflows

### Example 1: Data Transformation Pipeline

```python
# 1. Import raw data to temporary table
POST /analyze-b2-file
{
  "file_name": "raw_data.csv",
  "analysis_mode": "auto_always",
  "mapping": {
    "table_name": "temp_raw_data",
    "is_temporary": true,
    "temporary_expires_days": 3,
    "temporary_purpose": "Raw data for transformation"
  }
}

# 2. Transform data using LLM queries
POST /query
{
  "prompt": "Create a new table 'cleaned_customers' with cleaned data from temp_raw_data, removing duplicates and standardizing phone numbers"
}

# 3. Temporary table expires automatically after 3 days
```

### Example 2: Testing a New Data Source

```python
# 1. Import test file to temporary table
POST /tables/test_import/mark-temporary
{
  "expires_days": 1,
  "allow_additional_imports": false,
  "purpose": "Testing new vendor data format"
}

# 2. Verify data quality
GET /tables/test_import

# 3. If good, convert to permanent
DELETE /tables/test_import/mark-temporary

# 4. If bad, let it expire (auto-deleted in 24 hours)
```

### Example 3: Temporary Analysis Workspace

```python
# 1. Create analysis table
POST /query
{
  "prompt": "Create temp_q1_analysis from sales data for Q1 2026"
}

# 2. Mark as temporary
POST /tables/temp_q1_analysis/mark-temporary
{
  "expires_days": 7,
  "purpose": "Q1 sales analysis workspace"
}

# 3. Perform analysis
POST /query
{
  "prompt": "Analyze trends in temp_q1_analysis grouped by region"
}

# 4. Export results, let temp table expire
```

## Import Protection

### Why Import Protection?

By default, temporary tables block additional data imports to prevent:

- Accidental data overwrites during transformation
- Mixing transformed and raw data
- Confusion about data provenance

### Blocked by Default

```python
# This will fail:
POST /map-data
{
  "mapping": {
    "table_name": "temp_staging"  # Already marked as temporary
  }
}

# Error: "Cannot import to temporary table 'temp_staging'. 
#         This table does not allow additional imports."
```

### Allowing Imports

To enable imports to a temporary table:

```python
POST /tables/temp_staging/mark-temporary
{
  "expires_days": 7,
  "allow_additional_imports": true  # <-- Enable imports
}
```

### Converting to Permanent

If you need to keep importing data, convert to permanent:

```python
DELETE /tables/temp_staging/mark-temporary
```

## Troubleshooting

### Table Not Hidden from LLM

**Problem**: Temporary table still appears in LLM queries

**Solutions**:
1. Verify table is marked as temporary:
   ```
   GET /tables/temporary
   ```

2. Check if table is explicitly mentioned in the query
   - Explicit mentions bypass hiding
   - Rephrase query to be less specific

### Cannot Import to Table

**Problem**: "Cannot import to temporary table" error

**Solutions**:
1. Enable imports for temporary table:
   ```
   POST /tables/{table_name}/mark-temporary
   { "allow_additional_imports": true }
   ```

2. Convert to permanent table:
   ```
   DELETE /tables/{table_name}/mark-temporary
   ```

3. Import to a different table

### Table Expired Too Soon

**Problem**: Temporary table was deleted before I finished working with it

**Solutions**:
1. Check expiration before it's too late:
   ```
   GET /tables/temporary
   ```

2. Extend expiration:
   ```
   POST /tables/{table_name}/extend-expiration
   { "additional_days": 14 }
   ```

3. Convert to permanent if you need it long-term:
   ```
   DELETE /tables/{table_name}/mark-temporary
   ```

### Table Not Cleaning Up

**Problem**: Expired tables not being deleted

**Solutions**:
1. Verify cleanup task is running:
   - Check application logs for "Running scheduled cleanup"
   
2. Manually trigger cleanup:
   ```
   POST /tables/cleanup-expired
   ```

3. Check for errors in cleanup logs

### Cannot Delete Temporary Table

**Problem**: Cleanup fails to delete a specific table

**Possible Causes**:
- Table has foreign key constraints
- Table is being accessed by another process
- Permissions issue

**Solutions**:
1. Check cleanup response for specific errors:
   ```
   POST /tables/cleanup-expired
   ```

2. Manually investigate:
   ```sql
   SELECT * FROM temporary_tables WHERE table_name = 'problematic_table';
   ```

3. Try manual deletion:
   ```
   DELETE /tables/problematic_table
   ```

## Database Schema

The `temporary_tables` tracking table:

```sql
CREATE TABLE temporary_tables (
    table_name VARCHAR(255) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    created_by_user_id INTEGER,
    organization_id INTEGER NOT NULL,
    allow_additional_imports BOOLEAN DEFAULT FALSE,
    purpose TEXT,
    FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE
);
```

**Indexes**:
- `idx_temporary_tables_org` on `organization_id`
- `idx_temporary_tables_expires` on `expires_at`

## Best Practices

1. **Set Appropriate Expiration**:
   - Short workflows: 1-3 days
   - Medium projects: 7 days (default)
   - Long analysis: 14-30 days
   - Maximum: 365 days

2. **Use Descriptive Purposes**:
   ```json
   { "purpose": "Q4 sales transformation pipeline - stage 2" }
   ```

3. **Convert Important Tables**:
   - If a table becomes important, convert to permanent
   - Don't rely on extending expiration repeatedly

4. **Enable Imports Carefully**:
   - Only enable when you need multiple import rounds
   - Document why imports are enabled

5. **Clean Up Regularly**:
   - Review temporary tables weekly
   - Delete unneeded tables early

## Related Documentation

- [Import Tracking](./IMPORT_TRACKING.md) - Track all data imports
- [Duplicate Detection](./DUPLICATE_DETECTION.md) - Prevent duplicate imports
- [LangChain Integration](./LANGCHAIN_INTEGRATION.md) - LLM query interface
- [API Reference](./API_REFERENCE.md) - Complete API documentation

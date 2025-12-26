# Duplicate Detection System

## Overview

The Content Atlas application now uses a **Pandas-based duplicate detection system** that is significantly more reliable, performant, and scalable than the previous row-by-row SQL approach.

## Key Features

### 1. File-Level Duplicate Detection
- **SHA-256 hash-based**: Each uploaded file is hashed to create a unique fingerprint
- **Instant detection**: Returns an error immediately if the same file is uploaded twice to the same table
- **Tracked in database**: File imports are recorded in the `file_imports` table with metadata

### 2. Row-Level Duplicate Detection
- **Pandas vectorized operations**: Uses efficient DataFrame merge operations instead of individual SQL queries
- **Configurable uniqueness columns**: Can specify which columns to check for duplicates
- **Default behavior**: Checks all columns for exact matches if no uniqueness columns specified
- **Type-aware comparison**: Properly handles different data types (integers, strings, decimals, etc.)

### 3. Large File Support (100MB+)
- **Chunked processing**: Automatically processes files >10,000 records in chunks
- **Parallel duplicate checking**: Uses multi-threading to check chunks for duplicates simultaneously
- **Memory efficient**: Processes data in manageable batches to avoid memory issues
- **Bulk insert optimization**: Uses Pandas `to_sql` with `method='multi'` for fast insertion
- **Progress tracking**: Logs chunk progress for monitoring large imports
- **Two-phase processing**: Phase 1 checks all chunks in parallel, Phase 2 inserts sequentially for data integrity

### 4. User Override Options
- **`force_import`**: Bypass all duplicate checks and force data insertion
- **`allow_duplicates`**: Skip row-level duplicate checking (still checks file-level if enabled)
- **`check_file_level`**: Enable/disable file-level duplicate detection
- **`allow_file_level_retry`**: Opt-in to re-import the same file hash (LLM/user instructed retries) while keeping row-level duplicate protection
- **Custom error messages**: Configurable error messages for duplicate detection

## Configuration

Duplicate checking is configured via the `duplicate_check` parameter in the mapping configuration:

```json
{
  "table_name": "my_table",
  "db_schema": {
    "name": "VARCHAR(255)",
    "email": "VARCHAR(255)",
    "age": "INTEGER"
  },
  "mappings": {
    "name": "name",
    "email": "email",
    "age": "age"
  },
  "duplicate_check": {
    "enabled": true,
    "check_file_level": true,
    "allow_file_level_retry": false,
    "uniqueness_columns": ["email"],  // Optional: specify which columns must be unique
    "allow_duplicates": false,
    "force_import": false,
    "error_message": "Custom error message here"
  }
}
```

## Configuration Options

### `enabled` (boolean, default: true)
- Enable or disable duplicate detection entirely
- When false, no duplicate checks are performed

### `check_file_level` (boolean, default: true)
- Enable file-level duplicate detection using SHA-256 hashing
- Prevents uploading the same file twice to the same table

### `allow_file_level_retry` (boolean, default: false)
- Allows explicit retries of the same file hash without admin privileges
- Intended for LLM- or user-directed “retry and skip duplicates” flows
- Row-level duplicate protection remains enforced; only new rows are inserted
- Use when a previous import stopped mid-way and you need to resume without double-inserting

### `uniqueness_columns` (array of strings, optional)
- Specify which columns to check for uniqueness
- If not provided, all columns are checked for exact matches
- Example: `["email"]` - only check if email already exists
- Example: `["first_name", "last_name", "birth_date"]` - check combination of these fields

### `allow_duplicates` (boolean, default: false)
- When true, skips row-level duplicate checking
- File-level checking still applies if `check_file_level` is true
- Useful for tables where duplicate rows are acceptable

### `force_import` (boolean, default: false)
- When true, bypasses ALL duplicate checks
- Forces data insertion regardless of duplicates
- Use with caution - can lead to data duplication

### `error_message` (string, optional)
- Custom error message to display when duplicates are detected
- Default: "Duplicate data detected. The uploaded data overlaps with existing records."

## How It Works

### Standard Processing (< 10,000 records)

1. **File-level check** (if enabled):
   - Calculate SHA-256 hash of file content
   - Query `file_imports` table to check if hash exists for this table
   - If found, raise `FileAlreadyImportedException`

2. **Row-level check** (if enabled and not `allow_duplicates`):
   - Apply type coercion to new records based on schema
   - Create Pandas DataFrame from new records
   - Load existing data from table (only uniqueness columns)
   - Normalize data types for proper comparison
   - Use `DataFrame.merge()` with `indicator=True` to find overlaps
   - If duplicates found, raise `DuplicateDataException`

3. **Insert records**:
   - Apply type coercion to each record
   - Insert records one by one in a transaction
   - Record file import in `file_imports` table (if file-level checking enabled)

### Chunked Processing (≥ 10,000 records)

1. **File-level check** (once upfront)
2. **Split records into chunks** of 10,000
3. **Phase 1: Parallel Duplicate Checking** (CPU-intensive)
   - Pre-load existing data once (shared across all workers)
   - Check all chunks for duplicates in parallel using ThreadPoolExecutor
   - Uses up to 4 parallel workers (based on CPU count)
   - Each worker performs vectorized Pandas merge operations
   - Aggregates results from all chunks
   - Raises exception if any duplicates found
4. **Phase 2: Sequential Insertion** (I/O-intensive)
   - For each chunk:
     - Apply type coercion to all records in chunk
     - Bulk insert using Pandas `to_sql` with `method='multi'`
     - Log progress
5. **Record file import** (after all chunks complete)

## Performance Benefits

### Before (Row-by-Row SQL)
- **10,000 records**: ~10,000 individual SQL queries
- **100,000 records**: ~100,000 individual SQL queries
- **Memory**: Low but very slow
- **Reliability**: Transaction isolation issues, type coercion mismatches

### After (Pandas-Based)
- **10,000 records**: 1 bulk query to load existing data + vectorized comparison
- **100,000 records**: 10 chunks × (1 query + vectorized comparison + bulk insert)
- **Memory**: Moderate, managed through chunking
- **Reliability**: Consistent type handling, proper transaction management

### Speed Improvements
- **Small files (< 1,000 records)**: 5-10x faster
- **Medium files (1,000-10,000 records)**: 10-50x faster
- **Large files (> 10,000 records)**: 50-100x faster with chunking
- **Very large files (> 50,000 records)**: Additional 2-4x speedup from parallel duplicate checking

## Error Handling

### FileAlreadyImportedException
- **HTTP Status**: 409 Conflict
- **When**: Same file (by hash) uploaded twice to same table
- **Message**: "File has already been imported to table '{table_name}'."

### DuplicateDataException
- **HTTP Status**: 409 Conflict
- **When**: Row-level duplicates detected
- **Message**: Configurable via `error_message` parameter
- **Details**: Includes count of duplicate records found

## Examples

### Example 1: Check All Columns for Duplicates
```json
{
  "duplicate_check": {
    "enabled": true,
    "check_file_level": true
  }
}
```
This checks if the entire row (all columns) already exists in the table.

### Example 2: Check Only Email for Uniqueness
```json
{
  "duplicate_check": {
    "enabled": true,
    "check_file_level": false,
    "allow_file_level_retry": true,
    "uniqueness_columns": ["email"]
  }
}
```
This only checks if the email already exists, allowing other fields to differ.

### Example 3: Allow Duplicate Rows, But Not Same File
```json
{
  "duplicate_check": {
    "enabled": true,
    "check_file_level": true,
    "allow_duplicates": true
  }
}
```
This prevents uploading the same file twice but allows duplicate rows.

### Example 4: Force Import (No Checks)
```json
{
  "duplicate_check": {
    "enabled": true,
    "force_import": true
  }
}
```
This bypasses all duplicate checks and forces the import.

### Example 5: Custom Error Message
```json
{
  "duplicate_check": {
    "enabled": true,
    "uniqueness_columns": ["email"],
    "error_message": "A user with this email address already exists in the system."
  }
}
```

## Database Schema

### file_imports Table
```sql
CREATE TABLE file_imports (
    id SERIAL PRIMARY KEY,
    file_hash VARCHAR(64) UNIQUE NOT NULL,
    file_name VARCHAR(500),
    table_name VARCHAR(255) NOT NULL,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_count INTEGER,
    UNIQUE(file_hash, table_name)
);
```

## Testing

All duplicate detection scenarios are covered by tests in `tests/test_api.py`:

- `test_duplicate_detection_file_level`: Tests file-level duplicate detection
- `test_duplicate_detection_row_level`: Tests row-level duplicate detection
- `test_force_import_bypasses_duplicates`: Tests force_import override
- `test_small_file_duplicate_detection`: Tests with small files

Run tests with:
```bash
pytest tests/test_api.py -k duplicate -v
```

## Best Practices

1. **Use file-level checking for data integrity**: Prevents accidental re-uploads
2. **Specify uniqueness columns when possible**: More efficient than checking all columns
3. **Use chunked processing for large files**: Automatically enabled for files > 10,000 records
4. **Provide clear error messages**: Help users understand why their upload was rejected
5. **Use force_import sparingly**: Only when you're certain duplicates are acceptable

## LLM-Driven Duplicate Resolution

When an import encounters duplicates, the system now returns enough context for an automated (LLM) flow to decide what to do—merge into the existing row, keep the existing row, or skip/new.

### Response fields
- `needs_user_input`: `true` when duplicates were detected.
- `llm_followup`: A ready-made prompt describing the duplicates and how to resolve them.
- `duplicate_rows`: Array of duplicate previews; each item contains:
  - `record`: The incoming row that was skipped.
  - `existing_row`: The matching row currently in the table (`row_id` + `record`).
  - `id`: The duplicate id used for follow-up API calls.
- `duplicate_rows_count`: Total duplicates found.
- `import_id`: Import identifier for follow-up calls.

These fields are present in:
- `/map-data` and `/map-b2-data` responses
- `/import-history/{import_id}/duplicates` (full list)
- `/import-history/{import_id}/duplicates/{duplicate_id}` (full detail)

### Auto/LLM resolution flow
1) Import runs (auto or manual) and returns duplicates with `needs_user_input=true`.
2) The LLM inspects `duplicate_rows` (each includes `record` and `existing_row`) and, if needed, fetches full detail:
   - `GET /import-history/{import_id}/duplicates/{duplicate_id}`
3) The LLM decides per duplicate:
   - Merge: update existing row with chosen fields
   - Keep existing: mark resolved without changes
   - Skip/new: leave as-is or insert a new row if desired
4) Apply the decision:
   - `POST /import-history/{import_id}/duplicates/{duplicate_id}/merge` with `updates` (fields to apply), optional `resolved_by`, and `note`.

### Merge policy recommendations
- Prefer non-null values over null.
- Avoid overwriting trusted values without an explicit reason.
- For numeric fields like revenue, prefer the higher-confidence value (typically the non-zero or most recent depending on your business rule).
- Keep the uniqueness key stable (e.g., the email used for matching).

### Testing
- Duplicate flow: `python -m pytest tests/test_api.py::test_duplicate_detection_row_level -q`
  - Asserts duplicates are flagged, preview includes `existing_row`, and follow-up prompt is present.

## Future Enhancements

Potential improvements for future versions:

1. **Partial import strategy**: Allow importing non-duplicate rows while rejecting duplicates
2. **Duplicate resolution UI**: Let users choose how to handle duplicates interactively
3. **Configurable chunk size**: Allow users to specify chunk size based on their needs
4. **Duplicate reporting**: Provide detailed report of which rows are duplicates
5. **Merge strategies**: Options to update existing rows instead of rejecting duplicates

# System Table Protection

## Overview

Content Atlas implements security measures to prevent the LLM-powered query interface from accessing operational system tables. This ensures that sensitive operational data remains protected while allowing users to query their business data freely.

## Protected Tables

The following system tables are protected from LLM access and must never be listed, queried, or exposed in the frontend:

- `import_history` - Tracks file import operations
- `mapping_errors` - Stores data mapping errors
- `table_metadata` - Contains table metadata and descriptions
- `uploaded_files` - Records uploaded file information
- `users` - User account information
- `file_imports` - File import tracking with hashes
- `import_jobs` - Tracks background import job progress and metadata
- `import_duplicates` - Duplicate detection audit log
- `query_messages` - Stored LLM conversation messages
- `query_threads` - LLM conversation thread metadata
- `llm_instructions` - Custom LLM instructions and prompts

> 💡 **User uploads never overwrite these tables.** When a mapping request
> specifies a reserved table name (for example, `users`), the backend
> automatically remaps it to a safe alternative such as `users_user_data`
> before any DDL/DML happens. This keeps system state protected while
> still letting customers import similarly named business data.

## Implementation

### Two-Layer Protection

1. **Schema Context Filtering** (`app/db_context.py`)
   - System tables are excluded from the database schema provided to the LLM
   - The LLM never sees these tables in its context, reducing the likelihood of attempted access

2. **Query Execution Validation** (`app/query_agent.py`)
   - Even if the LLM attempts to query a system table, the `execute_sql_query` tool blocks it
   - Provides defense-in-depth security

### Validation Logic

The `execute_sql_query` tool performs the following checks:

```python
# Check for protected system tables
sql_upper = sql_query.upper()
for table in PROTECTED_SYSTEM_TABLES:
    # Check for table references in various SQL contexts
    table_patterns = [
        rf'\bFROM\s+["\']?{table.upper()}["\']?\b',
        rf'\bJOIN\s+["\']?{table.upper()}["\']?\b',
        rf'\bFROM\s+PUBLIC\.{table.upper()}\b',
        rf'\bJOIN\s+PUBLIC\.{table.upper()}\b'
    ]
    
    for pattern in table_patterns:
        if re.search(pattern, sql_upper):
            return f"ERROR: Access to system table '{table}' is not allowed."
```

### Detection Capabilities

The protection detects system table references in:

- Direct queries: `SELECT * FROM users`
- Quoted identifiers: `SELECT * FROM "users"`
- Schema-qualified names: `SELECT * FROM public.users`
- JOIN clauses: `JOIN users ON ...`
- Case-insensitive variations: `USERS`, `Users`, `uSeRs`

## Error Messages

When a protected table is accessed, users receive a clear error message:

```
ERROR: Access to system table 'users' is not allowed. This table contains operational data and is protected for security reasons.
```

## Testing

Comprehensive tests verify the protection mechanism:

```bash
python -m pytest tests/test_system_table_protection.py -v
```

Test coverage includes:
- Direct table queries
- Quoted identifiers
- Schema-qualified names
- JOIN operations
- Case-insensitive matching
- Multiple system tables in one query
- Verification that normal tables are not blocked

## Adding New Protected Tables

To protect additional tables:

1. Add the table name to `PROTECTED_SYSTEM_TABLES` in `app/query_agent.py`:
   ```python
   PROTECTED_SYSTEM_TABLES = {
       'api_keys',
       'import_history',
       'mapping_errors',
       'table_metadata',
       'uploaded_files',
       'users',
       'file_imports',
       'import_jobs',
       'import_duplicates',
       'query_messages',
       'query_threads',
       'your_new_table'  # Add here
   }
   ```

2. Add the table to the exclusion list in `app/db_context.py`:
   ```python
   AND table_name NOT IN ('spatial_ref_sys', ..., 'your_new_table')
   ```

3. Update the test in `tests/test_system_table_protection.py`:
   ```python
   expected_tables = {
       'import_history',
       # ... other tables
       'your_new_table'
   }
   ```

4. Run tests to verify:
   ```bash
   python -m pytest tests/test_system_table_protection.py -v
   ```

## Security Considerations

### Why Two Layers?

1. **Schema Filtering (First Layer)**
   - Reduces attack surface by not exposing table names
   - Prevents accidental queries due to LLM hallucination
   - More efficient - prevents unnecessary query attempts

2. **Query Validation (Second Layer)**
   - Defense-in-depth security principle
   - Protects against edge cases where LLM might learn table names
   - Provides explicit error messages for debugging

### Limitations

This protection does not prevent:
- Direct database access by users with credentials
- SQL injection through other application endpoints
- Access through database administration tools

It specifically protects the natural language query interface from accessing operational tables.

## Related Documentation

- [Query Agent Architecture](ARCHITECTURE.md#query-agent)
- [Database Context Management](ARCHITECTURE.md#database-context)
- [Security Best Practices](DEPLOYMENT.md#security)

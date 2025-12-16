# Database Reset for Development

This document describes how to reset the database for testing purposes while preserving user accounts.

## Overview

The database reset functionality allows you to quickly clear all data and start fresh during development, without losing your test user accounts. This is particularly useful when:

- Testing data import workflows
- Debugging duplicate detection logic
- Resetting to a clean state between test runs
- Clearing out test data before a demo

## What Gets Reset

### Deleted Items
- **All user-created data tables** (e.g., `contacts`, `products`, `sales`, etc.)
- **Tracking tables** (cleared but structure preserved):
  - `file_imports` - File import history
  - `table_metadata` - Table metadata records
  - `import_history` - Import operation logs
  - `import_jobs` - Background import job tracking
  - `uploaded_files` - Uploaded file tracking
- **B2 Storage files** - All files in the `uploads/` folder

### Preserved Items
- **Users table** - Your login accounts remain intact
- **Database structure** - System tables are preserved

## Usage

### Interactive Mode (Recommended)

```bash
python reset_dev_db.py
```

This will:
1. Show what will be reset
2. Display the database environment (development/production)
3. Ask for confirmation by typing "RESET"
4. Execute the reset
5. Show a summary of what was deleted

### Auto-Confirm Mode (For Scripts)

```bash
python reset_dev_db.py --yes
```

Skips the confirmation prompt. Useful for automation or CI/CD pipelines.

### Force Production Reset (DANGEROUS!)

```bash
python reset_dev_db.py --force-production --yes
```

⚠️ **WARNING**: This allows resetting a production database. This is **EXTREMELY DANGEROUS** and will cause **PERMANENT DATA LOSS**. Only use this if you absolutely know what you're doing.

## Safety Features

### 1. Production Environment Detection

The script automatically detects production environments by checking the database URL for:
- `production` or `prod` keywords
- AWS RDS endpoints (`rds.amazonaws.com`)
- Azure database endpoints
- Other cloud database indicators

If a production environment is detected, the script will refuse to run unless you use the `--force-production` flag.

### 2. Confirmation Prompts

- **Development**: Requires typing "RESET" to confirm
- **Production**: Requires typing "I UNDERSTAND THE RISKS" to confirm

### 3. Transaction Safety

All database operations are wrapped in a transaction. If any error occurs during the reset, the transaction is rolled back to prevent partial resets.

### 4. B2 Cleanup Separation

B2 file deletion happens **after** successful database reset. If the database reset fails, B2 files are not deleted.

## Example Output

```
================================================================================
DATABASE RESET UTILITY - DEVELOPMENT ONLY
================================================================================

Database: localhost:5432/data_mapper
Environment: DEVELOPMENT

⚠️  WARNING: This will reset the following:
   • All user-created data tables (contacts, products, etc.)
   • Tracking tables (file_imports, table_metadata, import_history, import_jobs, uploaded_files)
   • All files in B2 storage (uploads folder)

✓  The following will be PRESERVED:
   • Users table (your login accounts)

To confirm, type 'RESET' (in capital letters): RESET

🔄 Starting database reset...
--------------------------------------------------------------------------------

✅ Database reset completed!
--------------------------------------------------------------------------------

📋 Dropped 3 user tables:
   • contacts
   • products
   • sales

🗑️  Truncated 4 tracking tables:
   • file_imports
   • table_metadata
   • import_history
   • uploaded_files

☁️  Deleted 12 files from B2 storage

================================================================================
✅ Reset complete! Your user accounts are preserved.
================================================================================
```

## Programmatic Usage

You can also use the reset functionality programmatically in Python:

```python
from app.db_reset import reset_database_data, ProductionEnvironmentError

try:
    results = reset_database_data(force_production=False)
    
    print(f"Tables dropped: {len(results['tables_dropped'])}")
    print(f"Tables truncated: {len(results['tables_truncated'])}")
    print(f"B2 files deleted: {results['b2_files_deleted']}")
    
    if results['errors']:
        print(f"Errors: {results['errors']}")
        
except ProductionEnvironmentError as e:
    print(f"Cannot reset production: {e}")
```

## Troubleshooting

### "B2 not configured" Warning

If you see this warning, it means your B2 credentials are not set in the `.env` file. The database will still be reset, but B2 files won't be deleted.

To fix:
1. Add B2 credentials to `.env`:
   ```
   STORAGE_ACCESS_KEY_ID=your_key_id
   STORAGE_SECRET_ACCESS_KEY=your_key
   STORAGE_BUCKET_NAME=your_bucket
   STORAGE_ENDPOINT_URL=https://s3.us-west-004.backblazeb2.com
   STORAGE_PROVIDER=b2
   ```

### "Production environment detected" Error

This is a safety feature. If you're in development but seeing this error, check your `DATABASE_URL` in `.env`. It may contain keywords like "prod" or "production".

To override (only if you're sure):
```bash
python reset_dev_db.py --force-production --yes
```

### Partial Reset Failure

If the reset fails partway through, the database transaction will be rolled back. However, if B2 files were already deleted, they cannot be recovered. Always ensure you have backups of important data.

## Best Practices

1. **Always use interactive mode** unless you're automating tests
2. **Never use in production** without a complete backup
3. **Test the reset** on a copy of your database first
4. **Document your test data** so you can recreate it after reset
5. **Use version control** for any test data scripts

## Integration with Testing

You can integrate the reset into your test suite:

```python
# tests/conftest.py
import pytest
from app.db_reset import reset_database_data

@pytest.fixture(scope="session", autouse=True)
def reset_database():
    """Reset database before running tests."""
    reset_database_data(force_production=False)
    yield
```

## Related Documentation

- [Testing Guide](TESTING.md) - How to write and run tests
- [Setup Guide](SETUP.md) - Initial database setup
- [Architecture](ARCHITECTURE.md) - Database schema overview

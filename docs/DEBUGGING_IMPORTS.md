# Debugging Import Failures

This guide explains how to debug failed imports using the enhanced `investigate_import_failure.py` script.

## Quick Start

### Search for a specific file
```bash
python investigate_import_failure.py "Marketing Agency"
```

### Show recent failures overview
```bash
python investigate_import_failure.py
```

## Understanding the Three Data Sources

Import tracking in Content Atlas uses three separate systems:

| System | Purpose | When Created | API |
|--------|---------|--------------|-----|
| **uploaded_files** | Tracks file lifecycle | When file is uploaded | `/uploaded-files` |
| **import_jobs** | Tracks job execution | When analysis/mapping starts | `/import-jobs` |
| **import_history** | Tracks data imports | When data insertion begins | `/import-history` |

### Why a file might not appear in import_history

If a file fails during:
- **Analysis stage** - Only `uploaded_files` and `import_jobs` will have records
- **Mapping stage** - Only `uploaded_files` and `import_jobs` will have records
- **Execution stage** - All three systems will have records

This is why the old script (which only queried `import_history`) missed files that failed before data insertion.

## Script Output Sections

### 1. FILE Information
Shows the uploaded file details:
- Status (uploaded, mapping, mapped, failed)
- File size and upload timestamp
- Associated job ID and status
- Error message (if any)

### 2. JOB Information
Shows import job details:
- Job status (running, waiting_user, succeeded, failed)
- Current stage (analysis, planning, execution)
- Progress percentage
- Error message with full SQL/Python traceback
- Job metadata (including LLM decisions if stored)

### 3. IMPORT HISTORY
Shows actual import records (if execution reached this stage):
- Rows processed, inserted, skipped
- Duplicates found
- Mapping errors count
- Duration and performance metrics

### 4. MAPPING ERRORS
Shows detailed mapping errors (if any):
- Error type (e.g., type_mismatch, datetime_conversion)
- Source and target fields
- Problematic values
- Error messages

## Common Error Patterns

### Type Mismatch Errors
```
ERROR: invalid input syntax for type integer: "United States"
```
**Cause**: LLM mapped a text field to an INTEGER column

**Solution**: 
- Check the LLM's column mapping decision in job metadata
- Verify expected_column_types in the LLM decision
- Review the actual data types in the source file

### Duplicate Detection Failures
```
ERROR: Duplicate check columns ['email'] not found on table
```
**Cause**: Uniqueness columns don't exist in target schema

**Solution**:
- Check column_mapping in the LLM decision
- Verify the target table schema
- Ensure column names match after mapping

### Missing Required Fields
```
ERROR: null value in column "required_field" violates not-null constraint
```
**Cause**: Required column has NULL values

**Solution**:
- Check if source data has the required field
- Review the column mapping
- Consider adding default values or transformations

## Where LLM Decisions Are Stored

Currently, LLM prompts and responses are stored in:

1. **Job metadata** (`import_jobs.metadata` JSONB column)
   - Contains some LLM decisions
   - Limited by what was explicitly saved

2. **Interactive sessions** (in-memory only)
   - Lost after job completes
   - Not persisted to database

### Future Enhancement

To improve debugging, consider storing:
- Full LLM conversation history in job metadata
- Analysis iterations and refinements
- User confirmations and modifications

## API Endpoints Reference

### Uploaded Files
```bash
# List all files
curl http://localhost:8000/uploaded-files?limit=50

# Get specific file
curl http://localhost:8000/uploaded-files/{file_id}
```

### Import Jobs
```bash
# List all jobs
curl http://localhost:8000/import-jobs?limit=50

# Get jobs for specific file
curl http://localhost:8000/import-jobs?file_id={file_id}

# Get specific job
curl http://localhost:8000/import-jobs/{job_id}
```

### Import History
```bash
# List all imports
curl http://localhost:8000/import-history?limit=50

# Get specific import
curl http://localhost:8000/import-history/{import_id}

# Get mapping errors
curl http://localhost:8000/import-history/{import_id}/mapping-errors
```

## Tips for Debugging

1. **Start with the file name** - Use the script to search by partial file name
2. **Check job metadata** - Contains LLM decisions and analysis results
3. **Look for error_message** - Available in both jobs and files
4. **Check job stage** - Tells you how far the import progressed
5. **Review mapping errors** - Shows specific data quality issues

## Example Investigation Workflow

```bash
# 1. Search for the problematic file
python investigate_import_failure.py "Marketing"

# 2. Note the file_id and job_id from output

# 3. Query the job API directly for full metadata
curl http://localhost:8000/import-jobs/{job_id}

# 4. Check import history if execution started
curl http://localhost:8000/import-history?file_name=Marketing

# 5. Review mapping errors if available
curl http://localhost:8000/import-history/{import_id}/mapping-errors
```

## See Also

- [Import Tracking Documentation](./IMPORT_TRACKING.md)
- [API Reference](./API_REFERENCE.md)
- [Architecture Overview](./ARCHITECTURE.md)

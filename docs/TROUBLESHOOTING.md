# Troubleshooting Guide

This guide provides solutions for common issues, error codes, and performance bottlenecks encountered when using the Content Atlas.

## Table of Contents
- [Diagnostic Workflow](#diagnostic-workflow)
- [Error Codes Reference](#error-codes-reference)
- [Ingestion Problems](#ingestion-problems)
- [Schema & Mapping Conflicts](#schema--mapping-conflicts)
- [Performance Tuning](#performance-tuning)

---

## Diagnostic Workflow

When you encounter an issue, follow these steps:

1.  **Check the HTTP Status Code**: Is it a 4xx (Client Error) or 5xx (Server Error)?
2.  **Read the Error Detail**: The API returns JSON with a `detail` field explaining the error.
3.  **Check Application Logs**:
    ```bash
    docker-compose logs -f --tail=100 api
    ```
    Look for "ERROR" or "WARNING" lines corresponding to your request time.
4.  **Verify Input Data**: Ensure your file matches the [Data Models](DATA_MODELS.md) requirements.

---

## Error Codes Reference

| Code | Error Type | Likely Cause | Solution |
| :--- | :--- | :--- | :--- |
| **400** | Bad Request | Invalid JSON in `mapping_json` or malformed file. | Validate JSON syntax. Ensure file is not empty. |
| **401** | Unauthorized | Missing or invalid API Key/Session. | Check headers. Log in again. |
| **403** | Forbidden | Insufficient permissions. | Request Admin access if trying to force imports. |
| **404** | Not Found | Table does not exist. | Create the table first or check spelling. |
| **409** | Conflict | Duplicate file or data. | See [Ingestion Problems](#ingestion-problems). |
| **413** | Payload Too Large | File exceeds limit (100MB). | Use async upload or increase `UPLOAD_MAX_FILE_SIZE_MB`. |
| **422** | Unprocessable | Schema validation failed. | Fix data types in source file (e.g., text in int column). |
| **500** | Server Error | Unhandled exception. | Check server logs. Retry if transient. |
| **503** | Unavailable | System overloaded or starting up. | Retry with backoff. |
| **504** | Timeout | Processing took too long. | Use async endpoints for large files. |

---

## Ingestion Problems

### "File already imported" (409)
*   **Cause**: The system calculates a SHA-256 hash of every uploaded file. If you upload the exact same file again, it is rejected to prevent duplication.
*   **Solution**: 
    1.  If accidental: Do nothing; data is already there.
    2.  If intentional (retry): Add `duplicate_check.allow_file_level_retry = true` to your mapping config.

### "Duplicate data detected" (409)
*   **Cause**: Row-level deduplication found records that match existing data based on your `unique_columns` configuration.
*   **Solution**:
    1.  **Skip Duplicates**: The system skips duplicates automatically if configured, but throws 409 if the *check* fails or strict mode is on.
    2.  **Force Import**: Set `duplicate_check.force_import = true` (Admin only) to bypass checks.

### "Encoding Error" or Garbled Text
*   **Cause**: File is not UTF-8 encoded (e.g., Windows-1252 from Excel).
*   **Solution**: Save your CSV as "CSV UTF-8 (Comma delimited)" in Excel before uploading.

---

## Schema & Mapping Conflicts

### "Column not found"
*   **Context**: Occurs during mapping validation.
*   **Cause**: The `mappings` config references a source column name that doesn't exist in the file headers.
*   **Fix**: Check for typos, extra spaces, or case sensitivity in your mapping JSON vs CSV header.

### "Type Mismatch" / "Value Error"
*   **Context**: Data validation.
*   **Cause**: You mapped a column to `INTEGER` but the file contains "N/A", "Unknown", or decimal values.
*   **Fix**: 
    1.  Clean the data source.
    2.  Change the schema type to `TEXT` or `VARCHAR` to accept any input.
    3.  Use **Transformation Rules** (e.g., regex) to clean the data during import.

---

## Performance Tuning

### Slow Imports (>60 seconds)
*   **Diagnosis**: 
    *   If it hangs at "Parsing": File is too large for synchronous processing.
    *   If it hangs at "Checking Duplicates": Database is slow or table is huge.
*   **Tuning**:
    *   **Use Async**: Switch to the asynchronous import endpoints for files >50k rows.
    *   **Parallelism**: The system automatically uses up to 4 workers. Ensure your server has 4+ vCPUs.

### Memory Issues (OOM)
*   **Symptoms**: Container crashes during upload.
*   **Cause**: Loading entire 100MB+ file into memory.
*   **Fix**: 
    *   Increase Docker memory limit (`--memory=4g`).
    *   Reduce `CHUNK_SIZE` in configuration (requires code change currently).

### Optimization: Caching
*   **Feature**: The system caches parsed records for 5 minutes based on file hash.
*   **Tip**: If you fail a mapping (e.g., wrong column name), re-submitting the same file with corrected mapping is **much faster** because the system skips the expensive file parsing step.

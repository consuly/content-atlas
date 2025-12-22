# Operational Guide

This guide covers best practices for operating the Content Atlas platform, including handling large datasets, error recovery, and leveraging AI for complex data mappings.

## Table of Contents
- [Handling Large Files](#handling-large-files)
- [Retry Patterns & Resilience](#retry-patterns--resilience)
- [Error Handling](#error-handling)
- [AI Assistant Best Practices](#ai-assistant-best-practices)

---

## Handling Large Files

The system is optimized to handle large datasets, but following these best practices ensures stability and performance.

### Automatic Chunking
For files larger than **10,000 records**, the system automatically switches to a parallel processing mode:
1.  **Phase 0**: Maps data in parallel using available CPU cores.
2.  **Phase 1**: Checks for duplicates in parallel against the database.
3.  **Phase 2**: Inserts validated chunks sequentially to ensure consistency.

### Best Practices
*   **Async Processing**: For files exceeding **50,000 records**, prefer using the async endpoints (e.g., `/api/import/async` or via the Console's background mode). This prevents HTTP timeouts.
*   **Memory limits**: Ensure your Docker container has at least **4GB RAM** if you regularly process files >100MB. The chunking system keeps memory usage stable, but the initial file parse requires headroom.
*   **Upload Limits**: Configure `UPLOAD_MAX_FILE_SIZE_MB` in your `.env` if you need to support files larger than the default 100MB.

---

## Retry Patterns & Resilience

### Client-Side Retries
Clients integration with the API should implement **exponential backoff** for the following status codes:
*   `503 Service Unavailable`: The system might be restarting or overloaded.
*   `429 Too Many Requests`: You have hit the API rate limit.
*   `504 Gateway Timeout`: The request took too long (switch to async processing).

### Handling Import Failures
Import jobs are **atomic per chunk** but generally fail fast if a critical error occurs.

1.  **Duplicate File Error (409)**: 
    *   **Cause**: You uploaded a file with the exact same content (SHA-256 hash) as a previous import.
    *   **Fix**: If this is intentional (e.g., re-importing after cleanup), you must enable `allow_file_level_retry` in the configuration or use an Admin account to force the import.
    
2.  **Validation Error (422)**:
    *   **Cause**: Data type mismatch that couldn't be coerced (e.g., "ABC" in an Integer column).
    *   **Fix**: Fix the source file and retry. The system does *not* partially import invalid rows to maintain data integrity.

---

## Error Handling

### Common Status Codes

| Code | Meaning | Action |
| :--- | :--- | :--- |
| **200** | Success | Process result. |
| **400** | Bad Request | Check your JSON syntax or missing fields. |
| **401** | Unauthorized | Check your API Key or Session. |
| **403** | Forbidden | You don't have permission (e.g., non-admin trying to force import). |
| **404** | Not Found | Table or Resource ID does not exist. |
| **409** | Conflict | Duplicate file or data detected. Check `detail` message. |
| **422** | Unprocessable | Schema validation failed. Check `detail` for specific field errors. |
| **500** | Server Error | Check application logs. Report bug if persistent. |

### Interpreting Error Details
The API returns structured error details when possible:

```json
{
  "detail": "Duplicate data detected. 150 records overlap with existing data."
}
```

Or for validation errors:
```json
{
  "detail": [
    {
      "loc": ["body", "mapping_json"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

---

## AI Assistant Best Practices

The Content Atlas uses an LLM Agent to help with Schema Mapping and SQL Generation. Here is how to get the best results.

### 1. Complex Mappings
When the auto-mapper fails to guess the right column (e.g., mapping "Q3-23" to "Revenue"), use **LLM Instructions**.

*   **Mechanism**: You can persist "Instructions" in the system (via the `llm_instructions` table/API) that guide the agent.
*   **Example Instruction**: "Always map columns starting with 'Q' followed by a number to the 'quarterly_revenue' table."
*   **Result**: The agent retrieves relevant instructions based on the file context and applies them during the mapping phase.

### 2. Ambiguous Column Names
If your CSV has columns like `Val1`, `Val2`, the AI might guess wrong.
*   **Fix**: Rename header columns in the source file to be more descriptive (e.g., `Sales_Value`, `Tax_Value`) *before* upload.
*   **Alternative**: Provide an explicit `db_schema` hint in the API call to force the types, which helps the AI infer the meaning.

### 3. Iterative Refinement
1.  Run `POST /detect-b2-mapping` (or upload in UI) to see the AI's proposed mapping.
2.  Review the JSON response.
3.  Modify the `mappings` dictionary manually if needed.
4.  Submit the final `map-data` request with your corrected config.
    *   *Note: The AI learns from the Schema Templates you use, but explicit instructions are more reliable for edge cases.*

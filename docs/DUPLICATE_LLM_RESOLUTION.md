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

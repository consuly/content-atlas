# Row-Level Transformations

Pre-processing rules that reshape rows before column mapping. These run in pandas for speed, before in-file dedupe and mapping, and preserve `_source_record_number` for error traces.

## How to configure

- Set `mapping_config.rules.row_transformations` to a list of transformation objects.
- Supported types:
  - `explode_columns`: Duplicate a row once per populated source column.
    - `source_columns` (list, required)
    - `target_column` (required)
    - `drop_source_columns` (default: true)
    - `include_original_row`, `keep_empty_rows`, `dedupe_values`, `case_insensitive_dedupe`, `strip_whitespace`
  - `filter_rows`: Keep/drop rows matching regex patterns.
    - `include_regex` (optional), `exclude_regex` (optional), `columns` (optional; defaults to non-helper columns)
  - `regex_replace`: Regex-based cleaning with capture support.
    - `pattern` (required), `columns` (required), `replacement` (optional, default `""`)
    - `outputs` (optional list) to capture groups into named columns (`group`/`index`, `name`, `default`)
    - `skip_on_no_match` (optional) to leave values untouched when no match
  - `conditional_transform`: Gate nested row actions by regex.
    - `include_regex`/`exclude_regex`, `columns`
    - `actions`: list of other row transforms (e.g., `explode_columns`, `filter_rows`, `regex_replace`)

## Column-level regex (mapping stage)

`column_transformations` also supports `regex_replace`:
- `source_column` (required), `pattern` (required)
- `target_column` (optional; defaults to source)
- `replacement` (default `""`)
- `outputs` (optional) to map capture groups into multiple columns
- `skip_on_no_match` (optional) to leave values untouched on miss

## Notes

- Row transformations run before deduplication and mapping (both streaming and batch paths).
- Errors are recorded with the original record number where possible.
- Helper keys (e.g., `_source_record_number`) are ignored for uniqueness detection and are stripped before insert.

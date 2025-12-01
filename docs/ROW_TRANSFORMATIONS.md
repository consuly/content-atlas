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
  - `explode_list_rows`: Explode a list-like column into multiple rows (pandas `explode` style).
    - `source_column` (required), `target_column` (defaults to source)
    - `delimiter` (optional; defaults to comma/semicolon split), `strip_whitespace` (default: true)
    - `dedupe_values` (default: true), `case_insensitive_dedupe` (default: true)
    - `drop_source_column` (default: true), `include_original_row` (default: false), `keep_empty_rows` (default: false)
  - `concat_columns`: Merge multiple columns into a single string on the row.
    - `sources` (required), `target_column` (required)
    - `separator` (default: `" "`), `strip_whitespace` (default: true)
    - `skip_nulls` (default: true; when false, `null_replacement` is used), `null_replacement` (default: `""`)

## Column-level regex (mapping stage)

`column_transformations` also supports `regex_replace`:
- `source_column` (required), `pattern` (required)
- `target_column` (optional; defaults to source)
- `replacement` (default `""`)
- `outputs` (optional) to map capture groups into multiple columns
- `skip_on_no_match` (optional) to leave values untouched on miss

Additional column-level helpers:
- `merge_columns`: Concatenate multiple sources into one column (same options as `concat_columns` above).
- `explode_list_column`: Split a list-like value into fixed output columns without duplicating rows.
  - `source_column` (required), `outputs` (required, each with `name` and optional `index`/`default`)
  - `delimiter` (optional), `strip_whitespace` (default: true)
  - `dedupe_values` (default: true), `case_insensitive_dedupe` (default: true)

## Notes

- Row transformations run before deduplication and mapping (both streaming and batch paths).
- Errors are recorded with the original record number where possible.
- Helper keys (e.g., `_source_record_number`) are ignored for uniqueness detection and are stripped before insert.

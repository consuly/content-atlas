from typing import Any, Dict, List, Tuple, Optional
from dataclasses import dataclass, field
import logging
import re
import json
import pandas as pd

from app.api.schemas.shared import MappingConfig
from app.utils.phone import standardize_phone

logger = logging.getLogger(__name__)


@dataclass
class TransformationStats:
    """Statistics about row-level transformations."""
    input_rows: int = 0
    output_rows: int = 0
    rows_with_no_expansion: int = 0
    rows_dropped_by_filters: int = 0
    source_rows_with_no_output: List[int] = field(default_factory=list)
    
    @property
    def expansion_ratio(self) -> float:
        """Calculate the expansion ratio (output/input)."""
        return self.output_rows / self.input_rows if self.input_rows > 0 else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary for serialization."""
        return {
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "rows_with_no_expansion": self.rows_with_no_expansion,
            "rows_dropped_by_filters": self.rows_dropped_by_filters,
            "expansion_ratio": self.expansion_ratio,
            "source_rows_with_no_output": self.source_rows_with_no_output[:100],  # Limit to first 100
        }


def apply_row_transformations(
    records: List[Dict[str, Any]],
    mapping_config: MappingConfig,
    *,
    row_offset: int = 0,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], TransformationStats]:
    """
    Apply row-level transformations (pandas-backed) before column mapping.

    Returns the transformed records, any structured errors encountered while
    preparing the rows, and statistics about the transformations.
    """
    rules = mapping_config.rules or {}
    transformations = rules.get("row_transformations") or []
    
    # Initialize stats
    stats = TransformationStats(input_rows=len(records))
    
    if not transformations or not records:
        stats.output_rows = len(records)
        return records, [], stats

    transformed = records
    all_errors: List[Dict[str, Any]] = []

    # Some row transformations (e.g., explode_columns) depend on helper columns
    # produced by column-level rules such as split_multi_value_column. Because
    # row transforms run before map_data applies column transformations, we
    # materialize the subset of column transformations that generate additional
    # fields so they are available here.
    helper_col_xforms = []
    column_transformations = rules.get("column_transformations") or []
    if column_transformations:
        helper_col_xforms = [
            ct
            for ct in column_transformations
            if isinstance(ct, dict) and ct.get("type") in {"split_multi_value_column", "explode_list_column"}
        ]
    if helper_col_xforms:
        from app.domain.imports.mapper import _apply_column_transformations  # Local import to avoid cycles

        transformed = [
            _apply_column_transformations(record, helper_col_xforms) or record
            for record in transformed
        ]

    priority_order = ("explode_columns", "explode_list_rows")
    ordered = [
        t for t in transformations if isinstance(t, dict) and t.get("type") in priority_order
    ] + [
        t for t in transformations if not isinstance(t, dict) or t.get("type") not in priority_order
    ]

    exploded_targets = set()
    consumed_sources: set[str] = set()

    for transformation in ordered:
        if not isinstance(transformation, dict):
            continue
        t_type = transformation.get("type")
        if t_type == "explode_columns":
            target = (
                transformation.get("target_column")
                or transformation.get("target_field")
                or transformation.get("column")
            )
            sources_declared = transformation.get("source_columns") or transformation.get("columns") or []
            drop_source = transformation.get("drop_source_columns", True)
            if drop_source and sources_declared and all(src in consumed_sources for src in sources_declared):
                # Sources already consumed by a previous explode; skip without error to avoid false mapping errors.
                continue
            if target and target in exploded_targets:
                all_errors.append(
                    {
                        "type": "row_transformation",
                        "message": f"explode_columns skipped for '{target}': target already exploded earlier in pipeline",
                    }
                )
                continue
            transformed, errors, explode_stats = _apply_explode_columns(
                transformed,
                transformation,
                row_offset=row_offset,
            )
            all_errors.extend(errors)
            
            # Accumulate expansion stats from explode_columns
            if explode_stats.get("rows_with_no_expansion", 0) > 0:
                stats.rows_with_no_expansion += explode_stats["rows_with_no_expansion"]
                stats.source_rows_with_no_output.extend(explode_stats.get("source_rows_with_no_output", []))
            if target:
                exploded_targets.add(target)
            if drop_source and sources_declared:
                consumed_sources.update(sources_declared)
        elif t_type == "filter_rows":
            transformed, errors = _apply_filter_rows(
                transformed,
                transformation,
                row_offset=row_offset,
            )
            all_errors.extend(errors)
        elif t_type == "regex_replace":
            transformed, errors = _apply_regex_replace(
                transformed,
                transformation,
                row_offset=row_offset,
            )
            all_errors.extend(errors)
        elif t_type == "conditional_transform":
            transformed, errors = _apply_conditional_transform(
                transformed,
                transformation,
                row_offset=row_offset,
            )
            all_errors.extend(errors)
        elif t_type == "explode_list_rows":
            transformed, errors = _apply_explode_list_rows(
                transformed,
                transformation,
                row_offset=row_offset,
            )
            all_errors.extend(errors)
        elif t_type == "concat_columns":
            transformed, errors = _apply_concat_columns(
                transformed,
                transformation,
                row_offset=row_offset,
            )
            all_errors.extend(errors)
        elif t_type == "drop_columns":
            transformed, errors = _apply_drop_columns(
                transformed,
                transformation,
                row_offset=row_offset,
            )
            all_errors.extend(errors)
        elif t_type == "require_any_of":
            transformed, errors = _apply_require_any_of(
                transformed,
                transformation,
                row_offset=row_offset,
            )
            all_errors.extend(errors)
        elif t_type == "standardize_phone":
            transformed, errors = _apply_standardize_phone(
                transformed,
                transformation,
                row_offset=row_offset,
            )
            all_errors.extend(errors)
        else:
            all_errors.append(
                {
                    "type": "row_transformation",
                    "message": f"Unknown row transformation type '{t_type}'",
                    "record_number": None,
                }
            )

    # Update final stats
    stats.output_rows = len(transformed)
    
    return transformed, all_errors, stats


def _apply_explode_columns(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Duplicate each row once per populated source column and place the value
    into a single target column.
    
    Returns transformed records, errors, and expansion stats.
    """
    errors: List[Dict[str, Any]] = []
    expansion_stats = {
        "input_rows": len(records),
        "output_rows": 0,
        "rows_with_no_expansion": 0,
        "source_rows_with_no_output": []
    }
    source_columns = transformation.get("source_columns") or transformation.get("columns") or []
    target_column = (
        transformation.get("target_column")
        or transformation.get("target_field")
        or transformation.get("column")
    )
    drop_source = transformation.get("drop_source_columns", True)
    include_original = transformation.get("include_original_row", False)
    keep_empty_rows = transformation.get("keep_empty_rows", False)
    dedupe_values = transformation.get("dedupe_values", True)
    strip_whitespace = transformation.get("strip_whitespace", True)
    case_insensitive_dedupe = transformation.get("case_insensitive_dedupe", True)

    if not source_columns or not target_column:
        errors.append(
            {
                "type": "row_transformation",
                "message": "explode_columns requires source_columns and target_column",
            }
        )
        return records, errors, expansion_stats

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return [], errors, expansion_stats

    if "_source_record_number" not in df.columns:
        df["_source_record_number"] = [row_offset + idx + 1 for idx in range(len(df))]

    missing_sources = [col for col in source_columns if col not in df.columns]
    present_sources = [col for col in source_columns if col in df.columns]
    if not present_sources:
        errors.append(
            {
                "type": "row_transformation",
                "message": f"explode_columns skipped: none of the requested source columns exist ({source_columns})",
            }
        )
        return records, errors
    if missing_sources:
        for col in missing_sources:
            df[col] = None
        if not transformation.get("ignore_missing_sources", True):
            for col in missing_sources:
                errors.append(
                    {
                        "type": "row_transformation",
                        "message": f"Source column '{col}' missing for explode_columns",
                        "column": col,
                    }
                )

    value_df = df[present_sources].copy()
    value_df = value_df.map(lambda v: _normalize_value(v, strip_whitespace))
    # future_stack enables the upcoming stack behavior and silences pandas deprecation warnings.
    # Drop NA values manually because dropna cannot be combined with future_stack=True.
    stacked = value_df.stack(future_stack=True).reset_index()
    stacked = stacked.dropna(subset=[0])
    
    # Track which source rows produced output
    rows_with_output = set(stacked["level_0"].tolist()) if not stacked.empty else set()
    all_row_indices = set(range(len(df)))
    rows_with_no_output = all_row_indices - rows_with_output
    
    # Update expansion stats
    expansion_stats["rows_with_no_expansion"] = len(rows_with_no_output)
    expansion_stats["source_rows_with_no_output"] = [
        row_offset + idx + 1 for idx in sorted(rows_with_no_output)
    ]
    
    if rows_with_no_output:
        logger.warning(
            f"explode_columns: {len(rows_with_no_output)} source rows produced no output "
            f"(all source columns empty or null). Rows: {expansion_stats['source_rows_with_no_output'][:10]}"
        )
    
    if stacked.empty:
        expansion_stats["output_rows"] = len(df) if (include_original or keep_empty_rows) else 0
        return (
            df.drop(columns=source_columns, errors="ignore").to_dict("records") if include_original or keep_empty_rows else [],
            errors,
            expansion_stats
        )

    stacked.rename(columns={"level_0": "_row_index", "level_1": "source_column", 0: target_column}, inplace=True)
    if strip_whitespace:
        stacked[target_column] = stacked[target_column].apply(
            lambda v: v.strip() if isinstance(v, str) else v
        )

    if dedupe_values:
        stacked["_dedupe_key"] = stacked[target_column].apply(
            lambda v: v.lower() if case_insensitive_dedupe and isinstance(v, str) else v
        )
        stacked = stacked.loc[
            ~stacked[["_row_index", "_dedupe_key"]].duplicated()
        ].copy()
        stacked.drop(columns=["_dedupe_key"], inplace=True)

    base_df = df.drop(columns=source_columns, errors="ignore") if drop_source else df.copy()
    base_df = base_df.reset_index().rename(columns={"index": "_row_index"})

    exploded = stacked.merge(base_df, on="_row_index", how="left")
    exploded.drop(columns=["_row_index", "source_column"], inplace=True, errors="ignore")
    exploded_records = _df_to_records(exploded)

    if include_original:
        original_records = _df_to_records(base_df.drop(columns=["_row_index"], errors="ignore"))
        exploded_records = original_records + exploded_records
    elif keep_empty_rows:
        original_records = _df_to_records(base_df.drop(columns=["_row_index"], errors="ignore"))
        source_row_indices = set(stacked["_row_index"].tolist())
        empty_rows = [row for idx, row in enumerate(original_records) if idx not in source_row_indices]
        exploded_records = empty_rows + exploded_records

    expansion_stats["output_rows"] = len(exploded_records)
    
    return exploded_records, errors, expansion_stats


def _apply_explode_list_rows(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Explode a single list-like column into multiple rows (pandas explode style).
    """
    errors: List[Dict[str, Any]] = []
    source_column = transformation.get("source_column") or transformation.get("column")
    target_column = transformation.get("target_column") or transformation.get("target_field") or source_column
    delimiter = transformation.get("delimiter")
    drop_source = transformation.get("drop_source_column", True)
    include_original = transformation.get("include_original_row", False)
    keep_empty_rows = transformation.get("keep_empty_rows", False)
    strip_whitespace = transformation.get("strip_whitespace", True)
    dedupe_values = transformation.get("dedupe_values", True)
    case_insensitive_dedupe = transformation.get("case_insensitive_dedupe", True)

    if not source_column or not target_column:
        errors.append(
            {
                "type": "row_transformation",
                "message": "explode_list_rows requires source_column and target_column",
            }
        )
        return records, errors

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return [], errors
    if "_source_record_number" not in df.columns:
        df["_source_record_number"] = [row_offset + idx + 1 for idx in range(len(df))]

    if source_column not in df.columns:
        df[source_column] = None
        errors.append(
            {
                "type": "row_transformation",
                "message": f"Source column '{source_column}' missing for explode_list_rows",
                "column": source_column,
            }
        )

    df[target_column] = df[source_column].apply(
        lambda v: _parse_list_for_explode(v, delimiter=delimiter, strip_whitespace=strip_whitespace)
    )

    if dedupe_values:
        df[target_column] = df[target_column].apply(
            lambda values: _dedupe_preserve_order(values, case_insensitive_dedupe)
        )

    exploded = df.explode(target_column, ignore_index=True)
    exploded[target_column] = exploded[target_column].apply(
        lambda v: v.strip() if strip_whitespace and isinstance(v, str) else v
    )
    exploded = exploded.dropna(subset=[target_column])

    base_df = exploded.drop(columns=[source_column], errors="ignore") if drop_source else exploded.copy()

    exploded_records = _df_to_records(base_df)

    if include_original:
        original = _df_to_records(df.drop(columns=[target_column], errors="ignore") if drop_source else df.copy())
        exploded_records = original + exploded_records
    elif keep_empty_rows:
        has_values = set(exploded.get("_source_record_number", []))
        original = _df_to_records(df.drop(columns=[target_column], errors="ignore") if drop_source else df.copy())
        empty_rows = [row for row in original if row.get("_source_record_number") not in has_values]
        exploded_records = empty_rows + exploded_records

    return exploded_records, errors


def _apply_drop_columns(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Drop one or more columns from each record. Missing columns are ignored.
    """
    errors: List[Dict[str, Any]] = []
    columns = transformation.get("columns") or transformation.get("source_columns") or []
    if not columns:
        return records, errors

    df = pd.DataFrame.from_records(records)
    df = df.drop(columns=columns, errors="ignore")
    return df.to_dict("records"), errors


def _apply_require_any_of(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Keep only rows where at least one of the specified columns has a non-empty value.
    
    This is useful for OR-based row retention logic, such as:
    "Keep rows that have either an email OR a phone number"
    
    Args:
        records: Input records
        transformation: Must contain "columns" - list of column names to check
        row_offset: Starting row number for error reporting
        
    Returns:
        Filtered records and any errors
    """
    errors: List[Dict[str, Any]] = []
    columns = transformation.get("columns") or transformation.get("source_columns") or []
    
    if not columns:
        errors.append(
            {
                "type": "row_transformation",
                "message": "require_any_of requires columns parameter",
            }
        )
        return records, errors
    
    df = pd.DataFrame.from_records(records)
    if df.empty:
        return [], errors
    
    if "_source_record_number" not in df.columns:
        df["_source_record_number"] = [row_offset + idx + 1 for idx in range(len(df))]
    
    # Check which columns actually exist
    existing_cols = [col for col in columns if col in df.columns]
    missing_cols = [col for col in columns if col not in df.columns]
    
    if not existing_cols:
        errors.append(
            {
                "type": "row_transformation",
                "message": f"require_any_of: none of the specified columns exist ({columns})",
            }
        )
        return records, errors
    
    if missing_cols:
        errors.append(
            {
                "type": "row_transformation",
                "message": f"require_any_of: ignoring missing columns: {missing_cols}",
            }
        )
    
    # Create a mask: True if ANY of the columns has a non-empty value
    def _has_value(series: pd.Series) -> pd.Series:
        """Check if series has non-null, non-empty values"""
        return series.notna() & (series.astype(str).str.strip() != "")
    
    # Check each column and combine with OR logic
    has_any_value = pd.Series([False] * len(df))
    for col in existing_cols:
        has_any_value = has_any_value | _has_value(df[col])
    
    # Filter to keep only rows with at least one value
    filtered = df.loc[has_any_value].copy()
    
    rows_dropped = len(df) - len(filtered)
    if rows_dropped > 0:
        logger.info(
            f"require_any_of: Dropped {rows_dropped} rows where all columns {existing_cols} were empty"
        )
    
    return _df_to_records(filtered), errors


def _apply_filter_rows(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Keep/drop rows based on regex patterns across one or more columns.
    """
    errors: List[Dict[str, Any]] = []
    include_pattern = transformation.get("include_regex")
    exclude_pattern = transformation.get("exclude_regex")
    columns = transformation.get("columns") or transformation.get("source_columns")

    if not include_pattern and not exclude_pattern:
        errors.append(
            {
                "type": "row_transformation",
                "message": "filter_rows requires include_regex or exclude_regex",
            }
        )
        return records, errors

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return [], errors

    if "_source_record_number" not in df.columns:
        df["_source_record_number"] = [row_offset + idx + 1 for idx in range(len(df))]

    if not columns:
        columns = [col for col in df.columns if not str(col).startswith("_")]
    else:
        existing_cols = [col for col in columns if col in df.columns]
        missing_cols = [col for col in columns if col not in df.columns]
        if not existing_cols:
            errors.append(
                {
                    "type": "row_transformation",
                    "message": f"filter_rows skipped: none of the requested columns exist ({columns})",
                }
            )
            return records, errors
        if missing_cols:
            errors.append(
                {
                    "type": "row_transformation",
                    "message": f"filter_rows ignored missing columns: {missing_cols}",
                }
            )
        columns = existing_cols

    try:
        include_regex = re.compile(include_pattern) if include_pattern else None
    except re.error as exc:
        errors.append(
            {
                "type": "row_transformation",
                "message": f"Invalid include_regex: {exc}",
            }
        )
        include_regex = None
    try:
        exclude_regex = re.compile(exclude_pattern) if exclude_pattern else None
    except re.error as exc:
        errors.append(
            {
                "type": "row_transformation",
                "message": f"Invalid exclude_regex: {exc}",
            }
        )
        exclude_regex = None

    def _contains(series: pd.Series, pattern: re.Pattern) -> pd.Series:
        return series.astype(str).str.contains(pattern, na=False, regex=True)

    include_mask = pd.Series([True] * len(df))
    if include_regex:
        include_mask = df[columns].apply(lambda col: _contains(col, include_regex)).any(axis=1)

    exclude_mask = pd.Series([False] * len(df))
    if exclude_regex:
        exclude_mask = df[columns].apply(lambda col: _contains(col, exclude_regex)).any(axis=1)

    final_mask = include_mask & ~exclude_mask
    filtered = df.loc[final_mask].copy()
    return _df_to_records(filtered), errors


def _apply_concat_columns(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Concatenate multiple columns into one string column row-by-row.
    """
    errors: List[Dict[str, Any]] = []
    sources = transformation.get("sources") or transformation.get("columns") or []
    target_column = transformation.get("target_column") or transformation.get("target_field") or transformation.get("column")
    separator = transformation.get("separator", " ")
    strip_whitespace = transformation.get("strip_whitespace", True)
    skip_nulls = transformation.get("skip_nulls", True)
    null_replacement = transformation.get("null_replacement", "")

    if not sources or not target_column:
        errors.append(
            {
                "type": "row_transformation",
                "message": "concat_columns requires sources and target_column",
            }
        )
        return records, errors

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return [], errors

    for src in sources:
        if src not in df.columns:
            df[src] = None
            errors.append(
                {
                    "type": "row_transformation",
                    "message": f"Source column '{src}' missing for concat_columns",
                    "column": src,
                }
            )

    def _merge_row(row: pd.Series) -> Any:
        pieces: List[str] = []
        for src in sources:
            value = row.get(src)
            if value is None or (isinstance(value, float) and pd.isna(value)):
                if skip_nulls:
                    continue
                value = null_replacement
            text = str(value)
            if strip_whitespace:
                text = text.strip()
            if skip_nulls and not text:
                continue
            pieces.append(text)
        return separator.join(pieces) if pieces else None

    df[target_column] = df.apply(_merge_row, axis=1)
    return _df_to_records(df), errors


def _apply_regex_replace(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Apply regex-based replacements to specific columns.
    """
    errors: List[Dict[str, Any]] = []
    pattern = transformation.get("pattern")
    replacement = transformation.get("replacement", "")
    columns = transformation.get("columns") or transformation.get("source_columns") or transformation.get("target_columns")
    outputs = transformation.get("outputs") or transformation.get("targets")
    skip_on_no_match = transformation.get("skip_on_no_match", False)

    if not pattern or not columns:
        errors.append(
            {
                "type": "row_transformation",
                "message": "regex_replace requires pattern and columns",
            }
        )
        return records, errors

    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        errors.append(
            {
                "type": "row_transformation",
                "message": f"Invalid regex pattern: {exc}",
            }
        )
        return records, errors

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return [], errors

    if "_source_record_number" not in df.columns:
        df["_source_record_number"] = [row_offset + idx + 1 for idx in range(len(df))]

    for col in columns:
        if col not in df.columns:
            errors.append(
                {
                    "type": "row_transformation",
                    "message": f"Column '{col}' missing for regex_replace",
                    "column": col,
                }
            )
            df[col] = None

    def _replace_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        text = str(value)
        match = compiled.search(text)
        if not match:
            # Preserve original value when no match
            return value
        return compiled.sub(replacement, text)

    if outputs:
        # Multi-output mode: extract capture groups into multiple columns
        for col in columns:
            if col not in df.columns:
                df[col] = None
        
        # Track match statistics for logging
        match_counts = {col: 0 for col in columns}
        total_values = {col: 0 for col in columns}
        
        matches = df[columns].map(lambda v: compiled.search(str(v)) if v is not None else None)
        
        # Count matches for logging
        for col in columns:
            if col in df.columns:
                total_values[col] = df[col].notna().sum()
                match_counts[col] = matches[col].notna().sum()

        for output in outputs:
            if not isinstance(output, dict):
                continue
            name = output.get("name") or output.get("field") or output.get("column")
            if not name:
                continue
            group_id = output.get("group") or output.get("index")
            default = output.get("default")
            def _extract_match(row):
                # Use first matching column in row
                for col in columns:
                    m = matches.at[row.name, col]
                    if m:
                        try:
                            return m.group(group_id) if group_id is not None else m.group(0)
                        except Exception:
                            return default
                # No match found - preserve original value from first column if skip_on_no_match
                if skip_on_no_match and columns:
                    return row.get(columns[0])
                return None
            df[name] = df.apply(_extract_match, axis=1)
        
        # Log match statistics
        for col in columns:
            if match_counts[col] == 0 and total_values[col] > 0:
                sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else "N/A"
                logger.warning(
                    f"regex_replace: Pattern '{pattern}' matched 0/{total_values[col]} values in column '{col}'. "
                    f"Sample value: '{str(sample_value)[:50]}'. Original values preserved."
                )
            elif match_counts[col] < total_values[col]:
                logger.info(
                    f"regex_replace: Pattern '{pattern}' matched {match_counts[col]}/{total_values[col]} values in column '{col}'"
                )
    else:
        for col in columns:
            original_values = df[col].copy()
            df[col] = df[col].map(_replace_value)
            
            # Log if no values were changed (indicating no matches)
            if not df[col].isna().all() and (df[col] == original_values).all():
                sample_value = original_values.dropna().iloc[0] if not original_values.dropna().empty else "N/A"
                logger.warning(
                    f"regex_replace: Pattern '{pattern}' matched 0 values in column '{col}'. "
                    f"Sample value: '{str(sample_value)[:50]}'. Original values preserved."
                )

    return _df_to_records(df), errors


def _apply_conditional_transform(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Apply nested row transformations only when rows match include/exclude regex.
    """
    errors: List[Dict[str, Any]] = []
    actions = transformation.get("actions") or transformation.get("transformations") or []
    include_pattern = transformation.get("include_regex")
    exclude_pattern = transformation.get("exclude_regex")
    columns = transformation.get("columns") or transformation.get("source_columns")

    if not actions:
        errors.append(
            {
                "type": "row_transformation",
                "message": "conditional_transform requires actions list",
            }
        )
        return records, errors

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return [], errors

    if "_source_record_number" not in df.columns:
        df["_source_record_number"] = [row_offset + idx + 1 for idx in range(len(df))]
    df["_row_idx"] = range(len(df))

    if not columns:
        columns = [col for col in df.columns if not str(col).startswith("_")]

    for col in columns:
        if col not in df.columns:
            df[col] = None

    try:
        include_regex = re.compile(include_pattern) if include_pattern else None
    except re.error as exc:
        errors.append(
            {
                "type": "row_transformation",
                "message": f"Invalid include_regex: {exc}",
            }
        )
        include_regex = None
    try:
        exclude_regex = re.compile(exclude_pattern) if exclude_pattern else None
    except re.error as exc:
        errors.append(
            {
                "type": "row_transformation",
                "message": f"Invalid exclude_regex: {exc}",
            }
        )
        exclude_regex = None

    def _contains(series: pd.Series, pattern: re.Pattern) -> pd.Series:
        return series.astype(str).str.contains(pattern, na=False, regex=True)

    include_mask = pd.Series([True] * len(df))
    if include_regex:
        include_mask = df[columns].apply(lambda col: _contains(col, include_regex)).any(axis=1)

    exclude_mask = pd.Series([False] * len(df))
    if exclude_regex:
        exclude_mask = df[columns].apply(lambda col: _contains(col, exclude_regex)).any(axis=1)

    match_mask = include_mask & ~exclude_mask

    matched_records = df.loc[match_mask].to_dict("records")
    non_matched_records = df.loc[~match_mask].to_dict("records")

    transformed_matches = matched_records
    for action in actions:
        if not isinstance(action, dict):
            continue
        a_type = action.get("type")
        if a_type == "explode_columns":
            transformed_matches, new_errors, _ = _apply_explode_columns(
                transformed_matches,
                action,
                row_offset=row_offset,
            )
        elif a_type == "filter_rows":
            transformed_matches, new_errors = _apply_filter_rows(
                transformed_matches,
                action,
                row_offset=row_offset,
            )
        elif a_type == "regex_replace":
            transformed_matches, new_errors = _apply_regex_replace(
                transformed_matches,
                action,
                row_offset=row_offset,
            )
        else:
            new_errors = [
                {
                    "type": "row_transformation",
                    "message": f"Unknown conditional action type '{a_type}'",
                    "record_number": None,
                }
            ]
        errors.extend(new_errors)

    def _get_idx(rec: Dict[str, Any]) -> int:
        try:
            return int(rec.get("_row_idx", 0))
        except Exception:
            return 0

    combined = non_matched_records + transformed_matches
    combined.sort(key=_get_idx)
    for rec in combined:
        rec.pop("_row_idx", None)

    return combined, errors


def _normalize_value(value: Any, strip_whitespace: bool) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str):
        text = value.strip() if strip_whitespace else value
        if not text or text.lower() == "null":
            return None
        return text
    return value


def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    records = df.to_dict("records")
    for record in records:
        for key, val in list(record.items()):
            if isinstance(val, float) and pd.isna(val):
                record[key] = None
            elif pd.isna(val):
                record[key] = None
    return records


def _parse_list_for_explode(value: Any, *, delimiter: str = None, strip_whitespace: bool = True) -> List[Any]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        values = value
    elif isinstance(value, str):
        text = value.strip() if strip_whitespace else value
        if not text:
            return []
        # Try JSON first
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            values = parsed
        else:
            if delimiter:
                parts = re.split(re.escape(delimiter), text)
            else:
                email_tokens = re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+", text, flags=re.IGNORECASE)
                if len(email_tokens) > 1:
                    parts = email_tokens
                else:
                    parts = re.split(r"[;,]", text)
            values = [part.strip() if strip_whitespace else part for part in parts if part or not strip_whitespace]
            if not values:
                values = [text]
    else:
        values = [value]

    cleaned = []
    for v in values:
        if isinstance(v, str) and strip_whitespace:
            v = v.strip()
        if v in ("", None):
            continue
        cleaned.append(v)
    return cleaned


def _dedupe_preserve_order(values: List[Any], case_insensitive: bool) -> List[Any]:
    seen = set()
    deduped: List[Any] = []
    for v in values:
        key = v.lower() if case_insensitive and isinstance(v, str) else v
        if key in seen:
            continue
        seen.add(key)
        deduped.append(v)
    return deduped


def _apply_standardize_phone(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Standardize phone numbers in a column across all records.
    
    Transformation parameters:
        - source_column (required): Input column name
        - target_column (optional): Output column name (defaults to source_column)
        - default_country_code (optional): Country code to add if missing (e.g., "1", "44")
        - output_format (optional): "e164", "international", "national", "digits_only" (default: "e164")
        - preserve_extension (optional): Keep extension like "x123" (default: False)
        - strip_leading_zeros (optional): Remove leading zeros (default: True)
        - min_digits (optional): Minimum valid digits (default: 7)
        - max_digits (optional): Maximum valid digits (default: 15)
    """
    errors: List[Dict[str, Any]] = []
    source_column = transformation.get("source_column") or transformation.get("column")
    target_column = (
        transformation.get("target_column")
        or transformation.get("target_field")
        or source_column
    )
    
    if not source_column or not target_column:
        errors.append(
            {
                "type": "row_transformation",
                "message": "standardize_phone requires source_column",
            }
        )
        return records, errors
    
    # Extract parameters with defaults
    default_country_code = transformation.get("default_country_code")
    output_format = transformation.get("output_format", "e164")
    preserve_extension = transformation.get("preserve_extension", False)
    strip_leading_zeros = transformation.get("strip_leading_zeros", True)
    min_digits = transformation.get("min_digits", 7)
    max_digits = transformation.get("max_digits", 15)
    
    df = pd.DataFrame.from_records(records)
    if df.empty:
        return [], errors
    
    if "_source_record_number" not in df.columns:
        df["_source_record_number"] = [row_offset + idx + 1 for idx in range(len(df))]
    
    if source_column not in df.columns:
        df[source_column] = None
        errors.append(
            {
                "type": "row_transformation",
                "message": f"Source column '{source_column}' missing for standardize_phone",
                "column": source_column,
            }
        )
    
    # Apply standardization to each value
    df[target_column] = df[source_column].apply(
        lambda value: standardize_phone(
            value,
            default_country_code=default_country_code,
            output_format=output_format,
            preserve_extension=preserve_extension,
            strip_leading_zeros=strip_leading_zeros,
            min_digits=min_digits,
            max_digits=max_digits,
        )
    )
    
    return _df_to_records(df), errors

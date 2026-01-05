"""
Auto-import execution logic for LLM-analyzed files.

This module handles the execution of import strategies recommended by the LLM agent.
"""

from typing import Dict, Any, List, Optional, Set, Tuple
from sqlalchemy import text, inspect
from app.domain.imports.mapper import detect_mapping_from_file
from app.db.models import create_table_if_not_exists, insert_records, calculate_file_hash
from app.db.session import get_engine
from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig, ensure_safe_table_name
from app.db.metadata import store_table_metadata, enrich_table_metadata
from app.domain.imports.schema_mapper import analyze_schema_compatibility, transform_record
from app.domain.imports.schema_migrations import (
    apply_schema_migrations,
    SchemaMigrationError,
)
from app.domain.imports.history import start_import_tracking, complete_import_tracking
from app.domain.imports.fingerprinting import store_table_fingerprint
from app.utils.date import parse_flexible_date
import pandas as pd
import logging
import time
import re
import json

logger = logging.getLogger(__name__)


_TYPE_ALIAS_MAP = {
    "numeric": "DECIMAL",
    "number": "DECIMAL",
    "decimal": "DECIMAL",
    "float": "DECIMAL",
    "double": "DECIMAL",
    "currency": "DECIMAL",
    "percentage": "DECIMAL",
    "percent": "DECIMAL",
    "integer": "INTEGER",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "INTEGER",
    "whole": "INTEGER",
    "timestamp": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "date": "DATE",
    "time": "TIMESTAMP",
    "text": "TEXT",
    "string": "TEXT",
    "varchar": "TEXT",
    "char": "TEXT",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN"
}

_SUPPORTED_TYPES = {"TEXT", "DECIMAL", "INTEGER", "BIGINT", "TIMESTAMP", "DATE", "BOOLEAN"}

_SLASHED_DATE_PATTERN = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{2,4})")


def _load_previous_uniqueness_columns(engine, table_name: str) -> Optional[List[str]]:
    """
    Load uniqueness columns from the most recent successful import for a table.

    Keeping uniqueness stable across imports ensures duplicate detection remains
    consistent when the LLM proposes different unique keys for later files.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT mapping_config
                    FROM import_history
                    WHERE table_name = :table_name
                      AND status = 'success'
                      AND mapping_config IS NOT NULL
                    ORDER BY import_timestamp DESC
                    LIMIT 1
                    """
                ),
                {"table_name": table_name},
            )
            row = result.fetchone()
            if not row:
                return None

            cfg = row[0]
            if isinstance(cfg, str):
                try:
                    cfg = json.loads(cfg)
                except Exception:
                    cfg = None

            if not isinstance(cfg, dict):
                return None

            unique_cols = cfg.get("unique_columns")
            if not unique_cols:
                dc = cfg.get("duplicate_check") or {}
                unique_cols = (
                    dc.get("uniqueness_columns")
                    or dc.get("unique_columns")
                    or cfg.get("uniqueness_columns")
                )

            if unique_cols:
                logger.info(
                    "AUTO-IMPORT: Reusing previous uniqueness columns for table '%s': %s",
                    table_name,
                    unique_cols,
                )
            return unique_cols
    except Exception as exc:
        logger.warning(
            "AUTO-IMPORT: Could not load previous uniqueness columns for table '%s': %s",
            table_name,
            exc,
        )
        return None


def _detect_dayfirst(series: pd.Series) -> Optional[bool]:
    """
    Try to infer whether we should use day-first parsing for a column.

    Returns True/False when detection is confident, otherwise None so pandas keeps defaults.
    """
    if series is None:
        return None

    sample_values = series.dropna()
    if sample_values.empty:
        return None

    for value in sample_values.astype(str).head(100):
        match = _SLASHED_DATE_PATTERN.match(value)
        if not match:
            continue
        first = int(match.group(1))
        second = int(match.group(2))

        if first > 12 and second <= 12:
            return True
        if second > 12 and first <= 12:
            return False

    return None


def _normalize_existing_column_type(sqlalchemy_type: Any) -> str:
    """
    Map a SQLAlchemy-reflected column type to the limited set we use elsewhere.
    """
    type_str = str(sqlalchemy_type).upper()

    if "TIMESTAMP" in type_str or "DATETIME" in type_str:
        return "TIMESTAMP"
    if type_str == "DATE" or type_str.endswith(" DATE"):
        return "DATE"
    if "BOOL" in type_str:
        return "BOOLEAN"
    if any(token in type_str for token in ("INT", "SERIAL")):
        return "INTEGER"
    if any(token in type_str for token in ("DECIMAL", "NUMERIC", "REAL", "DOUBLE", "FLOAT", "MONEY")):
        return "DECIMAL"
    return "TEXT"


def _load_existing_table_schema(engine, table_name: str) -> Dict[str, str]:
    """
    Read the current table schema and return a mapping of column -> normalized SQL type.
    """
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return {}

    schema: Dict[str, str] = {}
    for column in inspector.get_columns(table_name):
        schema[column["name"]] = _normalize_existing_column_type(column["type"])
    return schema


def _normalize_uniqueness_columns(
    uniqueness_columns: Optional[List[str]],
    column_mapping: Dict[str, str],
    target_columns: List[str],
) -> List[str]:
    """
    Align uniqueness columns to actual target table columns.

    - Accepts source column names and maps them via column_mapping (source->target).
    - Falls back to the provided name when no mapping exists.
    - Applies a lightweight pluralization heuristic (email -> emails).
    """
    if not uniqueness_columns:
        return []

    target_set = set(target_columns or [])
    normalized: List[str] = []

    for col in uniqueness_columns:
        candidate = column_mapping.get(col, col)

        if candidate not in target_set:
            # Try singular/plural adjustment as a last resort
            if not candidate.endswith("s") and f"{candidate}s" in target_set:
                candidate = f"{candidate}s"
            elif candidate.endswith("s") and candidate[:-1] in target_set:
                candidate = candidate[:-1]

        if candidate not in normalized:
            normalized.append(candidate)

    return normalized


def _extract_columns_from_migrations(migrations: List[Dict[str, Any]]) -> Set[str]:
    """Return the set of column names already modified by caller-provided migrations."""
    targeted: Set[str] = set()
    for migration in migrations or []:
        if not migration:
            continue
        action = migration.get("action")
        if action == "replace_column":
            if migration.get("column_name"):
                targeted.add(migration["column_name"])
            elif migration.get("old_column"):
                targeted.add(migration["old_column"])
    return targeted


def _is_type_compatible(existing_type: str, desired_type: str) -> bool:
    """
    Check if existing column type can accommodate the desired type without migration.
    
    Returns True if the existing type can hold the desired data (no migration needed).
    This prevents incompatible type changes like NUMERIC -> BOOLEAN which fail in PostgreSQL.
    
    Compatibility rules:
    - NUMERIC/INTEGER/DECIMAL can hold BOOLEAN data as 0/1
    - TEXT can hold any type (widest type)
    - Same types are always compatible
    """
    if existing_type == desired_type:
        return True
    
    existing_upper = existing_type.upper()
    desired_upper = desired_type.upper()
    
    # NUMERIC types can hold BOOLEAN data as 0/1
    if desired_upper == "BOOLEAN" and any(
        token in existing_upper for token in ("NUMERIC", "DECIMAL", "INT", "BIGINT", "SMALLINT")
    ):
        logger.info(
            "Type compatibility: Existing %s can hold BOOLEAN data as 0/1, skipping migration",
            existing_type
        )
        return True
    
    # TEXT can hold any type (widest type)
    if existing_upper == "TEXT":
        logger.info(
            "Type compatibility: Existing TEXT can hold %s data, skipping migration",
            desired_type
        )
        return True
    
    # Different types require migration
    return False


def _build_alignment_migrations(
    existing_schema: Dict[str, str],
    desired_schema: Dict[str, str],
    already_targeted: Set[str],
) -> List[Dict[str, Any]]:
    """
    Generate replace_column migrations for columns whose types need to change.
    
    Only generates migrations when types are incompatible. Compatible types
    (e.g., NUMERIC can hold BOOLEAN as 0/1) skip migration to avoid casting errors.
    """
    migrations: List[Dict[str, Any]] = []
    for column, desired_type in (desired_schema or {}).items():
        if not desired_type:
            continue
        existing_type = existing_schema.get(column)
        if not existing_type:
            continue
        if existing_type == desired_type:
            continue
        if column in already_targeted:
            continue
        
        # Check type compatibility before generating migration
        if _is_type_compatible(existing_type, desired_type):
            logger.info(
                "AUTO-IMPORT: Skipping migration for column '%s': existing type %s is compatible with desired type %s",
                column,
                existing_type,
                desired_type
            )
            continue
        
        migrations.append(
            {
                "action": "replace_column",
                "column_name": column,
                "new_type": desired_type,
            }
        )
    return migrations


def _is_numeric_like(value: Any) -> bool:
    """Return True when the value looks like a plain numeric token."""
    if value is None:
        return False
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)) and not pd.isna(value):
        return True
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return False
        try:
            float(token)
            return True
        except ValueError:
            return False
    return False


def _is_integer_like(value: Any) -> bool:
    """Return True when the value represents an integer."""
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return float(value).is_integer()
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return False
        if token[0] in {"+", "-"}:
            token = token[1:]
        return token.isdigit()
    return False


def normalize_expected_type(raw_type: Optional[str]) -> str:
    """
    Normalize arbitrary type descriptions into the limited set we support.
    Defaults to TEXT when no confident match is found.
    """
    if raw_type is None:
        return "TEXT"
    raw_str = str(raw_type).strip()
    if not raw_str:
        return "TEXT"
    lookup_key = raw_str.lower()
    canonical = _TYPE_ALIAS_MAP.get(lookup_key)
    if canonical:
        return canonical
    upper_value = raw_str.upper()
    return upper_value if upper_value in _SUPPORTED_TYPES else "TEXT"


def _coerce_boolean_series(series: pd.Series) -> tuple[pd.Series, int]:
    """Best-effort boolean coercion using common truthy/falsey tokens."""
    true_tokens = {"true", "t", "1", "yes", "y", "on"}
    false_tokens = {"false", "f", "0", "no", "n", "off"}
    coerced = 0
    values = []
    for value in series:
        if pd.isna(value):
            values.append(None)
            continue
        token = str(value).strip().lower()
        if token in true_tokens:
            values.append(True)
        elif token in false_tokens:
            values.append(False)
        else:
            values.append(None)
            coerced += 1
    return pd.Series(values, dtype="object"), coerced


def coerce_records_to_expected_types(
    records: List[Dict[str, Any]],
    expected_types: Dict[str, str],
    existing_schema: Optional[Dict[str, str]] = None
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Use pandas to coerce the incoming records to the types the LLM determined.
    
    When existing_schema is provided, respects existing column types and converts
    data accordingly. For example, if existing column is NUMERIC and incoming data
    is BOOLEAN, converts true/false to 1/0 instead of trying to change the column type.
    
    Returns converted records and a summary of conversions applied.
    """
    if not records or not expected_types:
        return records, {}
    df = pd.DataFrame(records)
    if df.empty:
        return records, {}

    conversion_summary: Dict[str, Dict[str, Any]] = {}
    for source_col, raw_type in expected_types.items():
        normalized_type = normalize_expected_type(raw_type)
        column_summary: Dict[str, Any] = {"expected_type": normalized_type}

        if source_col not in df.columns:
            column_summary["status"] = "missing_source_column"
            conversion_summary[source_col] = column_summary
            continue

        series = df[source_col]
        
        # Check if we need to adapt to existing schema type
        existing_type = None
        if existing_schema and source_col in existing_schema:
            existing_type = existing_schema[source_col]
            
            # If existing column is NUMERIC and desired is BOOLEAN, convert to 0/1
            if normalized_type == "BOOLEAN" and any(
                token in existing_type.upper() for token in ("NUMERIC", "DECIMAL", "INT", "BIGINT", "SMALLINT")
            ):
                logger.info(
                    "AUTO-IMPORT: Converting BOOLEAN data to 0/1 for existing %s column '%s'",
                    existing_type,
                    source_col
                )
                # Convert boolean-like values to 0/1
                converted, coerced_count = _coerce_boolean_series(series)
                # Now convert True/False to 1/0
                numeric_series = converted.apply(lambda x: 1 if x is True else (0 if x is False else None))
                df[source_col] = numeric_series
                column_summary["status"] = "converted_boolean_to_numeric"
                column_summary["note"] = f"Converted to 0/1 to match existing {existing_type} column"
                if coerced_count:
                    column_summary["coerced_values"] = coerced_count
                conversion_summary[source_col] = column_summary
                continue
        
        try:
            if normalized_type in {"DECIMAL", "INTEGER", "BIGINT"}:
                converted = pd.to_numeric(series, errors="coerce")
                coerced_count = int((series.notna() & converted.isna()).sum())
                column_summary["status"] = "converted"
                if coerced_count:
                    column_summary["coerced_values"] = coerced_count
                df[source_col] = converted
            elif normalized_type in {"TIMESTAMP", "DATE"}:
                dayfirst_override = _detect_dayfirst(series)
                if dayfirst_override is None:
                    converted = pd.to_datetime(series, errors="coerce", utc=False)
                else:
                    converted = pd.to_datetime(
                        series,
                        errors="coerce",
                        utc=False,
                        dayfirst=dayfirst_override,
                    )
                coerced_count = int((series.notna() & converted.isna()).sum())
                column_summary["status"] = "converted"
                if coerced_count:
                    column_summary["coerced_values"] = coerced_count
                df[source_col] = converted
            elif normalized_type == "BOOLEAN":
                converted, coerced_count = _coerce_boolean_series(series)
                column_summary["status"] = "converted"
                if coerced_count:
                    column_summary["coerced_values"] = coerced_count
                df[source_col] = converted
            elif normalized_type == "TEXT":
                if pd.api.types.is_numeric_dtype(series):
                    # Smart conversion: remove .0 from float-integers (e.g. zip codes)
                    def _numeric_to_str(val):
                        if pd.isna(val):
                            return None
                        if isinstance(val, float) and val.is_integer():
                            return str(int(val))
                        return str(val)
                    df[source_col] = series.apply(_numeric_to_str)
                else:
                    df[source_col] = series.astype(str).where(series.notna(), None)
                column_summary["status"] = "converted"
            else:
                column_summary["status"] = "unsupported_type"
        except Exception as exc:
            column_summary["status"] = "error"
            column_summary["error"] = str(exc)
            logger.warning(
                "AUTO-IMPORT: Failed to coerce column '%s' to type '%s': %s",
                source_col,
                normalized_type,
                exc
            )

        conversion_summary[source_col] = column_summary

    df = df.where(pd.notnull(df), None)
    records_converted = df.to_dict(orient="records")
    for record in records_converted:
        for key, value in list(record.items()):
            if pd.isna(value):
                record[key] = None
                continue
            if isinstance(value, pd.Timestamp):
                record[key] = value.to_pydatetime()

    return records_converted, conversion_summary


def _parse_keep_only_instruction(instruction_text: Optional[str]) -> Dict[str, bool]:
    """
    Parse user instruction to detect "keep only primary" style directives.
    
    Returns a dict mapping data types to whether they should be filtered:
    {"email": True, "phone": True} means keep only primary email/phone
    """
    if not instruction_text:
        return {}
    
    lowered = instruction_text.lower()
    filters = {}
    
    # Patterns that indicate "keep only one" for a data type
    keep_only_patterns = [
        "keep only",
        "only keep",
        "primary only",
        "single",
        "one per",
        "first only",
    ]
    
    # Check if any keep-only pattern is present
    has_keep_only = any(pattern in lowered for pattern in keep_only_patterns)
    
    if not has_keep_only:
        return {}
    
    # Detect which data types are mentioned
    if any(term in lowered for term in ["email", "e-mail", "mail"]):
        filters["email"] = True
    
    if any(term in lowered for term in ["phone", "telephone", "tel", "mobile", "contact number"]):
        filters["phone"] = True
    
    return filters


def _filter_column_mapping_per_instruction(
    column_mapping: Dict[str, str],
    instruction_text: Optional[str],
    records: List[Dict[str, Any]]
) -> Tuple[Dict[str, str], List[str]]:
    """
    Filter column_mapping to respect user instructions like "keep only the primary email".
    
    Returns:
        (filtered_mapping, excluded_columns)
    """
    filters = _parse_keep_only_instruction(instruction_text)
    
    if not filters:
        return column_mapping, []
    
    filtered_mapping = dict(column_mapping)
    excluded_columns = []
    
    # Helper to detect if a column name contains a data type keyword
    def column_contains_type(col_name: str, data_type: str) -> bool:
        col_lower = col_name.lower()
        if data_type == "email":
            return any(term in col_lower for term in ["email", "e-mail", "mail"])
        elif data_type == "phone":
            return any(term in col_lower for term in ["phone", "tel", "mobile", "contact"])
        return False
    
    # Helper to detect if a column is "primary" or "first"
    def is_primary_column(col_name: str) -> bool:
        col_lower = col_name.lower()
        return any(term in col_lower for term in ["primary", "main", "first", "contact phone 1"])
    
    for data_type, should_filter in filters.items():
        if not should_filter:
            continue
        
        # Find all columns of this data type
        type_columns = [
            (source, target) 
            for source, target in column_mapping.items()
            if column_contains_type(source, data_type) or column_contains_type(target, data_type)
        ]
        
        if len(type_columns) <= 1:
            # Only one column of this type, no need to filter
            continue
        
        # Find the primary column
        primary_col = None
        for source, target in type_columns:
            if is_primary_column(source) or is_primary_column(target):
                primary_col = (source, target)
                break
        
        # If no explicit "primary", keep the first one
        if not primary_col:
            primary_col = type_columns[0]
        
        # Remove all non-primary columns
        for source, target in type_columns:
            if (source, target) != primary_col:
                filtered_mapping.pop(source, None)
                excluded_columns.append(source)
                logger.info(
                    "USER INSTRUCTION FILTER: Excluding column '%s' (user said keep only primary %s)",
                    source,
                    data_type
                )
    
    return filtered_mapping, excluded_columns


def _filter_row_transformations_per_instruction(
    row_transformations: List[Dict[str, Any]],
    instruction_text: Optional[str],
    excluded_columns: List[str]
) -> List[Dict[str, Any]]:
    """
    Remove explode_columns transformations that would violate user instructions.
    
    If user says "keep only primary email", we should NOT explode email columns.
    """
    filters = _parse_keep_only_instruction(instruction_text)
    
    if not filters:
        return row_transformations
    
    filtered_transforms = []
    
    for transform in row_transformations:
        if not isinstance(transform, dict):
            filtered_transforms.append(transform)
            continue
        
        if transform.get("type") != "explode_columns":
            filtered_transforms.append(transform)
            continue
        
        # Check if this explode_columns targets a filtered data type
        source_columns = transform.get("source_columns", [])
        target_column = transform.get("target_column", "")
        
        should_remove = False
        for data_type, should_filter in filters.items():
            if not should_filter:
                continue
            
            # Check if any source column or target column matches the filtered type
            for col in source_columns + [target_column]:
                col_lower = str(col).lower()
                if data_type == "email" and any(term in col_lower for term in ["email", "e-mail", "mail"]):
                    should_remove = True
                    break
                elif data_type == "phone" and any(term in col_lower for term in ["phone", "tel", "mobile"]):
                    should_remove = True
                    break
            
            if should_remove:
                break
        
        if should_remove:
            logger.info(
                "USER INSTRUCTION FILTER: Removing explode_columns for %s (user said keep only primary)",
                source_columns
            )
        else:
            filtered_transforms.append(transform)
    
    return filtered_transforms


def execute_llm_import_decision(
    file_content: bytes,
    file_name: str,
    all_records: List[Dict[str, Any]],
    llm_decision: Dict[str, Any],
    source_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute an import based on LLM's decision.
    
    Args:
        file_content: Raw file content
        file_name: Name of the file
        all_records: All records from the file (not just sample)
        llm_decision: LLM's decision with strategy, target_table, column_mapping, etc.
        source_path: Optional B2 file path for tracking (enables duplicate count matching)
        
    Returns:
        Execution result with success status and details
    """
    from app.domain.imports.orchestrator import execute_data_import
    from app.domain.imports.processors.csv_processor import process_csv
    
    schema_migration_results: List[Dict[str, Any]] = []
    try:
        strategy = llm_decision["strategy"]
        target_table = llm_decision["target_table"]
        column_mapping = dict(llm_decision.get("column_mapping", {}))
        unique_columns = llm_decision.get("unique_columns", [])
        has_header = llm_decision.get("has_header")
        forced_table = ensure_safe_table_name(llm_decision.get("forced_target_table") or target_table)
        forced_table_mode = llm_decision.get("forced_table_mode")
        target_table = forced_table or target_table
        if forced_table_mode == "existing" and strategy == "NEW_TABLE":
            logger.info("Adjusting strategy to ADAPT_DATA for existing-table request")
            strategy = "ADAPT_DATA"
        elif forced_table_mode == "new" and strategy != "NEW_TABLE":
            logger.info("Adjusting strategy to NEW_TABLE for new-table request")
            strategy = "NEW_TABLE"
        
        # RACE CONDITION FIX: Check if table was created by parallel worker
        # This handles ZIP file processing where multiple files with same structure
        # are processed in parallel. If the first file creates the table while we're
        # waiting, we should merge into it instead of creating a new table.
        if strategy == "NEW_TABLE":
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = :table_name
                    )
                """), {"table_name": target_table})
                table_exists_now = result.scalar()
                
                if table_exists_now:
                    logger.info(
                        "AUTO-IMPORT: Table '%s' was created by parallel worker. "
                        "Forcing ADAPT_DATA strategy to merge instead of recreating table.",
                        target_table
                    )
                    strategy = "ADAPT_DATA"
        
        logger.info(f"="*80)
        logger.info(f"AUTO-IMPORT: Executing LLM decision")
        logger.info(f"  Strategy: {strategy}")
        logger.info(f"  Target Table: {target_table}")
        logger.info(f"  File: {file_name}")
        logger.info(f"  Has Header: {has_header}")
        logger.info(f"  Column Mapping: {column_mapping}")
        logger.info(f"  Unique Columns: {unique_columns}")
        logger.info(f"="*80)
        
        # Detect file type
        file_type = "csv" if file_name.endswith('.csv') else \
                   "excel" if file_name.endswith(('.xlsx', '.xls')) else \
                   "json" if file_name.endswith('.json') else \
                   "xml" if file_name.endswith('.xml') else "unknown"
        
        # Parse file according to LLM's instructions
        if file_type == "csv" and has_header is not None:
            logger.info(f"AUTO-IMPORT: Parsing CSV with has_header={has_header}")
            records = process_csv(file_content, has_header=has_header)
        else:
            # For non-CSV or when has_header not specified, use all_records
            logger.info(f"AUTO-IMPORT: Using pre-parsed records ({len(all_records)} records)")
            records = all_records
        
        logger.info(f"AUTO-IMPORT: Parsed {len(records)} records")

        expected_column_types = llm_decision.get("expected_column_types") or {}
        column_transformations = llm_decision.get("column_transformations") or []
        row_transformations = llm_decision.get("row_transformations") or []
        column_validations = llm_decision.get("column_validations") or []
        instruction_text = llm_decision.get("llm_instruction") or ""
        multi_value_directives = llm_decision.get("multi_value_directives") or []
        require_explicit_multi_value = bool(llm_decision.get("require_explicit_multi_value"))
        # Load existing table schema early if table exists, so we can pass it to coercion
        engine = get_engine()
        table_exists = False
        existing_table_schema: Dict[str, str] = {}
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                )
            """), {"table_name": target_table})
            table_exists = result.scalar()

        if table_exists:
            existing_table_schema = _load_existing_table_schema(engine, target_table)
        
        column_type_enforcement_log: Dict[str, Dict[str, Any]] = {}
        if expected_column_types:
            # Pass existing schema to enable smart type coercion (e.g., boolean -> 0/1 for numeric columns)
            records, column_type_enforcement_log = coerce_records_to_expected_types(
                records,
                expected_column_types,
                existing_schema=existing_table_schema if table_exists else None
            )
            logger.info("AUTO-IMPORT: Applied expected column types via pandas: %s", column_type_enforcement_log)
        else:
            logger.info("AUTO-IMPORT: No expected column types provided by LLM; using heuristic inference.")
        
        # USER INSTRUCTION ENFORCEMENT: Filter columns and transformations per user instructions
        # This ensures that even if LLM doesn't perfectly follow instructions, execution layer enforces them
        filtered_mapping, excluded_columns = _filter_column_mapping_per_instruction(
            column_mapping,
            instruction_text,
            records
        )
        
        if excluded_columns:
            logger.info(
                "USER INSTRUCTION ENFORCEMENT: Filtered %d columns from mapping: %s",
                len(excluded_columns),
                excluded_columns
            )
            column_mapping = filtered_mapping
        
        # Filter row transformations to remove explode_columns for filtered data types
        filtered_row_transforms = _filter_row_transformations_per_instruction(
            row_transformations,
            instruction_text,
            excluded_columns
        )
        
        if len(filtered_row_transforms) != len(row_transformations):
            logger.info(
                "USER INSTRUCTION ENFORCEMENT: Removed %d row transformations that violate user instructions",
                len(row_transformations) - len(filtered_row_transforms)
            )
            row_transformations = filtered_row_transforms
        
        # Build MappingConfig using LLM's column mapping
        # IMPORTANT: LLM provides {source_col: target_col} but mapper.py expects {target_col: source_col}
        # We need to INVERT the mapping for mapper.py to work correctly

        if target_table == "clients_list":
            column_mapping = _canonicalize_clients_list_mapping(column_mapping, records)

        (
            column_mapping,
            column_transformations,
            row_transformations,
            exploded_source_columns,
        ) = _synthesize_multi_value_rules(
            column_mapping,
            column_transformations,
            row_transformations,
            records,
            instruction_text,
            multi_value_directives=multi_value_directives,
            require_explicit_multi_value=require_explicit_multi_value,
            import_strategy=strategy,
        )
        
        # CRITICAL FIX: Remove exploded source columns from uniqueness columns
        # When a column is exploded (e.g., 'Emails' -> 'contact_method'), the source column
        # is dropped and should not be used for uniqueness checking
        if exploded_source_columns and unique_columns:
            # Map source column names to their target column names in the mapping
            source_to_target = {src: tgt for src, tgt in column_mapping.items()}
            
            # Filter out uniqueness columns that reference exploded source columns
            original_unique_columns = list(unique_columns)
            unique_columns = [
                col for col in unique_columns 
                if col not in exploded_source_columns and source_to_target.get(col) not in exploded_source_columns
            ]
            
            if len(unique_columns) != len(original_unique_columns):
                removed_columns = set(original_unique_columns) - set(unique_columns)
                logger.info(
                    "AUTO-IMPORT: Removed exploded columns from uniqueness: %s (were being exploded into other columns)",
                    list(removed_columns)
                )
                logger.info(
                    "AUTO-IMPORT: Updated uniqueness columns: %s -> %s",
                    original_unique_columns,
                    unique_columns
                )
        
        # Invert the column_mapping: {source: target} -> {target: source}
        inverted_mapping = {target_col: source_col for source_col, target_col in column_mapping.items()}

        # CRITICAL FIX: Update inverted mapping for transformations that create new target columns
        # This ensures transformations like standardize_phone aren't overwritten by the original source values
        for transformation in column_transformations:
            if not isinstance(transformation, dict):
                continue

            t_type = transformation.get("type")
            if t_type == "standardize_phone":
                source_col = transformation.get("source_column") or transformation.get("column")
                target_col = transformation.get("target_column") or transformation.get("target_field") or source_col

                # If this transformation creates a new target column, update the mapping to read from it
                if target_col in inverted_mapping and inverted_mapping[target_col] == source_col:
                    # Change mapping from {target_col: source_col} to {target_col: target_col}
                    # This tells the mapper to read from the transformed column, not the original
                    inverted_mapping[target_col] = target_col
                    logger.info(
                        "AUTO-IMPORT: Updated mapping for transformed column '%s' to read from transformed value instead of original",
                        target_col
                    )
            elif t_type == "coalesce_columns":
                target_col = transformation.get("target_column") or transformation.get("target_field") or transformation.get("column")
                if target_col and target_col not in inverted_mapping:
                    # Add the new column to the mapping so it gets included in the schema
                    inverted_mapping[target_col] = target_col
                    logger.info(
                        "AUTO-IMPORT: Added new target column '%s' from coalesce_columns transformation",
                        target_col
                    )

        logger.info(f"AUTO-IMPORT: LLM column_mapping (source->target): {column_mapping}")
        logger.info(f"AUTO-IMPORT: Inverted mapping (target->source): {inverted_mapping}")
        
        # Get target columns (keys in inverted_mapping, which were values in original column_mapping)
        target_columns = list(inverted_mapping.keys())

        # Auto-generate validations if explicit ones are missing (OR supplement them)
        # This ensures we catch "Researching..." and invalid formats even if LLM didn't explicitly ask
        if not column_validations:
            column_validations = []
        
        existing_validation_cols = {v.get("column") for v in column_validations}

        for target_col in target_columns:
            # Skip if already has a validation rule
            if target_col in existing_validation_cols:
                continue

            lower_col = target_col.lower()
            
            # 1. Check expected types from LLM (mapped from source)
            source_col = inverted_mapping.get(target_col)
            if source_col and source_col in expected_column_types:
                exp_type = normalize_expected_type(expected_column_types[source_col])
                if exp_type == "BOOLEAN":
                    column_validations.append({"column": target_col, "validator": "boolean"})
                    existing_validation_cols.add(target_col)
                    continue

            # 2. Check semantic names
            if "email" in lower_col or "e-mail" in lower_col:
                column_validations.append({"column": target_col, "validator": "email"})
                existing_validation_cols.add(target_col)
            elif "phone" in lower_col or "mobile" in lower_col:
                column_validations.append({"column": target_col, "validator": "phone"})
                existing_validation_cols.add(target_col)
            elif "postal" in lower_col or "zip" in lower_col:
                column_validations.append({"column": target_col, "validator": "postal_code"})
                existing_validation_cols.add(target_col)

        if column_validations:
            logger.info(f"AUTO-IMPORT: Applied column validations: {json.dumps(column_validations)}")

        rules_payload: Dict[str, Any] = {}
        if column_transformations:
            rules_payload["column_transformations"] = column_transformations
        if row_transformations:
            rules_payload["row_transformations"] = row_transformations
        
        # Extract skip_duplicate_check flag from LLM decision
        # This controls row-level duplicate detection, not file-level
        skip_duplicate_check = bool(llm_decision.get("skip_file_duplicate_check", False))
        
        # Build db_schema prioritizing LLM expectations and falling back to heuristics where absent
        import re
        db_schema: Dict[str, str] = {}
        for target_col in target_columns:
            source_col = next((k for k, v in column_mapping.items() if v == target_col), None)
            schema_type: Optional[str] = None

            if source_col:
                source_expected = expected_column_types.get(source_col)
                if source_expected:
                    schema_type = normalize_expected_type(source_expected)

            if not schema_type:
                if source_col and records:
                    sample_values = [r.get(source_col) for r in records[:100] if r.get(source_col) is not None]
                    subset = sample_values[:100]
                    sample_str = [str(v) for v in subset]

                    phone_patterns = [
                        r'^\d{3}\.\d{3}\.\d{4}$',
                        r'^\d{3}-\d{3}-\d{4}$',
                        r'^\(\d{3}\)\s*\d{3}-\d{4}$',
                        r'^\d{3}\s+\d{3}\s+\d{4}$',
                        r'^\+?\d{1,3}[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',
                    ]
                    is_phone = any(re.match(pattern, s) for pattern in phone_patterns for s in sample_str)

                    all_numeric = bool(subset) and all(_is_numeric_like(v) for v in subset)

                    if is_phone or any('%' in s for s in sample_str) or any('@' in s for s in sample_str[:10]):
                        schema_type = "TEXT"
                    elif all_numeric:
                        if all(_is_integer_like(v) for v in subset):
                            INT32_MAX = 2_147_483_647
                            max_abs = max(abs(int(float(str(v)))) for v in subset if _is_integer_like(v))
                            schema_type = "BIGINT" if max_abs > INT32_MAX else "INTEGER"
                        else:
                            schema_type = "DECIMAL"
                    else:
                        parsed_samples = [parse_flexible_date(val, log_failures=False) for val in subset]
                        successful_parses = [ps for ps in parsed_samples if ps is not None]

                        if successful_parses and len(successful_parses) >= max(1, len(subset) // 2):
                            schema_type = "TIMESTAMP"
                        else:
                            schema_type = "TEXT"
                else:
                    schema_type = "TEXT"

            db_schema[target_col] = schema_type or "TEXT"
        
        logger.info(f"AUTO-IMPORT: Resolved schema types: {db_schema}")
        
        # Use the schema we already loaded earlier
        final_table_schema: Dict[str, str] = {}
        if table_exists:
            final_table_schema = dict(existing_table_schema)
        else:
            final_table_schema = dict(db_schema)
        
        # Stabilize uniqueness columns for duplicate checks when the table already exists.
        # Default to the last successful uniqueness set to keep dedupe behaviour stable.
        # Allow explicit override via llm_decision.get("allow_unique_override").
        effective_unique_columns = unique_columns or []
        allow_unique_override = bool(llm_decision.get("allow_unique_override"))
        if table_exists:
            previous_unique = _load_previous_uniqueness_columns(engine, target_table)
            if previous_unique:
                if allow_unique_override and effective_unique_columns and set(previous_unique) != set(effective_unique_columns):
                    logger.info(
                        "AUTO-IMPORT: LLM override requested; using new uniqueness columns %s instead of previous %s",
                        effective_unique_columns,
                        previous_unique,
                    )
                else:
                    if set(previous_unique) != set(effective_unique_columns):
                        logger.info(
                            "AUTO-IMPORT: Keeping existing uniqueness columns %s and ignoring proposed %s to preserve deduplication",
                            previous_unique,
                            effective_unique_columns or [],
                        )
                    effective_unique_columns = previous_unique

        schema_migrations = llm_decision.get("schema_migrations") or []
        if table_exists:
            already_targeted = _extract_columns_from_migrations(schema_migrations)
            auto_alignment = _build_alignment_migrations(
                existing_table_schema,
                db_schema,
                already_targeted,
            )
            if auto_alignment:
                logger.info(
                    "AUTO-IMPORT: Generated %d schema alignment migration(s) for table '%s'",
                    len(auto_alignment),
                    target_table,
                )
                schema_migrations = schema_migrations + auto_alignment
        if schema_migrations:
            if table_exists:
                try:
                    schema_migration_results = apply_schema_migrations(
                        engine, target_table, schema_migrations
                    )
                    logger.info(
                        "AUTO-IMPORT: Applied schema migrations: %s",
                        schema_migration_results,
                    )
                except SchemaMigrationError as exc:
                    logger.error(
                        "AUTO-IMPORT: Schema migration failed: %s", exc, exc_info=True
                    )
                    return {
                        "success": False,
                        "error": str(exc),
                        "strategy_attempted": strategy,
                        "target_table": target_table,
                    }
            else:
                logger.warning(
                    "AUTO-IMPORT: Requested schema migrations for non-existent table '%s'; skipping",
                    target_table,
                )

        if table_exists:
            final_table_schema = _load_existing_table_schema(engine, target_table)

        # Ensure uniqueness columns align with the target schema (mapping + pluralization)
        target_columns = list((final_table_schema or db_schema or {}).keys())
        normalized_uniques = _normalize_uniqueness_columns(
            effective_unique_columns,
            column_mapping,
            target_columns,
        )
        if normalized_uniques != effective_unique_columns:
            logger.info(
                "AUTO-IMPORT: Normalized uniqueness columns from %s to %s based on mapping and table schema",
                effective_unique_columns,
                normalized_uniques,
            )
        effective_unique_columns = normalized_uniques

        if table_exists and strategy in ["MERGE_EXACT", "ADAPT_DATA", "EXTEND_TABLE"]:
            logger.info(f"AUTO-IMPORT: Table '{target_table}' exists, will merge into it")
            # For merging, we only need the mappings, not the schema
            # The existing table schema will be used
            mapping_config = MappingConfig(
                table_name=target_table,
                db_schema=final_table_schema or existing_table_schema or {},
                mappings=inverted_mapping,  # Use inverted mapping (target->source)
                rules=rules_payload,
                column_validations=column_validations,
                unique_columns=effective_unique_columns,  # For duplicate detection (legacy)
                duplicate_check=DuplicateCheckConfig(
                    enabled=not skip_duplicate_check,  # Disable duplicate checking entirely if flag is set
                    check_file_level=True,  # Always check file-level duplicates
                    allow_file_level_retry=False,
                    allow_duplicates=skip_duplicate_check,  # Allow row duplicates if flag is set
                    uniqueness_columns=effective_unique_columns  # This is what duplicate checking actually uses
                )
            )
        else:
            # For new tables, use the inferred schema
            logger.info(f"AUTO-IMPORT: Creating new table '{target_table}' with inferred schema")
            mapping_config = MappingConfig(
                table_name=target_table,
                db_schema=db_schema,
                mappings=inverted_mapping,  # Use inverted mapping (target->source)
                rules=rules_payload,
                column_validations=column_validations,
                unique_columns=effective_unique_columns,  # For duplicate detection (legacy)
                duplicate_check=DuplicateCheckConfig(
                    enabled=not skip_duplicate_check,  # Disable duplicate checking entirely if flag is set
                    check_file_level=True,  # Always check file-level duplicates
                    allow_file_level_retry=False,
                    allow_duplicates=skip_duplicate_check,  # Allow row duplicates if flag is set
                    uniqueness_columns=effective_unique_columns  # This is what duplicate checking actually uses
                )
            )
        
        logger.info(f"AUTO-IMPORT: Created MappingConfig:")
        logger.info(f"  Table: {mapping_config.table_name}")
        logger.info(f"  Mappings: {mapping_config.mappings}")
        logger.info(f"  Unique Columns: {mapping_config.unique_columns}")
        
        # Log transformation details for debugging
        if column_transformations:
            logger.info(f"  Column Transformations: {json.dumps(column_transformations, indent=2)}")
        if row_transformations:
            logger.info(f"  Row Transformations: {json.dumps(row_transformations, indent=2)}")
        
        # Prepare metadata info
        metadata_info = {
            "purpose_short": llm_decision.get("purpose_short", "Data imported from file"),
            "data_domain": llm_decision.get("data_domain"),
            "key_entities": llm_decision.get("key_entities", [])
        }
        
        logger.info(f"AUTO-IMPORT: Calling execute_data_import with strategy: {strategy}")
        
        # Execute unified import with pre-parsed records
        result = execute_data_import(
            file_content=file_content,
            file_name=file_name,
            mapping_config=mapping_config,
            source_type="b2_storage" if source_path else "local_upload",
            source_path=source_path,
            import_strategy=strategy,
            metadata_info=metadata_info,
            pre_parsed_records=records,  # Use records parsed according to LLM instructions
            pre_mapped=False  # Records need to be mapped using column_mapping
        )
        
        logger.info(f"AUTO-IMPORT: Import completed successfully")
        logger.info(f"  Records processed: {result['records_processed']}")
        logger.info(f"  Table: {result['table_name']}")
        
        # Update schema fingerprint for intelligent matching of future files
        try:
            table_columns = list(mapping_config.db_schema.keys())
            engine = get_engine()
            store_table_fingerprint(engine, target_table, table_columns)
        except Exception as e:
            logger.warning(f"AUTO-IMPORT: Failed to update table fingerprint: {e}")

        return {
            "success": True,
            "strategy_executed": strategy,
            "table_name": target_table,
            "records_processed": result["records_processed"],
            "duplicates_skipped": result.get("duplicates_skipped", 0),
            "duplicate_rows": result.get("duplicate_rows"),
            "duplicate_rows_count": result.get("duplicate_rows_count"),
            "import_id": result.get("import_id"),
            "mapping_errors": result.get("mapping_errors", []),
            "type_mismatch_summary": result.get("type_mismatch_summary", []),
            "llm_followup": result.get("llm_followup"),
            "needs_user_input": result.get("needs_user_input"),
            "schema_migration_results": schema_migration_results,
            "validation_errors": result.get("validation_errors"),
        }
        
    except Exception as e:
        logger.error(f"Error executing LLM import decision: {str(e)}", exc_info=True)
        
        return {
            "success": False,
            "error": str(e),
            "strategy_attempted": llm_decision.get("strategy"),
            "target_table": llm_decision.get("target_table")
        }


def _mentions_multi_value_instruction(instruction_text: str) -> bool:
    """
    Very small heuristic to decide if the user asked for one-value-per-row logic.

    We intentionally keep this loose so we only synthesize rules when the user
    explicitly hints at multi-value handling.
    """
    if not instruction_text:
        return False
    lowered = instruction_text.lower()
    keywords = [
        "multiple",
        "one per",
        "explode",
        "split",
        "separate",
        "per row",
        "each",
    ]
    return any(keyword in lowered for keyword in keywords)


def _looks_like_email_column(source_column: str, target_column: str, records: List[Dict[str, Any]]) -> bool:
    """Heuristic to decide whether a column likely contains emails."""
    name_hints = ("email", "e-mail", "mail")
    if any(hint in str(source_column).lower() for hint in name_hints):
        return True
    if any(hint in str(target_column).lower() for hint in name_hints):
        return True

    for record in records[:100]:
        value = record.get(source_column)
        if isinstance(value, str) and "@" in value:
            return True
        if isinstance(value, list) and any(isinstance(item, str) and "@" in item for item in value):
            return True
    return False


def _canonicalize_clients_list_mapping(
    column_mapping: Dict[str, str],
    records: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Pass-through for clients_list mappings.

    We avoid any hard-coded column remapping or auto-adding inferred fields so the
    caller's mapping stays untouched and can align to whatever schema the user expects.
    """
    return dict(column_mapping)






def _resolve_delimiter(delimiter: Optional[str]) -> str:
    """Map human-friendly delimiter hints to actual separators."""
    if not delimiter:
        return ","
    normalized = delimiter.strip().lower()
    if normalized in {"comma", ",", "","default"}:
        return ","
    if normalized in {"semicolon", "semi-colon", ";"}:
        return ";"
    if normalized in {"pipe", "|"}:
        return "|"
    if normalized in {"tab", "\\t"}:
        return "\t"
    if normalized in {"space", " "}:
        return " "
    return delimiter


def _directive_outputs(source_column: str, directive: Dict[str, Any], max_items: int = 5) -> List[str]:
    """Build output column names for a directive-driven split."""
    explicit = directive.get("outputs")
    if explicit and isinstance(explicit, list):
        return [str(name) for name in explicit if name]
    return [f"{source_column}_item_{idx+1}" for idx in range(max_items)]


def _apply_multi_value_directives(
    directives: List[Dict[str, Any]],
    available_columns: Set[str],
    column_mapping: Dict[str, str],
    column_transformations: List[Dict[str, Any]],
    row_transformations: List[Dict[str, Any]],
) -> tuple[Dict[str, str], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Apply explicit directives supplied by the LLM/user."""
    updated_mapping = dict(column_mapping)
    updated_col_xforms = list(column_transformations)
    updated_row_xforms = list(row_transformations)

    for directive in directives or []:
        if not isinstance(directive, dict):
            continue
        source_col = directive.get("source_column")
        if not source_col:
            continue
        if source_col not in available_columns:
            logger.info("MULTI-VALUE: Skipping directive; source column '%s' not found", source_col)
            continue
        target_col = directive.get("target_column") or column_mapping.get(source_col) or source_col
        delimiter = _resolve_delimiter(directive.get("delimiter"))
        outputs = _directive_outputs(source_col, directive, max_items=int(directive.get("max_items") or 5))

        updated_col_xforms.append(
            {
                "type": "split_multi_value_column",
                "source_column": source_col,
                "delimiter": delimiter,
                "outputs": [{"name": name, "index": idx} for idx, name in enumerate(outputs)],
            }
        )
        updated_row_xforms.append(
            {
                "type": "explode_columns",
                "source_columns": outputs,
                "target_column": target_col,
                "drop_source_columns": True,
                "strip_whitespace": True,
                "dedupe_values": True,
                "case_insensitive_dedupe": True,
            }
        )

        # Rewrite mapping to read from the exploded target
        for mapped_source, mapped_target in list(updated_mapping.items()):
            if mapped_target == target_col or mapped_target == source_col:
                updated_mapping.pop(mapped_source, None)
        updated_mapping[target_col] = target_col

    return updated_mapping, updated_col_xforms, updated_row_xforms


def _synthesize_multi_value_rules(
    column_mapping: Dict[str, str],
    column_transformations: List[Dict[str, Any]],
    row_transformations: List[Dict[str, Any]],
    records: List[Dict[str, Any]],
    instruction_text: str,
    *,
    multi_value_directives: Optional[List[Dict[str, Any]]] = None,
    require_explicit_multi_value: bool = True,
    import_strategy: Optional[str] = None,
) -> tuple[Dict[str, str], List[Dict[str, Any]], List[Dict[str, Any]], Set[str]]:
    """
    Process multi-value transformation rules by trusting the LLM's explicit directives.
    - For NEW_TABLE strategy: Do NOT apply multi-value explosion by default
    - For MERGE/EXTEND/ADAPT: Only apply if explicitly requested by user or LLM
    
    This function has been simplified to ONLY apply explicit directives provided by the LLM.
    It no longer performs automatic sibling detection or pattern-based explosion, which was
    causing incorrect record multiplication.
    
    The LLM has full context of:
    - User instructions
    - Available columns
    - Data samples
    
    Therefore, we trust the LLM's decision and execute it as-is without "smart" modifications.
    
    Multi-value explosion will only occur when:
    1. Explicit multi_value_directives are provided by the LLM
    2. Explicit row_transformations with explode_columns are provided by the LLM
    
    Args:
        import_strategy: The import strategy (NEW_TABLE, MERGE_EXACT, etc.) to help
                        determine if multi-value processing should be applied
    """
    if not records or not column_mapping:
        return column_mapping, column_transformations, row_transformations, set()

    available_columns: Set[str] = set()
    for record in records[:50]:
        available_columns.update(record.keys())

    # Apply explicit directives from the LLM
    if multi_value_directives:
        logger.info(
            "AUTO-IMPORT: Applying %d explicit multi-value directive(s) from LLM",
            len(multi_value_directives)
        )
        column_mapping, column_transformations, row_transformations = _apply_multi_value_directives(
            multi_value_directives,
            available_columns,
            column_mapping,
            column_transformations,
            row_transformations,
        )

    # Track columns that will be created by column_transformations
    # These columns need to be available for row_transformations
    columns_created_by_transforms: Set[str] = set()
    for ct in column_transformations:
        if not isinstance(ct, dict):
            continue
        if ct.get("type") == "split_multi_value_column":
            outputs = ct.get("outputs") or []
            for output in outputs:
                if isinstance(output, dict) and output.get("name"):
                    columns_created_by_transforms.add(output["name"])
    
    # Expand available columns to include those created by column transformations
    available_columns_with_transforms = available_columns | columns_created_by_transforms
    
    logger.info(
        "AUTO-IMPORT: Available columns for row transformations: %d original + %d created = %d total",
        len(available_columns),
        len(columns_created_by_transforms),
        len(available_columns_with_transforms)
    )

    # Process any explode_columns transformations already in row_transformations
    # These come from the LLM and should be respected as-is
    updated_mapping = dict(column_mapping)
    exploded_targets: Set[str] = set()
    exploded_source_columns: Set[str] = set()
    
    for rt in row_transformations:
        if not isinstance(rt, dict) or rt.get("type") != "explode_columns":
            continue
        
        target = rt.get("target_column") or rt.get("target_field") or rt.get("column")
        if not target:
            continue
            
        raw_sources = list(rt.get("source_columns") or rt.get("columns") or [])
        
        # Validate that source columns exist (including those created by column transformations)
        existing_sources = [col for col in raw_sources if col in available_columns_with_transforms]
        
        if not existing_sources:
            logger.warning(
                "AUTO-IMPORT: explode_columns for target '%s' has no valid source columns. "
                "Requested: %s, Available: %s",
                target,
                raw_sources,
                list(available_columns)[:10]
            )
            continue
        
        # Use exactly the columns the LLM specified (no expansion, no modification)
        rt["source_columns"] = existing_sources
        
        logger.info(
            "AUTO-IMPORT: Respecting LLM explode_columns decision: %s -> %s",
            existing_sources,
            target
        )
        
        # Track which source columns are being exploded
        exploded_source_columns.update(existing_sources)
        
        # Update mapping to point to the exploded target
        for mapped_source in existing_sources:
            if mapped_source in updated_mapping:
                updated_mapping.pop(mapped_source)
        
        updated_mapping[target] = target
        exploded_targets.add(target)
    
    # Ensure exploded targets are present in mapping
    for target_col in exploded_targets:
        updated_mapping[target_col] = target_col
    
    logger.info(
        "AUTO-IMPORT: Multi-value synthesis complete. Exploded targets: %s",
        list(exploded_targets) if exploded_targets else "none"
    )
    
    return updated_mapping, column_transformations, row_transformations, exploded_source_columns

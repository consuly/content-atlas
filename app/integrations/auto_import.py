"""
Auto-import execution logic for LLM-analyzed files.

This module handles the execution of import strategies recommended by the LLM agent.
"""

from typing import Dict, Any, List, Optional, Set
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

    for value in sample_values.astype(str).head(25):
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


def _build_alignment_migrations(
    existing_schema: Dict[str, str],
    desired_schema: Dict[str, str],
    already_targeted: Set[str],
) -> List[Dict[str, Any]]:
    """Generate replace_column migrations for columns whose types need to change."""
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
    expected_types: Dict[str, str]
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Use pandas to coerce the incoming records to the types the LLM determined.
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


def execute_llm_import_decision(
    file_content: bytes,
    file_name: str,
    all_records: List[Dict[str, Any]],
    llm_decision: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Execute an import based on LLM's decision.
    
    Args:
        file_content: Raw file content
        file_name: Name of the file
        all_records: All records from the file (not just sample)
        llm_decision: LLM's decision with strategy, target_table, column_mapping, etc.
        
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
        instruction_text = llm_decision.get("llm_instruction") or ""
        multi_value_directives = llm_decision.get("multi_value_directives") or []
        require_explicit_multi_value = bool(llm_decision.get("require_explicit_multi_value"))
        column_type_enforcement_log: Dict[str, Dict[str, Any]] = {}
        if expected_column_types:
            records, column_type_enforcement_log = coerce_records_to_expected_types(
                records,
                expected_column_types
            )
            logger.info("AUTO-IMPORT: Applied expected column types via pandas: %s", column_type_enforcement_log)
        else:
            logger.info("AUTO-IMPORT: No expected column types provided by LLM; using heuristic inference.")
        
        # Build MappingConfig using LLM's column mapping
        # IMPORTANT: LLM provides {source_col: target_col} but mapper.py expects {target_col: source_col}
        # We need to INVERT the mapping for mapper.py to work correctly

        if target_table == "clients_list":
            column_mapping = _canonicalize_clients_list_mapping(column_mapping, records)

        (
            column_mapping,
            column_transformations,
            row_transformations,
        ) = _synthesize_multi_value_rules(
            column_mapping,
            column_transformations,
            row_transformations,
            records,
            instruction_text,
            multi_value_directives=multi_value_directives,
            require_explicit_multi_value=require_explicit_multi_value,
        )
        
        # Invert the column_mapping: {source: target} -> {target: source}
        inverted_mapping = {target_col: source_col for source_col, target_col in column_mapping.items()}
        
        logger.info(f"AUTO-IMPORT: LLM column_mapping (source->target): {column_mapping}")
        logger.info(f"AUTO-IMPORT: Inverted mapping (target->source): {inverted_mapping}")
        
        # Get target columns (keys in inverted_mapping, which were values in original column_mapping)
        target_columns = list(inverted_mapping.keys())

        rules_payload: Dict[str, Any] = {}
        if column_transformations:
            rules_payload["column_transformations"] = column_transformations
        if row_transformations:
            rules_payload["row_transformations"] = row_transformations
        
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
                    subset = sample_values[:20]
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
        
        # IMPORTANT: For merging into existing tables, we need to check if table exists
        # and use its schema instead of creating a new one
        engine = get_engine()
        table_exists = False
        existing_table_schema: Dict[str, str] = {}
        final_table_schema: Dict[str, str] = {}
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
            final_table_schema = dict(existing_table_schema)
        else:
            final_table_schema = dict(db_schema)
        
        # Stabilize uniqueness columns for duplicate checks when the table already exists.
        effective_unique_columns = unique_columns
        if table_exists:
            previous_unique = _load_previous_uniqueness_columns(engine, target_table)
            if previous_unique:
                effective_unique_columns = previous_unique
                logger.info(
                    "AUTO-IMPORT: Using previous uniqueness columns for duplicate detection: %s",
                    effective_unique_columns,
                )

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

        if table_exists and strategy in ["MERGE_EXACT", "ADAPT_DATA"]:
            logger.info(f"AUTO-IMPORT: Table '{target_table}' exists, will merge into it")
            # For merging, we only need the mappings, not the schema
            # The existing table schema will be used
            mapping_config = MappingConfig(
                table_name=target_table,
                db_schema=final_table_schema or existing_table_schema or {},
                mappings=inverted_mapping,  # Use inverted mapping (target->source)
                rules=rules_payload,
                unique_columns=effective_unique_columns,  # For duplicate detection (legacy)
                duplicate_check=DuplicateCheckConfig(
                    enabled=True,
                    check_file_level=True,
                    allow_duplicates=False,
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
                unique_columns=effective_unique_columns,  # For duplicate detection (legacy)
                duplicate_check=DuplicateCheckConfig(
                    enabled=True,
                    check_file_level=True,
                    allow_duplicates=False,
                    uniqueness_columns=effective_unique_columns  # This is what duplicate checking actually uses
                )
            )
        
        logger.info(f"AUTO-IMPORT: Created MappingConfig:")
        logger.info(f"  Table: {mapping_config.table_name}")
        logger.info(f"  Mappings: {mapping_config.mappings}")
        logger.info(f"  Unique Columns: {mapping_config.unique_columns}")
        
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
            source_type="local_upload",
            import_strategy=strategy,
            metadata_info=metadata_info,
            pre_parsed_records=records,  # Use records parsed according to LLM instructions
            pre_mapped=False  # Records need to be mapped using column_mapping
        )
        
        logger.info(f"AUTO-IMPORT: Import completed successfully")
        logger.info(f"  Records processed: {result['records_processed']}")
        logger.info(f"  Table: {result['table_name']}")
        
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

    for record in records[:25]:
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


def _find_numbered_siblings(source_column: str, available_columns: Set[str]) -> List[str]:
    """
    Detect sibling columns that share a base name with numeric suffixes.
    Example: Email 1, Email 2, Email_3.
    """
    def _base(name: str) -> str:
        return re.sub(r"[\s_-]*\d+$", "", name).strip().lower()

    base = _base(source_column)
    if not base:
        return []

    def _suffix_int(name: str) -> int:
        m = re.search(r"(\\d+)$", name)
        try:
            return int(m.group(1)) if m else 0
        except Exception:
            return 0

    candidates: List[tuple[int, str]] = []
    for col in available_columns:
        if _base(col) != base:
            continue
        candidates.append((_suffix_int(col), col))

    candidates.sort(key=lambda pair: pair[0])
    return [col for _, col in candidates]


def _detect_delimited_values(records: List[Dict[str, Any]], column: str) -> Dict[str, Any]:
    """
    Look for comma/semicolon-delimited or JSON-array values to infer multi-value columns.
    """
    max_items = 1
    saw_multi = False
    email_pattern = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+", re.IGNORECASE)
    for record in records[:50]:
        value = record.get(column)
        if value is None:
            continue
        if isinstance(value, list):
            max_items = max(max_items, len(value))
            saw_multi = saw_multi or len(value) > 1
            continue
        if not isinstance(value, str):
            continue
        text = value.strip()
        if not text:
            continue
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                max_items = max(max_items, len(parsed))
                saw_multi = saw_multi or len(parsed) > 1
                continue
        except Exception:
            pass
        # Count email-like tokens even when separated by whitespace or uncommon delimiters.
        email_tokens = email_pattern.findall(text)
        if len(email_tokens) > 1:
            max_items = max(max_items, len(email_tokens))
            saw_multi = True
            continue
        parts = [p.strip() for p in re.split(r"[;,]", text) if p.strip()]
        if len(parts) > 1:
            max_items = max(max_items, len(parts))
            saw_multi = True
    return {"max_items": max_items, "saw_multi": saw_multi}


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
    require_explicit_multi_value: bool = False,
) -> tuple[Dict[str, str], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    If the user instruction asks for multi-value rows to be split/one-per-row (or we
    detect obvious numbered sources), synthesize column/row transforms and rewrite the
    mapping to point to the exploded column so mapper.py sees the right source.
    """
    if not records or not column_mapping:
        return column_mapping, column_transformations, row_transformations

    instruction_flags_multi = _mentions_multi_value_instruction(instruction_text)
    has_numbered_sources = any(re.search(r"\d+$", src) for src in column_mapping.keys())

    available_columns: Set[str] = set()
    for record in records[:50]:
        available_columns.update(record.keys())

    # Honor explicit directives first
    if multi_value_directives:
        column_mapping, column_transformations, row_transformations = _apply_multi_value_directives(
            multi_value_directives,
            available_columns,
        column_mapping,
        column_transformations,
        row_transformations,
    )

    # Group sources by target to detect fan-in mappings (multiple sources -> same target).
    targets_to_sources: Dict[str, List[str]] = {}
    for src, tgt in column_mapping.items():
        if not tgt:
            continue
        targets_to_sources.setdefault(tgt, []).append(src)

    fan_in_present = any(len(sources) > 1 for sources in targets_to_sources.values())
    numbered_sources_present = any(_find_numbered_siblings(src, available_columns) for src in column_mapping.keys())
    # When explicit mode is on, still honor a direct instruction OR obvious multiple-source mapping (fan-in or numbered siblings).
    # This keeps user-supplied instructions (e.g., "split multiple emails into one per row") working
    # while avoiding implicit auto-detection if no such instruction is present.
    if (
        require_explicit_multi_value
        and not instruction_flags_multi
        and not multi_value_directives
        and not fan_in_present
        and not numbered_sources_present
    ):
        return column_mapping, column_transformations, row_transformations

    if not instruction_flags_multi and not has_numbered_sources:
        return column_mapping, column_transformations, row_transformations

    updated_mapping = dict(column_mapping)
    updated_col_xforms = list(column_transformations)
    updated_row_xforms = list(row_transformations)

    existing_explodes = {
        rt.get("target_column") or rt.get("target_field") or rt.get("column")
        for rt in row_transformations
        if isinstance(rt, dict) and rt.get("type") == "explode_columns"
    }

    exploded_targets: Set[str] = set()

    # Group sources by target to detect fan-in mappings (multiple sources -> same target).
    targets_to_sources: Dict[str, List[str]] = {}
    for src, tgt in column_mapping.items():
        if not tgt:
            continue
        targets_to_sources.setdefault(tgt, []).append(src)

    # If the LLM already supplied an explode, ensure mapping points to that target and merge sources.
    for rt in updated_row_xforms:
        if not isinstance(rt, dict) or rt.get("type") != "explode_columns":
            continue
        target = rt.get("target_column") or rt.get("target_field") or rt.get("column")
        if not target:
            continue
        sources = set(rt.get("source_columns") or rt.get("columns") or [])
        mapped_sources = set(targets_to_sources.get(target, []))
        if mapped_sources:
            sources |= mapped_sources
        # If no mapped sources are known, try to pull siblings that look like this target.
        if not sources and target:
            sources = set(
                col
                for col in available_columns
                if target.lower() in str(col).lower()
            )
        # Keep only existing columns as explode sources; drop unknowns to avoid empty results.
        existing_sources = [col for col in sorted(sources, key=lambda c: str(c).lower()) if col in available_columns]
        if existing_sources:
            rt["source_columns"] = existing_sources
        else:
            rt["source_columns"] = []
        for mapped_source, mapped_target in list(updated_mapping.items()):
            if mapped_target == target or mapped_source in rt["source_columns"]:
                updated_mapping.pop(mapped_source, None)
        updated_mapping[target] = target
        exploded_targets.add(target)

    for source_col, target_col in list(column_mapping.items()):
        if not target_col:
            continue
        if target_col in existing_explodes:
            continue

        sibling_sources = _find_numbered_siblings(source_col, available_columns)
        delimited = _detect_delimited_values(records, source_col)

        sources_to_use: List[str] = []
        need_split = False
        split_outputs: List[str] = []
        transform_added = False

        explode_target = target_col
        # If the target looks like a numbered field (email_1, phone_2), collapse to base.
        m_target_num = re.match(r"^(.*?)[\s_-]?(\d+)$", target_col)
        if m_target_num:
            explode_target = m_target_num.group(1) or target_col
            explode_target = explode_target.strip("_ ").lower()
            if not explode_target:
                explode_target = target_col

        fan_in_sources = targets_to_sources.get(target_col, [])

        if sibling_sources and len(sibling_sources) > 1:
            # Always include the mapped source column first for stability
            sources_to_use = sorted({source_col, *sibling_sources}, key=lambda c: c.lower())
            # Also pull in non-numbered columns that clearly belong to the same base (e.g., "Primary Email")
            if explode_target:
                related = {
                    col
                    for col in available_columns
                    if explode_target in str(col).lower()
                }
                sources_to_use = sorted(set(sources_to_use) | related, key=lambda c: c.lower())
            transform_added = True
        elif fan_in_sources and len(fan_in_sources) > 1:
            # Multiple sources mapped to the same target; explode them.
            sources_to_use = sorted(
                {
                    src
                    for src in fan_in_sources
                    if "validation" not in src.lower() and "total ai" not in src.lower()
                },
                key=lambda c: c.lower(),
            )
            transform_added = True
        elif delimited.get("saw_multi"):
            if not _looks_like_email_column(source_col, target_col, records):
                # Avoid synthesizing split/explode for non-email fields (e.g., location strings with commas).
                continue
            need_split = True
            max_items = max(2, min(delimited.get("max_items", 1), 10))
            split_outputs = [f"{source_col}_item_{idx+1}" for idx in range(max_items)]
            sources_to_use = split_outputs
            transform_added = True
        elif instruction_flags_multi and _looks_like_email_column(source_col, target_col, records):
            # Single-column email case with explicit instruction: still route through explode so
            # downstream mapping targets the canonical email column.
            sources_to_use = [source_col]
            transform_added = True
        else:
            continue

        if need_split:
            updated_col_xforms.append(
                {
                    "type": "split_multi_value_column",
                    "source_column": source_col,
                    "outputs": [
                        {"name": name, "index": idx}
                        for idx, name in enumerate(split_outputs)
                    ],
                }
            )

        updated_row_xforms.append(
            {
                "type": "explode_columns",
                "source_columns": sources_to_use,
                "target_column": explode_target,
                "drop_source_columns": True,
                "strip_whitespace": True,
                "dedupe_values": True,
                "case_insensitive_dedupe": True,
            }
        )

        # Rewrite mapping so mapper.py reads from the exploded column name.
        if transform_added:
            for mapped_source, mapped_target in list(updated_mapping.items()):
                if (
                    mapped_target == target_col
                    or mapped_target == explode_target
                    or str(mapped_target).startswith(f"{explode_target}_")
                ):
                    updated_mapping.pop(mapped_source, None)
            # Preserve the exploded target but do not overwrite the source list; mapping should point to the target.
            updated_mapping[explode_target] = explode_target
            exploded_targets.add(explode_target)

    if instruction_flags_multi and not exploded_targets:
        skip_tokens = ("validation", "status", "total ai")
        email_candidates = [
            col
            for col in sorted(available_columns, key=lambda c: str(c).lower())
            if _looks_like_email_column(col, column_mapping.get(col) or col, records)
            and not any(token in str(col).lower() for token in skip_tokens)
        ]
        if len(email_candidates) > 1:
            explode_target = next(
                (
                    tgt
                    for tgt in updated_mapping.values()
                    if _looks_like_email_column("", tgt, records)
                ),
                None,
            )
            if not explode_target:
                explode_target = next(
                    (tgt for tgt in updated_mapping.keys() if "email" in str(tgt).lower()),
                    None,
                )
            explode_target = explode_target or "email"
            updated_row_xforms.append(
                {
                    "type": "explode_columns",
                    "source_columns": email_candidates,
                    "target_column": explode_target,
                    "drop_source_columns": True,
                    "strip_whitespace": True,
                    "dedupe_values": True,
                    "case_insensitive_dedupe": True,
                }
            )
            for mapped_source, mapped_target in list(updated_mapping.items()):
                if mapped_target == explode_target or mapped_source in email_candidates:
                    updated_mapping.pop(mapped_source, None)
            updated_mapping[explode_target] = explode_target
            exploded_targets.add(explode_target)

    for target_col in exploded_targets:
        # Ensure exploded targets are present and ordered last so inversion prefers them
        updated_mapping.pop(target_col, None)
        updated_mapping[target_col] = target_col

    if instruction_flags_multi and exploded_targets:
        skip_tokens = ("validation", "status", "total ai")
        email_candidates = [
            col
            for col in sorted(available_columns, key=lambda c: str(c).lower())
            if _looks_like_email_column(col, column_mapping.get(col) or col, records)
            and not any(token in str(col).lower() for token in skip_tokens)
        ]
        if len(email_candidates) > 1:
            for rt in updated_row_xforms:
                if not isinstance(rt, dict) or rt.get("type") != "explode_columns":
                    continue
                target = rt.get("target_column")
                if target not in exploded_targets:
                    continue
                sources = rt.get("source_columns") or []
                merged = sorted(
                    {src for src in sources if src in available_columns} | set(email_candidates),
                    key=lambda c: str(c).lower(),
                )
                rt["source_columns"] = merged

    return updated_mapping, updated_col_xforms, updated_row_xforms

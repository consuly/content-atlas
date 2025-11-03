"""
Auto-import execution logic for LLM-analyzed files.

This module handles the execution of import strategies recommended by the LLM agent.
"""

from typing import Dict, Any, List, Optional
from sqlalchemy import text, inspect
from app.domain.imports.mapper import detect_mapping_from_file
from app.db.models import create_table_if_not_exists, insert_records, calculate_file_hash
from app.db.session import get_engine
from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig
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
    "bigint": "INTEGER",
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

_SUPPORTED_TYPES = {"TEXT", "DECIMAL", "INTEGER", "TIMESTAMP", "DATE", "BOOLEAN"}


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
            if normalized_type in {"DECIMAL", "INTEGER"}:
                converted = pd.to_numeric(series, errors="coerce")
                coerced_count = int((series.notna() & converted.isna()).sum())
                column_summary["status"] = "converted"
                if coerced_count:
                    column_summary["coerced_values"] = coerced_count
                df[source_col] = converted
            elif normalized_type in {"TIMESTAMP", "DATE"}:
                converted = pd.to_datetime(series, errors="coerce", utc=False)
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
        column_mapping = llm_decision.get("column_mapping", {})
        unique_columns = llm_decision.get("unique_columns", [])
        has_header = llm_decision.get("has_header")
        
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
        
        # Invert the column_mapping: {source: target} -> {target: source}
        inverted_mapping = {target_col: source_col for source_col, target_col in column_mapping.items()}
        
        logger.info(f"AUTO-IMPORT: LLM column_mapping (source->target): {column_mapping}")
        logger.info(f"AUTO-IMPORT: Inverted mapping (target->source): {inverted_mapping}")
        
        # Get target columns (keys in inverted_mapping, which were values in original column_mapping)
        target_columns = list(inverted_mapping.keys())
        
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
                            schema_type = "INTEGER"
                        else:
                            schema_type = "DECIMAL"
                    else:
                        parsed_samples = [parse_flexible_date(val) for val in subset]
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
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                )
            """), {"table_name": target_table})
            table_exists = result.scalar()
        
        schema_migrations = llm_decision.get("schema_migrations") or []
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

        if table_exists and strategy in ["MERGE_EXACT", "ADAPT_DATA"]:
            logger.info(f"AUTO-IMPORT: Table '{target_table}' exists, will merge into it")
            # For merging, we only need the mappings, not the schema
            # The existing table schema will be used
            mapping_config = MappingConfig(
                table_name=target_table,
                db_schema={},  # Empty - will use existing table schema
                mappings=inverted_mapping,  # Use inverted mapping (target->source)
                rules={"column_transformations": column_transformations} if column_transformations else {},
                unique_columns=unique_columns,  # For duplicate detection (legacy)
                duplicate_check=DuplicateCheckConfig(
                    enabled=True,
                    check_file_level=True,
                    allow_duplicates=False,
                    uniqueness_columns=unique_columns  # This is what duplicate checking actually uses
                )
            )
        else:
            # For new tables, use the inferred schema
            logger.info(f"AUTO-IMPORT: Creating new table '{target_table}' with inferred schema")
            mapping_config = MappingConfig(
                table_name=target_table,
                db_schema=db_schema,
                mappings=inverted_mapping,  # Use inverted mapping (target->source)
                rules={"column_transformations": column_transformations} if column_transformations else {},
                unique_columns=unique_columns,  # For duplicate detection (legacy)
                duplicate_check=DuplicateCheckConfig(
                    enabled=True,
                    check_file_level=True,
                    allow_duplicates=False,
                    uniqueness_columns=unique_columns  # This is what duplicate checking actually uses
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
            "mapping_errors": result.get("mapping_errors", []),
            "type_mismatch_summary": result.get("type_mismatch_summary", []),
            "llm_followup": result.get("llm_followup"),
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

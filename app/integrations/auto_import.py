"""
Auto-import execution logic for LLM-analyzed files.

This module handles the execution of import strategies recommended by the LLM agent.
"""

from typing import Dict, Any, List
from sqlalchemy import text, inspect
from app.domain.imports.mapper import detect_mapping_from_file
from app.db.models import create_table_if_not_exists, insert_records, calculate_file_hash
from app.db.session import get_engine
from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig
from app.db.metadata import store_table_metadata, enrich_table_metadata
from app.domain.imports.schema_mapper import analyze_schema_compatibility, transform_record
from app.domain.imports.history import start_import_tracking, complete_import_tracking
from app.utils.date import parse_flexible_date
import logging
import time

logger = logging.getLogger(__name__)


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
        
        # Build MappingConfig using LLM's column mapping
        # IMPORTANT: LLM provides {source_col: target_col} but mapper.py expects {target_col: source_col}
        # We need to INVERT the mapping for mapper.py to work correctly
        
        # Invert the column_mapping: {source: target} -> {target: source}
        inverted_mapping = {target_col: source_col for source_col, target_col in column_mapping.items()}
        
        logger.info(f"AUTO-IMPORT: LLM column_mapping (source->target): {column_mapping}")
        logger.info(f"AUTO-IMPORT: Inverted mapping (target->source): {inverted_mapping}")
        
        # Get target columns (keys in inverted_mapping, which were values in original column_mapping)
        target_columns = list(inverted_mapping.keys())
        
        # Build db_schema - infer types from data using conservative heuristics
        import re
        db_schema = {}
        for target_col in target_columns:
            # Find source column that maps to this target
            source_col = next((k for k, v in column_mapping.items() if v == target_col), None)
            if source_col and records:
                # Sample values from source column
                sample_values = [r.get(source_col) for r in records[:100] if r.get(source_col) is not None]
                subset = sample_values[:20]
                
                # Convert to strings for pattern matching
                sample_str = [str(v) for v in subset]
                
                # Infer type (conservative approach)
                # Check for phone number patterns first (must be TEXT, not NUMERIC)
                phone_patterns = [
                    r'^\d{3}\.\d{3}\.\d{4}$',  # 415.610.7325
                    r'^\d{3}-\d{3}-\d{4}$',    # 415-610-7325
                    r'^\(\d{3}\)\s*\d{3}-\d{4}$',  # (415) 610-7325
                    r'^\d{3}\s+\d{3}\s+\d{4}$',  # 415 610 7325
                    r'^\+?\d{1,3}[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',  # International formats
                ]
                is_phone = False
                for pattern in phone_patterns:
                    if any(re.match(pattern, s) for s in sample_str):
                        is_phone = True
                        break
                
                if is_phone:
                    db_schema[target_col] = "TEXT"
                elif any('%' in s for s in sample_str):
                    # Percentage values - use TEXT
                    db_schema[target_col] = "TEXT"
                elif any('@' in str(v) for v in sample_values[:10]):
                    # Likely email addresses - use TEXT for unlimited length
                    db_schema[target_col] = "TEXT"
                else:
                    # Detect date-like values using flexible parser
                    parsed_samples = [
                        parse_flexible_date(val)
                        for val in subset
                    ]
                    successful_parses = [ps for ps in parsed_samples if ps is not None]
                    
                    if successful_parses and len(successful_parses) >= max(1, len(subset) // 2):
                        db_schema[target_col] = "TIMESTAMP"
                    elif subset and all(isinstance(v, (int, float)) for v in subset if v is not None):
                        # Numeric data (only if not phone/percentage/email)
                        db_schema[target_col] = "DECIMAL"
                    else:
                        # Default to TEXT when no other signal is detected
                        db_schema[target_col] = "TEXT"
            else:
                db_schema[target_col] = "TEXT"  # Default
        
        logger.info(f"AUTO-IMPORT: Inferred schema: {db_schema}")
        
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
        
        if table_exists and strategy in ["MERGE_EXACT", "ADAPT_DATA"]:
            logger.info(f"AUTO-IMPORT: Table '{target_table}' exists, will merge into it")
            # For merging, we only need the mappings, not the schema
            # The existing table schema will be used
            mapping_config = MappingConfig(
                table_name=target_table,
                db_schema={},  # Empty - will use existing table schema
                mappings=inverted_mapping,  # Use inverted mapping (target->source)
                rules={},
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
                rules={},
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
            "mapping_errors": result.get("mapping_errors", [])
        }
        
    except Exception as e:
        logger.error(f"Error executing LLM import decision: {str(e)}", exc_info=True)
        
        return {
            "success": False,
            "error": str(e),
            "strategy_attempted": llm_decision.get("strategy"),
            "target_table": llm_decision.get("target_table")
        }

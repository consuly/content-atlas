"""
Auto-import execution logic for LLM-analyzed files.

This module handles the execution of import strategies recommended by the LLM agent.
"""

from typing import Dict, Any, List
from sqlalchemy import text, inspect
from .mapper import detect_mapping_from_file
from .models import create_table_if_not_exists, insert_records, calculate_file_hash
from .database import get_engine
from .schemas import MappingConfig
from .table_metadata import store_table_metadata, enrich_table_metadata
from .schema_mapper import analyze_schema_compatibility, transform_record
from .import_history import start_import_tracking, complete_import_tracking
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
        llm_decision: LLM's decision with strategy and target_table
        
    Returns:
        Execution result with success status and details
    """
    start_time = time.time()
    import_id = None
    
    try:
        strategy = llm_decision["strategy"]
        target_table = llm_decision["target_table"]
        
        logger.info(f"Executing LLM decision: {strategy} into table '{target_table}'")
        
        # Detect file type
        if file_name.endswith('.csv'):
            file_type = 'csv'
        elif file_name.endswith(('.xlsx', '.xls')):
            file_type = 'excel'
        elif file_name.endswith('.json'):
            file_type = 'json'
        elif file_name.endswith('.xml'):
            file_type = 'xml'
        else:
            file_type = 'unknown'
        
        # Calculate file hash
        file_hash = calculate_file_hash(file_content)
        file_size = len(file_content)
        
        # Use existing detection logic to generate MappingConfig
        _, detected_mapping, columns_found, rows_sampled = detect_mapping_from_file(
            file_content, file_name
        )
        
        # Override table name with LLM's decision
        detected_mapping.table_name = target_table
        
        # Start import tracking
        import_id = start_import_tracking(
            source_type="local_upload",
            file_name=file_name,
            table_name=target_table,
            file_size_bytes=file_size,
            file_type=file_type,
            file_hash=file_hash,
            mapping_config=detected_mapping,
            import_strategy=strategy
        )
        
        # Map data using the detected mapping
        from .mapper import map_data
        mapped_records, errors = map_data(all_records, detected_mapping)
        
        if errors:
            logger.warning(f"Mapping errors encountered: {errors}")
        
        # Handle schema transformation for merge strategies
        engine = get_engine()
        inspector = inspect(engine)
        table_exists = inspector.has_table(target_table)
        
        if strategy in ["MERGE_EXACT", "EXTEND_TABLE", "ADAPT_DATA"] and table_exists:
            # Get existing table schema
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table_name
                    AND column_name != 'id'
                    ORDER BY ordinal_position
                """), {"table_name": target_table})
                
                existing_columns = [row[0] for row in result]
            
            # Get source columns from mapped records
            source_columns = list(mapped_records[0].keys()) if mapped_records else []
            
            # Analyze schema compatibility and get column mapping
            logger.info(f"Analyzing schema compatibility between source ({len(source_columns)} cols) and target ({len(existing_columns)} cols)")
            compatibility = analyze_schema_compatibility(source_columns, existing_columns)
            
            logger.info(f"Schema compatibility: {compatibility['match_percentage']:.1f}% matched, "
                       f"{compatibility['new_count']} new columns")
            
            # Transform records to match target schema
            column_mapping = compatibility['column_mapping']
            target_schema = {col: 'TEXT' for col in existing_columns}  # Simplified schema
            
            transformed_records = []
            for record in mapped_records:
                transformed = transform_record(record, column_mapping, target_schema)
                transformed_records.append(transformed)
            
            mapped_records = transformed_records
            
            # If there are new columns, we need to extend the table
            new_columns = compatibility['new_columns']
            if new_columns:
                logger.info(f"Adding {len(new_columns)} new columns to table '{target_table}': {new_columns}")
                with engine.begin() as conn:
                    for col_name in new_columns:
                        try:
                            # Add column as TEXT (can be refined later)
                            conn.execute(text(f'ALTER TABLE "{target_table}" ADD COLUMN IF NOT EXISTS "{col_name}" TEXT'))
                            logger.info(f"Added column '{col_name}' to table '{target_table}'")
                        except Exception as e:
                            logger.warning(f"Could not add column '{col_name}': {e}")
        
        # Create table if needed (for NEW_TABLE or if table doesn't exist)
        if strategy == "NEW_TABLE" or not table_exists:
            if not table_exists:
                logger.warning(f"Table '{target_table}' doesn't exist despite metadata. Creating it.")
            create_table_if_not_exists(engine, detected_mapping)
        
        # Insert records
        insert_start = time.time()
        records_processed = insert_records(
            engine,
            detected_mapping.table_name,
            mapped_records,
            config=detected_mapping,
            file_content=file_content,
            file_name=file_name
        )
        insert_time = time.time() - insert_start
        
        # Store or enrich table metadata
        if strategy == "NEW_TABLE":
            # Store metadata for new table
            store_table_metadata(
                table_name=target_table,
                purpose_short=llm_decision.get("purpose_short", "Data imported from file"),
                data_domain=llm_decision.get("data_domain"),
                key_entities=llm_decision.get("key_entities", [])
            )
            logger.info(f"Stored metadata for new table '{target_table}'")
        else:
            # Enrich existing table metadata
            enrich_table_metadata(
                table_name=target_table,
                additional_purpose=f"Merged data from {file_name}",
                new_entities=llm_decision.get("key_entities")
            )
            logger.info(f"Enriched metadata for existing table '{target_table}'")
        
        # Complete import tracking
        duration = time.time() - start_time
        complete_import_tracking(
            import_id=import_id,
            status="success",
            total_rows_in_file=len(all_records),
            rows_processed=records_processed,
            rows_inserted=records_processed,
            duration_seconds=duration,
            insert_time_seconds=insert_time
        )
        
        return {
            "success": True,
            "strategy_executed": strategy,
            "table_name": target_table,
            "records_processed": records_processed,
            "mapping_errors": errors if errors else []
        }
        
    except Exception as e:
        logger.error(f"Error executing LLM import decision: {str(e)}", exc_info=True)
        
        # Complete import tracking with failure
        if import_id:
            complete_import_tracking(
                import_id=import_id,
                status="failed",
                total_rows_in_file=len(all_records) if all_records else 0,
                rows_processed=0,
                rows_inserted=0,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )
        
        return {
            "success": False,
            "error": str(e),
            "strategy_attempted": llm_decision.get("strategy"),
            "target_table": llm_decision.get("target_table")
        }

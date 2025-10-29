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
    from .import_orchestrator import execute_data_import
    
    try:
        strategy = llm_decision["strategy"]
        target_table = llm_decision["target_table"]
        
        logger.info(f"Executing LLM decision: {strategy} into table '{target_table}'")
        
        # Use existing detection logic to generate MappingConfig
        _, detected_mapping, columns_found, rows_sampled = detect_mapping_from_file(
            file_content, file_name
        )
        
        # Override table name with LLM's decision
        detected_mapping.table_name = target_table
        
        # Prepare metadata info
        metadata_info = {
            "purpose_short": llm_decision.get("purpose_short", "Data imported from file"),
            "data_domain": llm_decision.get("data_domain"),
            "key_entities": llm_decision.get("key_entities", [])
        }
        
        # Execute unified import
        result = execute_data_import(
            file_content=file_content,
            file_name=file_name,
            mapping_config=detected_mapping,
            source_type="local_upload",
            import_strategy=strategy,
            metadata_info=metadata_info
        )
        
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

"""
Unified import orchestration layer.

This module provides a centralized function for all data imports,
ensuring consistent behavior across all API endpoints and reducing code duplication.
"""

from typing import Dict, Any, List, Optional
from sqlalchemy import text, inspect
import time
import logging

from .processors.csv_processor import process_csv, process_excel, process_large_excel
from .processors.json_processor import process_json
from .processors.xml_processor import process_xml
from .mapper import map_data
from .models import (
    create_table_if_not_exists, 
    insert_records, 
    calculate_file_hash,
    DuplicateDataException,
    FileAlreadyImportedException
)
from .database import get_engine
from .schemas import MappingConfig
from .import_history import start_import_tracking, complete_import_tracking
from .table_metadata import store_table_metadata, enrich_table_metadata
from .schema_mapper import analyze_schema_compatibility, transform_record

logger = logging.getLogger(__name__)


def detect_file_type(filename: str) -> str:
    """Detect file type from filename."""
    if filename.endswith('.csv'):
        return 'csv'
    elif filename.endswith(('.xlsx', '.xls')):
        return 'excel'
    elif filename.endswith('.json'):
        return 'json'
    elif filename.endswith('.xml'):
        return 'xml'
    else:
        raise ValueError(f"Unsupported file type: {filename}")


def process_file_content(file_content: bytes, file_type: str) -> List[Dict[str, Any]]:
    """
    Process file content based on file type.
    
    Args:
        file_content: Raw file content
        file_type: Type of file ('csv', 'excel', 'json', 'xml')
        
    Returns:
        List of records extracted from file
    """
    # Use chunked processing for large Excel files (>50MB)
    if file_type == 'excel' and len(file_content) > 50 * 1024 * 1024:  # 50MB
        return process_large_excel(file_content)
    elif file_type == 'csv':
        return process_csv(file_content)
    elif file_type == 'excel':
        return process_excel(file_content)
    elif file_type == 'json':
        return process_json(file_content)
    elif file_type == 'xml':
        return process_xml(file_content)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def handle_schema_transformation(
    mapped_records: List[Dict[str, Any]],
    target_table: str,
    strategy: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Handle schema transformation for merge strategies.
    
    Args:
        mapped_records: Records to transform
        target_table: Target table name
        strategy: Import strategy (MERGE_EXACT, EXTEND_TABLE, ADAPT_DATA, etc.)
        
    Returns:
        Transformed records
    """
    if not mapped_records:
        return mapped_records
    
    engine = get_engine()
    inspector = inspect(engine)
    table_exists = inspector.has_table(target_table)
    
    # Only transform for merge strategies on existing tables
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
        source_columns = list(mapped_records[0].keys())
        
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
        
        # If there are new columns, extend the table
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
        
        return transformed_records
    
    return mapped_records


def execute_data_import(
    file_content: bytes,
    file_name: str,
    mapping_config: MappingConfig,
    source_type: str,  # "local_upload" or "b2_storage"
    source_path: Optional[str] = None,
    import_strategy: Optional[str] = None,
    metadata_info: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Central function for all data imports.
    
    This function orchestrates the entire import process:
    1. File processing
    2. Data mapping
    3. Schema transformation (if needed)
    4. Table creation (if needed)
    5. Data insertion
    6. Import tracking
    7. Metadata management
    
    Args:
        file_content: Raw file content
        file_name: Name of the file
        mapping_config: Mapping configuration
        source_type: Source type ("local_upload" or "b2_storage")
        source_path: Optional source path (for B2 files)
        import_strategy: Optional import strategy (NEW_TABLE, MERGE_EXACT, etc.)
        metadata_info: Optional metadata for table (purpose, domain, entities)
        
    Returns:
        Dict with success status, records_processed, table_name, and optional errors
        
    Raises:
        FileAlreadyImportedException: If file has already been imported
        DuplicateDataException: If duplicate data is detected
        Exception: For other errors
    """
    start_time = time.time()
    import_id = None
    records = []
    
    try:
        # Detect file type
        file_type = detect_file_type(file_name)
        
        # Calculate file hash and size
        file_hash = calculate_file_hash(file_content)
        file_size = len(file_content)
        
        # Start import tracking
        import_id = start_import_tracking(
            source_type=source_type,
            file_name=file_name,
            table_name=mapping_config.table_name,
            file_size_bytes=file_size,
            file_type=file_type,
            file_hash=file_hash,
            source_path=source_path,
            mapping_config=mapping_config,
            import_strategy=import_strategy
        )
        
        logger.info(f"Starting import: {file_name} → {mapping_config.table_name} (strategy: {import_strategy})")
        
        # Process file
        parse_start = time.time()
        records = process_file_content(file_content, file_type)
        parse_time = time.time() - parse_start
        total_rows = len(records)
        
        logger.info(f"Parsed {total_rows} records in {parse_time:.2f}s")
        
        # Map data
        mapped_records, mapping_errors = map_data(records, mapping_config)
        
        if mapping_errors:
            logger.warning(f"Mapping errors encountered: {mapping_errors}")
        
        # Handle schema transformation for merge strategies
        mapped_records = handle_schema_transformation(
            mapped_records,
            mapping_config.table_name,
            import_strategy
        )
        
        # Create table if needed
        engine = get_engine()
        inspector = inspect(engine)
        table_exists = inspector.has_table(mapping_config.table_name)
        
        if not table_exists or import_strategy == "NEW_TABLE":
            create_table_if_not_exists(engine, mapping_config)
            logger.info(f"Created table: {mapping_config.table_name}")
        
        # Insert records
        insert_start = time.time()
        records_processed = insert_records(
            engine,
            mapping_config.table_name,
            mapped_records,
            config=mapping_config,
            file_content=file_content,
            file_name=file_name
        )
        insert_time = time.time() - insert_start
        
        logger.info(f"Inserted {records_processed} records in {insert_time:.2f}s")
        
        # Manage table metadata
        if metadata_info:
            if import_strategy == "NEW_TABLE" or not table_exists:
                # Store metadata for new table
                store_table_metadata(
                    table_name=mapping_config.table_name,
                    purpose_short=metadata_info.get("purpose_short", "Data imported from file"),
                    data_domain=metadata_info.get("data_domain"),
                    key_entities=metadata_info.get("key_entities", [])
                )
                logger.info(f"Stored metadata for table '{mapping_config.table_name}'")
            else:
                # Enrich existing table metadata
                enrich_table_metadata(
                    table_name=mapping_config.table_name,
                    additional_purpose=f"Merged data from {file_name}",
                    new_entities=metadata_info.get("key_entities")
                )
                logger.info(f"Enriched metadata for table '{mapping_config.table_name}'")
        
        # Complete import tracking
        duration = time.time() - start_time
        complete_import_tracking(
            import_id=import_id,
            status="success",
            total_rows_in_file=total_rows,
            rows_processed=records_processed,
            rows_inserted=records_processed,
            duration_seconds=duration,
            parsing_time_seconds=parse_time,
            insert_time_seconds=insert_time
        )
        
        logger.info(f"Import completed successfully in {duration:.2f}s")
        
        return {
            "success": True,
            "records_processed": records_processed,
            "table_name": mapping_config.table_name,
            "mapping_errors": mapping_errors if mapping_errors else [],
            "duration_seconds": duration
        }
        
    except FileAlreadyImportedException as e:
        if import_id:
            complete_import_tracking(
                import_id=import_id,
                status="failed",
                total_rows_in_file=0,
                rows_processed=0,
                rows_inserted=0,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )
        logger.error(f"File already imported: {str(e)}")
        raise
        
    except DuplicateDataException as e:
        if import_id:
            complete_import_tracking(
                import_id=import_id,
                status="failed",
                total_rows_in_file=len(records) if records else 0,
                rows_processed=0,
                rows_inserted=0,
                duplicates_found=e.duplicates_found,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )
        logger.error(f"Duplicate data detected: {str(e)}")
        raise
        
    except Exception as e:
        if import_id:
            complete_import_tracking(
                import_id=import_id,
                status="failed",
                total_rows_in_file=len(records) if records else 0,
                rows_processed=0,
                rows_inserted=0,
                duration_seconds=time.time() - start_time,
                error_message=str(e)
            )
        logger.error(f"Import failed: {str(e)}", exc_info=True)
        raise

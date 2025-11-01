"""
Unified import orchestration layer.

This module provides a centralized function for all data imports,
ensuring consistent behavior across all API endpoints and reducing code duplication.
"""

from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy import text, inspect
import time
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from .processors.csv_processor import process_csv, process_excel, process_large_excel
from .processors.json_processor import process_json
from .processors.xml_processor import process_xml
from .mapper import map_data
from app.db.models import (
    create_table_if_not_exists, 
    insert_records, 
    calculate_file_hash,
    DuplicateDataException,
    FileAlreadyImportedException
)
from app.db.session import get_engine
from app.api.schemas.shared import MappingConfig
from .history import (
    start_import_tracking, 
    complete_import_tracking,
    update_mapping_status,
    record_mapping_errors_batch
)
from app.db.metadata import store_table_metadata, enrich_table_metadata
from .schema_mapper import analyze_schema_compatibility, transform_record

logger = logging.getLogger(__name__)

# Chunk size for parallel processing - increased to 20K for better performance
# Reduces overhead of chunk management while maintaining parallelism benefits
CHUNK_SIZE = 20000


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
    # Use chunked processing for large Excel files (>10MB for better performance)
    if file_type == 'excel' and len(file_content) > 10 * 1024 * 1024:  # 10MB
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


def _map_chunk(
    chunk_records: List[Dict[str, Any]],
    config: MappingConfig,
    chunk_num: int
) -> Tuple[int, List[Dict[str, Any]], List[str]]:
    """
    Map a single chunk of records. Designed to be called in parallel.
    
    Args:
        chunk_records: Records in this chunk
        config: Mapping configuration
        chunk_num: Chunk number (for logging)
    
    Returns:
        Tuple of (chunk_num, mapped_records, errors)
    """
    chunk_start = time.time()
    logger.info(f"Mapping chunk {chunk_num} ({len(chunk_records)} records)")
    
    try:
        mapped_records, errors = map_data(chunk_records, config)
        chunk_time = time.time() - chunk_start
        records_per_sec = len(mapped_records) / chunk_time if chunk_time > 0 else 0
        logger.info(f"⏱️  Chunk {chunk_num}: Mapped {len(mapped_records)} records in {chunk_time:.2f}s ({records_per_sec:.0f} rec/sec, {len(errors)} errors)")
        return (chunk_num, mapped_records, errors)
    except Exception as e:
        logger.error(f"Error mapping chunk {chunk_num}: {e}")
        raise


def _map_chunks_parallel(
    raw_chunks: List[List[Dict[str, Any]]],
    config: MappingConfig,
    max_workers: int = 4
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Map multiple chunks in parallel and aggregate results.
    
    Args:
        raw_chunks: List of record chunks to map
        config: Mapping configuration
        max_workers: Maximum number of parallel workers
    
    Returns:
        Tuple of (all_mapped_records, all_errors)
    """
    logger.info(f"Starting parallel mapping for {len(raw_chunks)} chunks with {max_workers} workers")
    
    all_mapped_records = []
    all_errors = []
    chunk_results = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunk mapping tasks
        future_to_chunk = {
            executor.submit(_map_chunk, chunk_records, config, chunk_num + 1): chunk_num
            for chunk_num, chunk_records in enumerate(raw_chunks)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_chunk):
            chunk_num = future_to_chunk[future]
            try:
                result_chunk_num, mapped_records, errors = future.result()
                chunk_results[result_chunk_num] = (mapped_records, errors)
                logger.info(f"Chunk {result_chunk_num} mapping completed")
            except Exception as e:
                logger.error(f"Error in chunk {chunk_num + 1} mapping: {e}")
                raise
    
    # Aggregate results in order
    for chunk_num in sorted(chunk_results.keys()):
        mapped_records, errors = chunk_results[chunk_num]
        all_mapped_records.extend(mapped_records)
        all_errors.extend(errors)
    
    logger.info(f"Parallel mapping completed: {len(all_mapped_records)} total records, {len(all_errors)} total errors")
    return all_mapped_records, all_errors


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
        Transformed records with proper column mapping
    """
    if not mapped_records:
        return mapped_records
    
    engine = get_engine()
    inspector = inspect(engine)
    table_exists = inspector.has_table(target_table)
    
    # Only transform for merge strategies on existing tables
    if strategy in ["MERGE_EXACT", "EXTEND_TABLE", "ADAPT_DATA"] and table_exists:
        logger.info(f"Applying schema transformation for strategy '{strategy}' on table '{target_table}'")
        
        # Get existing table schema (excluding metadata columns)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name != 'id'
                AND column_name NOT LIKE '\\_%'
                ORDER BY ordinal_position
            """), {"table_name": target_table})
            
            existing_columns = [row[0] for row in result]
        
        logger.info(f"Existing table columns ({len(existing_columns)}): {existing_columns[:5]}{'...' if len(existing_columns) > 5 else ''}")
        
        # Get source columns from mapped records
        source_columns = list(mapped_records[0].keys())
        logger.info(f"Source columns ({len(source_columns)}): {source_columns[:5]}{'...' if len(source_columns) > 5 else ''}")
        
        # Analyze schema compatibility and get column mapping
        logger.info(f"Analyzing schema compatibility between source and target...")
        compatibility = analyze_schema_compatibility(source_columns, existing_columns)
        
        logger.info(f"Schema compatibility analysis:")
        logger.info(f"  - Match percentage: {compatibility['match_percentage']:.1f}%")
        logger.info(f"  - Matched columns: {compatibility['matched_count']}")
        logger.info(f"  - New columns: {compatibility['new_count']}")
        logger.info(f"  - Compatibility score: {compatibility['compatibility_score']:.2f}")
        
        # Log column mapping details
        column_mapping = compatibility['column_mapping']
        mapped_count = sum(1 for v in column_mapping.values() if v is not None)
        logger.info(f"Column mapping: {mapped_count}/{len(source_columns)} source columns mapped")
        
        # Log a few example mappings
        example_mappings = list(column_mapping.items())[:5]
        for src, tgt in example_mappings:
            if tgt:
                logger.info(f"  '{src}' -> '{tgt}'")
            else:
                logger.info(f"  '{src}' -> (new column)")
        
        # Get target schema with data types
        target_schema = {col: 'TEXT' for col in existing_columns}  # Simplified schema
        
        # Transform records to match target schema
        logger.info(f"Transforming {len(mapped_records)} records...")
        transformed_records = []
        for i, record in enumerate(mapped_records):
            transformed = transform_record(record, column_mapping, target_schema)
            transformed_records.append(transformed)
            
            # Log first transformed record for debugging
            if i == 0:
                logger.info(f"First transformed record has {len(transformed)} columns: {list(transformed.keys())[:5]}{'...' if len(transformed) > 5 else ''}")
        
        # If there are new columns, extend the table
        new_columns = compatibility['new_columns']
        if new_columns:
            logger.info(f"Extending table with {len(new_columns)} new columns: {new_columns}")
            with engine.begin() as conn:
                for col_name in new_columns:
                    try:
                        # Add column as TEXT (can be refined later)
                        conn.execute(text(f'ALTER TABLE "{target_table}" ADD COLUMN IF NOT EXISTS "{col_name}" TEXT'))
                        logger.info(f"  Added column '{col_name}' to table '{target_table}'")
                    except Exception as e:
                        logger.warning(f"  Could not add column '{col_name}': {e}")
        
        logger.info(f"Schema transformation complete: {len(transformed_records)} records ready for insertion")
        return transformed_records
    
    logger.info(f"No schema transformation needed (strategy: {strategy}, table_exists: {table_exists})")
    return mapped_records


def execute_data_import(
    file_content: bytes,
    file_name: str,
    mapping_config: MappingConfig,
    source_type: str,  # "local_upload" or "b2_storage"
    source_path: Optional[str] = None,
    import_strategy: Optional[str] = None,
    metadata_info: Optional[Dict[str, Any]] = None,
    pre_parsed_records: Optional[List[Dict[str, Any]]] = None,
    pre_mapped: bool = False
) -> Dict[str, Any]:
    """
    Central function for all data imports.
    
    This function orchestrates the entire import process:
    1. File processing (skipped if pre_parsed_records provided)
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
        pre_parsed_records: Optional pre-parsed records from cache (avoids re-parsing)
        
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
        
        # Process file (or use cached records)
        parse_start = time.time()
        if pre_parsed_records is not None:
            # Use cached records - skip file parsing
            records = pre_parsed_records
            parse_time = 0.0  # No parsing time since we used cache
            logger.info(f"Using {len(records)} cached records (skipped file parsing)")
        else:
            # Parse file normally
            records = process_file_content(file_content, file_type)
            parse_time = time.time() - parse_start
            logger.info(f"Parsed {len(records)} records in {parse_time:.2f}s")
        
        total_rows = len(records)
        
        # Start mapping status tracking
        update_mapping_status(import_id, 'in_progress')
        
        # Map data - skip if already pre-mapped
        if pre_mapped:
            # Records are already mapped, skip mapping phase
            mapped_records = records
            mapping_errors = []
            map_time = 0.0
            logger.info(f"Using {len(mapped_records)} pre-mapped records (skipped mapping)")
        else:
            # Map data - use parallel mapping for large datasets
            map_start = time.time()
            if total_rows > CHUNK_SIZE:
                # Split into chunks for parallel mapping
                chunks = []
                for chunk_start in range(0, total_rows, CHUNK_SIZE):
                    chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
                    chunk_records = records[chunk_start:chunk_end]
                    chunks.append(chunk_records)
                
                total_chunks = len(chunks)
                logger.info(f"Split {total_rows} records into {total_chunks} chunks for parallel mapping")
                
                # Determine number of workers
                max_workers = min(4, os.cpu_count() or 2)
                logger.info(f"Using {max_workers} parallel workers for mapping")
                
                # Map chunks in parallel
                mapped_records, mapping_errors = _map_chunks_parallel(chunks, mapping_config, max_workers)
            else:
                # Use sequential mapping for small datasets
                logger.info(f"Using sequential mapping for {total_rows} records")
                mapped_records, mapping_errors = map_data(records, mapping_config)
            
            map_time = time.time() - map_start
            records_per_sec = len(mapped_records) / map_time if map_time > 0 else 0
            logger.info(f"⏱️  TIMING: Mapping completed in {map_time:.2f}s ({len(mapped_records)} records, {records_per_sec:.0f} rec/sec)")
        
        # Cache mapped records for potential re-use (e.g., if user retries with same config)
        # This is done in main.py's records_cache, but we log it here for visibility
        logger.info(f"Mapped records ready for caching (if file hash available)")
        
        # Track mapping completion and errors
        if mapping_errors:
            logger.warning(f"Mapping errors encountered: {len(mapping_errors)} errors")
            
            # Convert error strings to structured format for storage
            error_records = []
            for i, error_msg in enumerate(mapping_errors):
                error_records.append({
                    'record_number': i + 1,
                    'error_type': 'mapping_error',
                    'error_message': error_msg,
                    'source_field': None,
                    'target_field': None,
                    'source_value': None,
                    'chunk_number': None
                })
            
            # Batch insert errors
            record_mapping_errors_batch(import_id, error_records)
            
            # Update mapping status based on whether we have any mapped records
            if len(mapped_records) > 0:
                mapping_status = 'completed_with_errors'
            else:
                mapping_status = 'failed'
            
            update_mapping_status(
                import_id,
                mapping_status,
                errors_count=len(mapping_errors),
                duration_seconds=map_time
            )
        else:
            # Success case - no errors
            update_mapping_status(
                import_id,
                'completed',
                errors_count=0,
                duration_seconds=map_time
            )
        
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
        records_inserted, duplicates_skipped = insert_records(
            engine,
            mapping_config.table_name,
            mapped_records,
            config=mapping_config,
            file_content=file_content,
            file_name=file_name
        )
        insert_time = time.time() - insert_start
        
        logger.info(f"Inserted {records_inserted} records in {insert_time:.2f}s (skipped {duplicates_skipped} duplicates)")
        
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
            rows_processed=len(mapped_records),
            rows_inserted=records_inserted,
            rows_skipped=duplicates_skipped,
            duplicates_found=duplicates_skipped,
            duration_seconds=duration,
            parsing_time_seconds=parse_time,
            insert_time_seconds=insert_time
        )
        
        logger.info(f"Import completed successfully in {duration:.2f}s")
        
        return {
            "success": True,
            "records_processed": records_inserted,
            "duplicates_skipped": duplicates_skipped,
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

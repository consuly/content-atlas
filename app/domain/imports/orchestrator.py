"""
Unified import orchestration layer.

This module provides a centralized function for all data imports,
ensuring consistent behavior across all API endpoints and reducing code duplication.
"""

from typing import Dict, Any, List, Optional, Tuple
from collections.abc import Mapping, Sequence
from sqlalchemy import text, inspect
import time
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, wait, FIRST_COMPLETED

from .processors.csv_processor import (
    process_csv,
    process_excel,
    process_large_excel,
    stream_csv_records,
    detect_csv_header,
)
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
    record_mapping_errors_batch,
    list_duplicate_rows,
    record_duplicate_rows
)
from app.db.metadata import store_table_metadata, enrich_table_metadata
from .schema_mapper import analyze_schema_compatibility, transform_record
from app.core.config import settings

logger = logging.getLogger(__name__)

# Chunk size for parallel processing - increased to 20K for better performance
# Reduces overhead of chunk management while maintaining parallelism benefits
CHUNK_SIZE = 20000
MAP_STAGE_TIMEOUT_SECONDS = settings.map_stage_timeout_seconds
MAP_PARALLEL_MAX_WORKERS = max(1, settings.map_parallel_max_workers)
DUPLICATE_PREVIEW_LIMIT = 20
STREAMING_CSV_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10MB threshold to stream huge CSVs


def _records_look_like_mappings(records: Any, sample_size: int = 5) -> bool:
    """
    Ensure we only attempt to map structures that behave like dictionaries.

    Accepts any Mapping implementation or objects exposing .get(), ignoring
    None placeholders. This prevents cryptic AttributeErrors when cached
    rows were stored as raw strings/lists instead of parsed records.
    """
    if records is None:
        return False
    if isinstance(records, (str, bytes)):
        return False
    if isinstance(records, Sequence):
        if not records:
            return True
        checked = 0
        for record in records:
            if record is None:
                continue
            if isinstance(record, Mapping):
                checked += 1
            elif hasattr(record, "get") and callable(getattr(record, "get")):
                checked += 1
            else:
                return False
            if checked >= sample_size:
                break
        return True
    return False


def _summarize_type_mismatches(mapping_errors: List[Any]) -> List[Dict[str, Any]]:
    """Aggregate type mismatch errors for easier remediation suggestions."""
    summary: Dict[str, Dict[str, Any]] = {}
    for error in mapping_errors:
        if not isinstance(error, dict):
            continue
        if error.get("type") != "type_mismatch":
            continue
        column = error.get("column")
        if not column:
            continue
        entry = summary.setdefault(column, {
            "column": column,
            "expected_type": error.get("expected_type"),
            "samples": [],
            "occurrences": 0
        })
        entry["occurrences"] += 1
        value = error.get("value")
        if value is not None:
            value_str = str(value)
            if value_str not in entry["samples"] and len(entry["samples"]) < 5:
                entry["samples"].append(value_str)
    return sorted(summary.values(), key=lambda item: item["column"])


def _merge_type_mismatch_summaries(
    aggregated: Dict[str, Dict[str, Any]],
    new_summary: List[Dict[str, Any]],
) -> None:
    """Accumulate type mismatch summaries across streaming chunks."""
    for item in new_summary:
        col = item.get("column")
        if not col:
            continue
        target = aggregated.setdefault(
            col,
            {
                "column": col,
                "expected_type": item.get("expected_type"),
                "occurrences": 0,
                "samples": [],
            },
        )
        target["occurrences"] += item.get("occurrences", 0)
        for sample in item.get("samples", []) or []:
            if sample not in target["samples"] and len(target["samples"]) < 5:
                target["samples"].append(sample)


def _build_type_mismatch_followup(table_name: str, summary: List[Dict[str, Any]]) -> str:
    """Craft a follow-up message explaining type mismatches and next steps."""
    if not summary:
        return ""

    lines = [
        "WARNING: Import blocked by column type mismatches.",
        "Columns causing issues:"
    ]
    for item in summary:
        expected = item.get("expected_type") or "unknown type"
        samples = item.get("samples") or []
        sample_str = ", ".join(samples[:3]) if samples else "n/a"
        lines.append(
            f"- {item['column']} (expected {expected}; sample values: {sample_str})"
        )
    lines.append(
        "Ask the LLM to propose a schema migration plan: "
        "create a new column with the correct type, migrate existing values, "
        "and retire the old column so future imports succeed."
    )
    lines.append(
        "Provide the column details above when requesting the migration."
    )

    return "\n".join(lines)


def _determine_uniqueness_columns(mapping_config: MappingConfig, sample_record: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    Resolve which columns define uniqueness for duplicate handling.
    Prefers duplicate_check.uniqueness_columns, then legacy unique_columns, then the sample record keys.
    """
    if mapping_config.duplicate_check and mapping_config.duplicate_check.uniqueness_columns:
        return mapping_config.duplicate_check.uniqueness_columns
    if mapping_config.unique_columns:
        return mapping_config.unique_columns
    if sample_record:
        return list(sample_record.keys())
    return []


def _build_duplicate_followup(
    table_name: str,
    duplicates: List[Dict[str, Any]],
    uniqueness_columns: List[str],
    import_id: Optional[str]
) -> str:
    """Craft a follow-up prompt for LLM/user to resolve duplicate rows."""
    if not duplicates:
        return ""

    uniq_desc = ", ".join(uniqueness_columns) if uniqueness_columns else "all columns"
    import_hint = import_id or "{import_id}"
    lines = [
        f"Duplicates detected in table '{table_name}' (key: {uniq_desc}).",
        "For each duplicate row decide whether to merge into the existing row, keep as-is, or skip.",
        f"Fetch full context (incoming vs existing row) via GET /import-history/{import_hint}/duplicates/{{id}} before applying updates.",
        "Suggested merge policy: prefer non-null values, avoid overwriting trusted values without confirmation, and keep higher-confidence numeric values like revenue."
    ]

    preview_count = min(len(duplicates), 3)
    lines.append(f"Preview ({preview_count}):")
    for dup in duplicates[:preview_count]:
        record = dup.get("record") or {}
        if uniqueness_columns:
            summary_bits = [f"{col}={record.get(col)}" for col in uniqueness_columns]
            summary = ", ".join(summary_bits)
        else:
            summary = ", ".join([f"{k}={v}" for k, v in record.items()])
        lines.append(f"- id {dup.get('id')}: {summary}")

    lines.append("Reply with merge decisions per duplicate id and the field updates to apply.")
    return "\n".join(lines)


def _normalize_uniqueness_value(value: Any) -> Any:
    """Lightweight normalization so fingerprints are stable for in-file dedupe."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip().lower()
    if isinstance(value, (int, float, bool)):
        return value
    return str(value)


def _dedupe_records_in_memory(
    records: List[Dict[str, Any]],
    mapping_config: MappingConfig,
    import_id: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Quickly drop duplicates inside the uploaded file before mapping.

    Respects duplicate_check flags: disabled, force_import, allow_duplicates, or dedupe_within_file=False
    will all skip this pass to preserve legacy behavior.
    """
    if not records:
        return records, 0

    dedupe_cfg = mapping_config.duplicate_check
    if (
        not mapping_config.check_duplicates
        or not dedupe_cfg.enabled
        or dedupe_cfg.force_import
        or dedupe_cfg.allow_duplicates
        or not dedupe_cfg.dedupe_within_file
    ):
        return records, 0

    uniqueness_columns = _determine_uniqueness_columns(mapping_config, records[0])
    if not uniqueness_columns:
        logger.info("In-file dedupe skipped: no uniqueness columns available")
        return records, 0

    seen = set()
    deduped_records: List[Dict[str, Any]] = []
    duplicate_entries: List[Dict[str, Any]] = []

    for idx, record in enumerate(records, start=1):
        fingerprint = tuple(_normalize_uniqueness_value(record.get(col)) for col in uniqueness_columns)
        if fingerprint in seen:
            duplicate_entries.append({"record_number": idx, "record": record.copy()})
            continue
        seen.add(fingerprint)
        deduped_records.append(record)

    skipped = len(duplicate_entries)
    if skipped:
        logger.info(
            "In-file dedupe removed %d duplicate rows before mapping (uniqueness: %s)",
            skipped,
            ", ".join(uniqueness_columns),
        )
        if import_id and duplicate_entries:
            try:
                record_duplicate_rows(import_id, duplicate_entries)
            except Exception as e:  # pragma: no cover - defensive logging
                logger.error("Failed to persist in-file duplicate rows: %s", str(e))

    return deduped_records, skipped


def _dedupe_records_streaming_chunk(
    records: List[Dict[str, Any]],
    mapping_config: MappingConfig,
    seen_fingerprints: set,
    import_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Deduplicate a streaming CSV chunk while tracking seen fingerprints across chunks."""
    dedupe_cfg = mapping_config.duplicate_check
    if (
        not mapping_config.check_duplicates
        or not dedupe_cfg.enabled
        or dedupe_cfg.force_import
        or dedupe_cfg.allow_duplicates
        or not dedupe_cfg.dedupe_within_file
    ):
        return records, 0

    uniqueness_columns = _determine_uniqueness_columns(
        mapping_config,
        records[0] if records else None,
    )
    if not uniqueness_columns:
        return records, 0

    deduped_records: List[Dict[str, Any]] = []
    duplicate_entries: List[Dict[str, Any]] = []

    for idx, record in enumerate(records, start=1):
        fingerprint = tuple(_normalize_uniqueness_value(record.get(col)) for col in uniqueness_columns)
        if fingerprint in seen_fingerprints:
            duplicate_entries.append({"record_number": idx, "record": record.copy()})
            continue
        seen_fingerprints.add(fingerprint)
        deduped_records.append(record)

    skipped = len(duplicate_entries)
    if skipped and import_id:
        try:
            record_duplicate_rows(import_id, duplicate_entries)
        except Exception as e:  # pragma: no cover - defensive logging
            logger.error("Failed to persist in-file duplicate rows: %s", str(e))

    return deduped_records, skipped


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


def _execute_streaming_csv_import(
    *,
    file_content: bytes,
    file_name: str,
    mapping_config: MappingConfig,
    source_type: str,
    source_path: Optional[str],
    import_strategy: Optional[str],
    metadata_info: Optional[Dict[str, Any]],
    import_id: str,
) -> Dict[str, Any]:
    """Stream CSV chunks to avoid loading huge files into memory."""
    parse_time_total = 0.0
    map_time_total = 0.0
    insert_time_total = 0.0
    raw_total_rows = 0
    mapped_total_rows = 0
    records_inserted_total = 0
    duplicates_skipped_total = 0
    intra_file_duplicates_skipped = 0
    mapping_errors_count = 0
    type_mismatch_agg: Dict[str, Dict[str, Any]] = {}
    duplicate_rows: List[Dict[str, Any]] = []
    uniqueness_columns: List[str] = []

    update_mapping_status(import_id, "in_progress")
    engine = get_engine()
    type_mismatch_summary: List[Dict[str, Any]] = []

    # Maintain cross-chunk dedupe fingerprints when requested
    seen_fingerprints: set = set()
    has_header = detect_csv_header(file_content)
    chunk_iter = stream_csv_records(
        file_content,
        has_header=has_header,
        chunk_size=CHUNK_SIZE,
    )

    first_chunk = True
    for chunk_num, chunk_records in enumerate(chunk_iter, start=1):
        chunk_start = time.time()
        raw_total_rows += len(chunk_records)

        # Optional in-file dedupe across the entire stream
        chunk_records, intra_chunk_skipped = _dedupe_records_streaming_chunk(
            chunk_records,
            mapping_config,
            seen_fingerprints,
            import_id=import_id,
        )
        intra_file_duplicates_skipped += intra_chunk_skipped
        parse_time_total += time.time() - chunk_start

        if not chunk_records:
            continue

        map_start = time.time()
        mapped_records, mapping_errors = map_data(chunk_records, mapping_config)
        map_time_total += time.time() - map_start
        mapping_errors_count += len(mapping_errors)
        type_summary = _summarize_type_mismatches(mapping_errors)
        _merge_type_mismatch_summaries(type_mismatch_agg, type_summary)

        mapped_records = handle_schema_transformation(
            mapped_records,
            mapping_config.table_name,
            import_strategy,
        )

        insert_start = time.time()
        chunk_file_content = file_content if first_chunk else None
        inserted, chunk_duplicates = insert_records(
            engine,
            mapping_config.table_name,
            mapped_records,
            config=mapping_config,
            file_content=chunk_file_content,
            file_name=file_name,
        )
        insert_time_total += time.time() - insert_start
        first_chunk = False

        records_inserted_total += inserted
        duplicates_skipped_total += chunk_duplicates
        mapped_total_rows += len(mapped_records)

        if not uniqueness_columns and mapped_records:
            uniqueness_columns = _determine_uniqueness_columns(
                mapping_config,
                mapped_records[0],
            )

    type_mismatch_summary = sorted(type_mismatch_agg.values(), key=lambda item: item["column"])

    # Update mapping status
    mapping_status = "completed_with_errors" if mapping_errors_count else "completed"
    update_mapping_status(
        import_id,
        mapping_status,
        errors_count=mapping_errors_count,
        duration_seconds=map_time_total,
    )

    # Manage table metadata
    if metadata_info:
        inspector = inspect(engine)
        table_exists = inspector.has_table(mapping_config.table_name)
        if import_strategy == "NEW_TABLE" or not table_exists:
            store_table_metadata(
                table_name=mapping_config.table_name,
                purpose_short=metadata_info.get("purpose_short", "Data imported from file"),
                data_domain=metadata_info.get("data_domain"),
                key_entities=metadata_info.get("key_entities", []),
            )
        else:
            enrich_table_metadata(
                table_name=mapping_config.table_name,
                additional_purpose=f"Merged data from {file_name}",
                new_entities=metadata_info.get("key_entities"),
            )

    duration = parse_time_total + map_time_total + insert_time_total

    if duplicates_skipped_total > 0:
        try:
            duplicate_rows = list_duplicate_rows(
                import_id,
                limit=DUPLICATE_PREVIEW_LIMIT,
                include_existing_row=True,
            )
        except Exception as e:
            logger.error("Failed to load duplicate rows for preview: %s", str(e))

    duplicate_followup = _build_duplicate_followup(
        mapping_config.table_name,
        duplicate_rows,
        uniqueness_columns,
        import_id,
    )
    duplicate_total = duplicates_skipped_total
    followup_message = duplicate_followup or ""
    needs_user_input = duplicates_skipped_total > 0

    metadata_payload: Dict[str, Any] = {}
    if type_mismatch_summary:
        metadata_payload["type_mismatch_summary"] = type_mismatch_summary
    if intra_file_duplicates_skipped:
        metadata_payload["intra_file_duplicates_skipped"] = intra_file_duplicates_skipped

    complete_import_tracking(
        import_id=import_id,
        status="success",
        total_rows_in_file=raw_total_rows,
        rows_processed=mapped_total_rows,
        rows_inserted=records_inserted_total,
        rows_skipped=duplicates_skipped_total,
        duplicates_found=duplicates_skipped_total,
        duration_seconds=duration,
        parsing_time_seconds=parse_time_total,
        insert_time_seconds=insert_time_total,
        metadata=metadata_payload or None,
    )

    return {
        "success": True,
        "records_processed": records_inserted_total,
        "duplicates_skipped": duplicates_skipped_total,
        "intra_file_duplicates_skipped": intra_file_duplicates_skipped,
        "table_name": mapping_config.table_name,
        "mapping_errors": [],
        "type_mismatch_summary": type_mismatch_summary,
        "duration_seconds": duration,
        "llm_followup": followup_message or None,
        "needs_user_input": needs_user_input,
        "duplicate_rows": duplicate_rows or None,
        "duplicate_rows_count": duplicate_total if duplicate_total else None,
        "import_id": import_id,
    }


def _map_chunk(
    chunk_records: List[Dict[str, Any]],
    config: MappingConfig,
    chunk_num: int
) -> Tuple[int, List[Dict[str, Any]], List[Dict[str, Any]]]:
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
    max_workers: Optional[int] = None,
    timeout_seconds: Optional[int] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Map multiple chunks in parallel and aggregate results.
    
    Args:
        raw_chunks: List of record chunks to map
        config: Mapping configuration
        max_workers: Maximum number of parallel workers
        timeout_seconds: Optional timeout for the mapping stage in seconds
    
    Returns:
        Tuple of (all_mapped_records, all_errors)
    """
    if max_workers is None:
        max_workers = MAP_PARALLEL_MAX_WORKERS
    logger.info(f"Starting parallel mapping for {len(raw_chunks)} chunks with {max_workers} workers")
    if timeout_seconds and timeout_seconds > 0:
        logger.info("Enforcing mapping timeout of %d seconds for parallel mapping", timeout_seconds)
    
    all_mapped_records: List[Dict[str, Any]] = []
    all_errors: List[Dict[str, Any]] = []
    chunk_results: Dict[int, Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]] = {}

    deadline = time.time() + timeout_seconds if timeout_seconds and timeout_seconds > 0 else None
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunk mapping tasks
        future_to_chunk = {
            executor.submit(_map_chunk, chunk_records, config, chunk_num + 1): chunk_num
            for chunk_num, chunk_records in enumerate(raw_chunks)
        }
        pending_futures = set(future_to_chunk.keys())

        while pending_futures:
            if deadline is not None:
                wait_timeout = deadline - time.time()
                if wait_timeout <= 0:
                    pending_chunks = [
                        future_to_chunk[future] + 1
                        for future in pending_futures
                        if not future.done()
                    ]
                    logger.error(
                        "Parallel mapping timed out after %d seconds; pending chunks: %s",
                        timeout_seconds,
                        pending_chunks or "none",
                    )
                    for future in pending_futures:
                        future.cancel()
                    raise TimeoutError(
                        f"Mapping stage timed out after {timeout_seconds} seconds. "
                        "Check mapping rules or reduce dataset size."
                    )
            else:
                wait_timeout = None

            try:
                done, not_done = wait(
                    pending_futures,
                    timeout=wait_timeout,
                    return_when=FIRST_COMPLETED
                )
            except FuturesTimeoutError as exc:  # pragma: no cover - defensive
                pending_chunks = [
                    future_to_chunk[future] + 1
                    for future in pending_futures
                    if not future.done()
                ]
                logger.error(
                    "Parallel mapping timed out after %d seconds; pending chunks: %s",
                    timeout_seconds,
                    pending_chunks or "none",
                )
                for future in pending_futures:
                    future.cancel()
                raise TimeoutError(
                    f"Mapping stage timed out after {timeout_seconds} seconds. "
                    "Check mapping rules or reduce dataset size."
                ) from exc

            if not done:
                pending_chunks = [
                    future_to_chunk[future] + 1
                    for future in pending_futures
                    if not future.done()
                ]
                logger.error(
                    "Parallel mapping timed out after %d seconds; pending chunks: %s",
                    timeout_seconds,
                    pending_chunks or "none",
                )
                for future in pending_futures:
                    future.cancel()
                raise TimeoutError(
                    f"Mapping stage timed out after {timeout_seconds} seconds. "
                    "Check mapping rules or reduce dataset size."
                )

            for future in done:
                chunk_num = future_to_chunk[future]
                try:
                    result_chunk_num, mapped_records, errors = future.result()
                    chunk_results[result_chunk_num] = (mapped_records, errors)
                    logger.info(f"Chunk {result_chunk_num} mapping completed")
                except Exception as e:
                    logger.error(f"Error in chunk {chunk_num + 1} mapping: {e}")
                    raise

            pending_futures = not_done
    
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
        
        # Prefer exact/safe mapping when the incoming records already match the
        # destination schema (common for LLM-produced mappings). Only fall back
        # to fuzzy matching when the majority of columns don't line up.
        logger.info(f"Analyzing schema compatibility between source and target...")
        existing_lower_map = {col.lower(): col for col in existing_columns}
        matched_columns = [src for src in source_columns if src.lower() in existing_lower_map]
        exact_match_ratio = (len(matched_columns) / len(source_columns)) if source_columns else 1.0

        if exact_match_ratio >= 0.8:
            logger.info(
                "High exact-match ratio detected (%.1f%%). Using safe identity mapping and treating unknown columns as new.",
                exact_match_ratio * 100,
            )
            column_mapping = {
                src: existing_lower_map.get(src.lower(), src) for src in source_columns
            }
            new_columns = [src for src in source_columns if src.lower() not in existing_lower_map]
            match_percentage = exact_match_ratio * 100
            compatibility_score = exact_match_ratio * (1 - (len(new_columns) / (len(source_columns) + len(existing_columns) or 1)) * 0.5)
            compatibility = {
                "column_mapping": column_mapping,
                "matched_columns": matched_columns,
                "new_columns": new_columns,
                "match_percentage": match_percentage,
                "matched_count": len(matched_columns),
                "new_count": len(new_columns),
                "compatibility_score": compatibility_score,
            }
        else:
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
        for new_col in compatibility['new_columns']:
            target_schema[new_col] = 'TEXT'
        
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
    intra_file_duplicates_skipped = 0
    
    try:
        type_mismatch_summary: List[Dict[str, Any]] = []
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
        
        # Stream large CSVs to avoid materializing everything in memory
        streaming_csv = file_type == "csv" and len(file_content) >= STREAMING_CSV_THRESHOLD_BYTES

        # Process file (or use cached records)
        parse_start = time.time()
        used_cached_records = pre_parsed_records is not None
        if used_cached_records:
            # Use cached records - skip file parsing
            records = pre_parsed_records
            parse_time = 0.0  # No parsing time since we used cache
            logger.info(f"Using {len(records)} cached records (skipped file parsing)")
        elif streaming_csv:
            logger.info(
                "Streaming CSV import for %s (size=%d bytes)",
                file_name,
                len(file_content),
            )
            return _execute_streaming_csv_import(
                file_content=file_content,
                file_name=file_name,
                mapping_config=mapping_config,
                source_type=source_type,
                source_path=source_path,
                import_strategy=import_strategy,
                metadata_info=metadata_info,
                import_id=import_id,
            )
        else:
            # Parse file normally
            records = process_file_content(file_content, file_type)
            parse_time = time.time() - parse_start
            logger.info(f"Parsed {len(records)} records in {parse_time:.2f}s")

        if used_cached_records and not _records_look_like_mappings(records):
            logger.warning(
                "Cached records are not dict-like for file '%s'; reprocessing raw bytes",
                file_name
            )
            reparse_start = time.time()
            records = process_file_content(file_content, file_type)
            parse_time = time.time() - reparse_start
            logger.info(
                "Fallback parsing loaded %d records after cache validation failure",
                len(records)
            )
            used_cached_records = False

        if not _records_look_like_mappings(records):
            raise ValueError(
                "Parsed records are not structured as column dictionaries. "
                "Double-check the file format and header configuration."
            )
        
        raw_total_rows = len(records)

        # Optional quick in-file dedupe before mapping
        records, intra_file_duplicates_skipped = _dedupe_records_in_memory(
            records,
            mapping_config,
            import_id=import_id
        )
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
                
                # Determine number of workers (configurable via MAP_PARALLEL_MAX_WORKERS)
                max_workers = MAP_PARALLEL_MAX_WORKERS
                logger.info(f"Using {max_workers} parallel workers for mapping")

                # Map chunks in parallel
                timeout_seconds = MAP_STAGE_TIMEOUT_SECONDS if MAP_STAGE_TIMEOUT_SECONDS > 0 else None
                mapped_records, mapping_errors = _map_chunks_parallel(
                    chunks,
                    mapping_config,
                    max_workers,
                    timeout_seconds=timeout_seconds
                )
            else:
                # Use sequential mapping for small datasets
                logger.info(f"Using sequential mapping for {total_rows} records")
                timeout_seconds = MAP_STAGE_TIMEOUT_SECONDS if MAP_STAGE_TIMEOUT_SECONDS > 0 else None
                if timeout_seconds:
                    logger.info("Enforcing mapping timeout of %d seconds for sequential mapping", timeout_seconds)
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(map_data, records, mapping_config)
                    try:
                        if timeout_seconds is None:
                            mapped_records, mapping_errors = future.result()
                        else:
                            mapped_records, mapping_errors = future.result(timeout=timeout_seconds)
                    except FuturesTimeoutError as exc:
                        logger.error(
                            "Sequential mapping timed out after %d seconds",
                            timeout_seconds
                        )
                        future.cancel()
                        raise TimeoutError(
                            f"Mapping stage timed out after {timeout_seconds} seconds. "
                            "Check mapping rules or reduce dataset size."
                        ) from exc
            type_mismatch_summary = _summarize_type_mismatches(mapping_errors)
            
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
                if isinstance(error_msg, dict):
                    error_message = error_msg.get("message", str(error_msg))
                    error_type = error_msg.get("type", "mapping_error")
                    source_field = error_msg.get("column") or error_msg.get("source_field")
                    target_field = error_msg.get("target_field")
                    source_value = error_msg.get("value")
                else:
                    error_message = str(error_msg)
                    error_type = "mapping_error"
                    source_field = None
                    target_field = None
                    source_value = None

                error_records.append({
                    'record_number': i + 1,
                    'error_type': error_type,
                    'error_message': error_message,
                    'source_field': source_field,
                    'target_field': target_field,
                    'source_value': source_value,
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
        
        # Complete import tracking with structured metadata
        duration = time.time() - start_time
        metadata_payload: Dict[str, Any] = {}
        if type_mismatch_summary:
            metadata_payload["type_mismatch_summary"] = type_mismatch_summary
        if intra_file_duplicates_skipped:
            metadata_payload["intra_file_duplicates_skipped"] = intra_file_duplicates_skipped

        duplicate_rows: List[Dict[str, Any]] = []
        duplicate_total = duplicates_skipped
        if duplicates_skipped > 0:
            try:
                duplicate_rows = list_duplicate_rows(
                    import_id,
                    limit=DUPLICATE_PREVIEW_LIMIT,
                    include_existing_row=True
                )
            except Exception as e:
                logger.error("Failed to load duplicate rows for preview: %s", str(e))

        uniqueness_columns = _determine_uniqueness_columns(
            mapping_config,
            mapped_records[0] if mapped_records else None
        )

        duplicate_followup = _build_duplicate_followup(
            mapping_config.table_name,
            duplicate_rows,
            uniqueness_columns,
            import_id
        )
        followup_parts = [
            duplicate_followup,
            _build_type_mismatch_followup(mapping_config.table_name, type_mismatch_summary)
        ]
        followup_message = "\n\n".join([part for part in followup_parts if part]) if any(followup_parts) else ""
        needs_user_input = duplicates_skipped > 0

        complete_import_tracking(
            import_id=import_id,
            status="success",
            total_rows_in_file=raw_total_rows,
            rows_processed=len(mapped_records),
            rows_inserted=records_inserted,
            rows_skipped=duplicates_skipped,
            duplicates_found=duplicates_skipped,
            duration_seconds=duration,
            parsing_time_seconds=parse_time,
            insert_time_seconds=insert_time,
            metadata=metadata_payload or None
        )

        logger.info(f"Import completed successfully in {duration:.2f}s")
        
        return {
            "success": True,
            "records_processed": records_inserted,
            "duplicates_skipped": duplicates_skipped,
            "intra_file_duplicates_skipped": intra_file_duplicates_skipped,
            "table_name": mapping_config.table_name,
            "mapping_errors": mapping_errors if mapping_errors else [],
            "type_mismatch_summary": type_mismatch_summary,
            "duration_seconds": duration,
            "llm_followup": followup_message or None,
            "needs_user_input": needs_user_input,
            "duplicate_rows": duplicate_rows if duplicate_rows else None,
            "duplicate_rows_count": duplicate_total if duplicate_total else None,
            "import_id": import_id
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

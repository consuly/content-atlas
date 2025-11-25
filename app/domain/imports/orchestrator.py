"""
Unified import orchestration layer.

This module provides a centralized function for all data imports,
ensuring consistent behavior across all API endpoints and reducing code duplication.
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import csv
import io
import math
import re
from collections.abc import Mapping, Sequence
from sqlalchemy import text, inspect
import time
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, wait, FIRST_COMPLETED
import pandas as pd
from decimal import Decimal, InvalidOperation

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
from .preprocessor import apply_row_transformations
from app.db.models import (
    create_file_imports_table_if_not_exists,
    create_table_if_not_exists,
    insert_records,
    calculate_file_hash,
    DuplicateDataException,
    FileAlreadyImportedException,
    check_file_already_imported,
)
from app.db.session import get_engine
from app.domain.imports.jobs import (
    update_import_job,
    get_import_job,
    fail_active_job,
)
from app.api.schemas.shared import MappingConfig
from .history import (
    start_import_tracking, 
    complete_import_tracking,
    update_mapping_status,
    initialize_mapping_chunks,
    mark_chunk_in_progress,
    mark_chunk_completed,
    mark_chunk_failed,
    summarize_chunk_status,
    record_mapping_errors_batch,
    list_duplicate_rows,
    record_duplicate_rows
)
from app.db.metadata import store_table_metadata, enrich_table_metadata
from .schema_mapper import analyze_schema_compatibility, transform_record
from app.core.config import settings
from app.utils.locks import TableLockManager

logger = logging.getLogger(__name__)

# Chunk size for parallel processing - increased to 20K for better performance
# Reduces overhead of chunk management while maintaining parallelism benefits
CHUNK_SIZE = 20000
MAP_STAGE_TIMEOUT_SECONDS = settings.map_stage_timeout_seconds
MAP_PARALLEL_MAX_WORKERS = max(1, settings.map_parallel_max_workers)
DUPLICATE_PREVIEW_LIMIT = 20
STREAMING_CSV_THRESHOLD_BYTES = 1 * 1024 * 1024  # 1MB threshold to stream CSVs for better memory efficiency


@dataclass
class RowCountResult:
    total_rows: Optional[int]
    data_rows: Optional[int]
    header_rows: int = 0
    detected_header: Optional[bool] = None
    header_row_index: Optional[int] = None
    reason: Optional[str] = None


def _columns_cover_mapping(records: List[Dict[str, Any]], mapping_config: Optional[MappingConfig]) -> bool:
    """Check if parsed columns cover the expected source fields in the mapping."""
    if not records or not mapping_config or not mapping_config.mappings:
        return True
    available_columns = set(records[0].keys())
    required_sources = {
        source for source in mapping_config.mappings.values() if source
    }
    return required_sources.issubset(available_columns)


def _count_csv_rows(file_content: bytes) -> Optional[int]:
    """Return total CSV row count without assuming header semantics."""
    try:
        text_io = io.StringIO(file_content.decode("utf-8", errors="ignore"))
        return sum(1 for _ in csv.reader(text_io))
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Unable to count CSV rows: %s", exc)
        return None


def _is_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    text = str(value).strip()
    return text != ""


def _looks_numeric(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    return bool(re.fullmatch(r"[-+]?\d+(\.\d+)?", text))


def _coerce_int_like(value: Any) -> Optional[int]:
    """
    Best-effort conversion to int for range checks.
    Returns None when the value isn't a whole number.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value) or not float(value).is_integer():
            return None
        return int(value)
    if isinstance(value, Decimal):
        if value != value.to_integral():
            return None
        return int(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        token = token.replace(",", "")
        if token.startswith("$"):
            token = token[1:]
        if token.startswith("(") and token.endswith(")"):
            token = f"-{token[1:-1]}"
        try:
            as_decimal = Decimal(token)
        except InvalidOperation:
            return None
        if as_decimal != as_decimal.to_integral():
            return None
        return int(as_decimal)
    return None


def _widen_integer_columns_for_overflow(
    records: List[Dict[str, Any]],
    mapping_config: MappingConfig,
) -> List[Dict[str, Any]]:
    """
    Upgrade integer columns to BIGINT when sample data exceeds 32-bit range.
    Returns a list of adjustments applied for logging/telemetry.
    """
    if not records or not mapping_config or not mapping_config.db_schema:
        return []

    INT32_MAX = 2_147_483_647
    adjustments: List[Dict[str, Any]] = []
    for col_name, raw_type in list(mapping_config.db_schema.items()):
        if not raw_type:
            continue
        type_upper = raw_type.upper()
        if "BIGINT" in type_upper or "INT" not in type_upper:
            continue

        max_abs: Optional[int] = None
        for record in records:
            candidate = _coerce_int_like(record.get(col_name))
            if candidate is None:
                continue
            abs_val = abs(candidate)
            if max_abs is None or abs_val > max_abs:
                max_abs = abs_val

        if max_abs is not None and max_abs > INT32_MAX:
            mapping_config.db_schema[col_name] = "BIGINT"
            adjustments.append(
                {
                    "column": col_name,
                    "from": raw_type,
                    "to": "BIGINT",
                    "max_observed": max_abs,
                }
            )

    return adjustments


def _guess_excel_header_row(df: pd.DataFrame) -> int:
    """Heuristically pick the header row from the first few non-empty rows."""
    preview = df.head(5)
    best_idx = 0
    best_score = float("-inf")

    for idx in range(len(preview)):
        row = preview.iloc[idx].tolist()
        tokens = [str(val).strip() for val in row if _is_non_empty_value(val)]
        if not tokens:
            continue
        text_like = sum(1 for token in tokens if not _looks_numeric(token))
        numeric_like = sum(1 for token in tokens if _looks_numeric(token))
        score = text_like - numeric_like
        if score > best_score:
            best_score = score
            best_idx = idx
        # Early exit when the row is overwhelmingly text-like (typical header)
        if score >= len(tokens) * 0.6:
            return idx

    return best_idx


def _count_excel_rows(file_content: bytes) -> RowCountResult:
    """Count Excel rows and estimate the data start row."""
    try:
        df_raw = pd.read_excel(io.BytesIO(file_content), header=None, engine="openpyxl")
    except Exception:
        try:
            df_raw = pd.read_excel(io.BytesIO(file_content), header=None)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Unable to inspect Excel rows: %s", exc)
            return RowCountResult(total_rows=None, data_rows=None)

    df_trimmed = df_raw.dropna(how="all")
    total_rows = len(df_trimmed)
    if total_rows == 0:
        return RowCountResult(total_rows=0, data_rows=0, header_rows=0, detected_header=None, header_row_index=None)

    header_row_index = _guess_excel_header_row(df_trimmed)
    data_rows = max(total_rows - header_row_index - 1, 0)

    return RowCountResult(
        total_rows=total_rows,
        data_rows=data_rows,
        header_rows=header_row_index + 1,
        detected_header=True,
        header_row_index=header_row_index,
        reason="excel_row_scan",
    )


def _count_file_rows(file_content: bytes, file_type: str, header_present: Optional[bool] = None) -> RowCountResult:
    """Lightweight row counting that accounts for optional headers."""
    if file_type == "csv":
        total_rows = _count_csv_rows(file_content)
        if header_present is None:
            try:
                header_present = detect_csv_header(file_content)
            except Exception:
                header_present = True
        header_rows = 1 if header_present else 0
        data_rows = max((total_rows or 0) - header_rows, 0) if total_rows is not None else None
        return RowCountResult(
            total_rows=total_rows,
            data_rows=data_rows,
            header_rows=header_rows,
            detected_header=header_present,
            reason="csv_row_scan",
        )

    if file_type == "excel":
        return _count_excel_rows(file_content)

    return RowCountResult(total_rows=None, data_rows=None)


def _update_job_progress(
    job_id: Optional[str],
    *,
    stage: Optional[str] = None,
    progress: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """Best-effort job progress updates; never fails the import if the job is missing."""
    if not job_id:
        return
    try:
        update_import_job(job_id, stage=stage, progress=progress, metadata=metadata)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Unable to update job %s progress: %s", job_id, exc)


def _mark_mapping_failed(
    import_id: Optional[str],
    job_id: Optional[str],
    error_message: str,
    *,
    status: str = "failed",
    errors_count: int = 0,
) -> None:
    """
    Best-effort fail handler that updates mapping status and the active job/file.

    This ensures the frontend sees a terminal state instead of a stuck 'mapping' job.
    """
    if import_id:
        try:
            update_mapping_status(import_id, status, errors_count=errors_count)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Unable to update mapping status for %s: %s", import_id, exc)

    if not job_id:
        return

    try:
        job = get_import_job(job_id)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Unable to load job %s for failure handling: %s", job_id, exc)
        job = None

    if job and job.get("file_id"):
        try:
            fail_active_job(job["file_id"], job_id, error_message)
            return
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Unable to mark active job %s failed: %s", job_id, exc)

    # Fallback when file_id is missing or fail_active_job failed
    try:
        update_import_job(
            job_id,
            status=status,
            stage="mapping",
            error_message=error_message,
            completed=True,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Unable to update job %s status to failed: %s", job_id, exc)


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
        return [key for key in sample_record.keys() if not str(key).startswith("_")]
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


def process_file_content(file_content: bytes, file_type: str, has_header: Optional[bool] = None) -> List[Dict[str, Any]]:
    """
    Process file content based on file type.
    
    Args:
        file_content: Raw file content
        file_type: Type of file ('csv', 'excel', 'json', 'xml')
        has_header: Optional header hint for CSV files
        
    Returns:
        List of records extracted from file
    """
    # Use chunked processing for large Excel files (>10MB for better performance)
    if file_type == 'excel' and len(file_content) > 10 * 1024 * 1024:  # 10MB
        return process_large_excel(file_content)
    elif file_type == 'csv':
        return process_csv(file_content, has_header=has_header)
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
    job_id: Optional[str] = None,
    row_count_info: Optional[RowCountResult] = None,
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
    mapping_errors_sample: List[Dict[str, Any]] = []
    MAPPING_ERROR_SAMPLE_LIMIT = 50
    type_mismatch_agg: Dict[str, Dict[str, Any]] = {}
    duplicate_rows: List[Dict[str, Any]] = []
    uniqueness_columns: List[str] = []
    chunk_status_summary: Dict[str, int] = {}
    row_count_warning: Optional[str] = None

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
    try:
        for chunk_num, chunk_records in enumerate(chunk_iter, start=1):
            chunk_start = time.time()
            chunk_start_row = raw_total_rows + 1
            raw_total_rows += len(chunk_records)

            preprocess_errors: List[Dict[str, Any]] = []
            # Apply pandas-backed row transforms before dedupe/mapping (e.g., explode email columns)
            chunk_records, preprocess_errors = apply_row_transformations(
                chunk_records,
                mapping_config,
                row_offset=chunk_start_row - 1,
            )

            # Optional in-file dedupe across the entire stream (after preprocessing)
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

            if import_id:
                mark_chunk_in_progress(import_id, chunk_num)

            try:
                map_start = time.time()
                mapped_records, mapping_errors = map_data(
                    chunk_records,
                    mapping_config,
                    row_offset=chunk_start_row - 1,
                )
                combined_errors = preprocess_errors + mapping_errors
                map_time_total += time.time() - map_start
                mapping_errors_count += len(combined_errors)
                type_summary = _summarize_type_mismatches(mapping_errors)
                _merge_type_mismatch_summaries(type_mismatch_agg, type_summary)

                if combined_errors:
                    error_records: List[Dict[str, Any]] = []
                    for err in combined_errors:
                        if isinstance(err, dict):
                            record_number = err.get("record_number")
                            error_message = err.get("message", str(err))
                            error_type = err.get("type", "mapping_error")
                            source_field = err.get("column") or err.get("source_field")
                            target_field = err.get("target_field")
                            source_value = err.get("value")
                            chunk_number = err.get("chunk_number") or chunk_num
                            error_records.append(
                                {
                                    "record_number": record_number,
                                    "error_type": error_type,
                                    "error_message": error_message,
                                    "source_field": source_field,
                                    "target_field": target_field,
                                    "source_value": source_value,
                                    "chunk_number": chunk_number,
                                }
                            )
                            if len(mapping_errors_sample) < MAPPING_ERROR_SAMPLE_LIMIT:
                                sample_entry = dict(err)
                                sample_entry.setdefault("chunk_number", chunk_number)
                                mapping_errors_sample.append(sample_entry)
                        else:
                            fallback_error = {
                                "record_number": chunk_start_row + len(error_records),
                                "error_type": "mapping_error",
                                "error_message": str(err),
                                "source_field": None,
                                "target_field": None,
                                "source_value": None,
                                "chunk_number": chunk_num,
                            }
                            error_records.append(fallback_error)
                            if len(mapping_errors_sample) < MAPPING_ERROR_SAMPLE_LIMIT:
                                mapping_errors_sample.append(fallback_error)
                    try:
                        record_mapping_errors_batch(import_id, error_records)
                    except Exception as exc:
                        logger.warning("Unable to persist mapping errors for chunk %s: %s", chunk_num, exc)

                mapped_records = handle_schema_transformation(
                    mapped_records,
                    mapping_config.table_name,
                    import_strategy,
                )

                insert_start = time.time()
                chunk_file_content = file_content if first_chunk else None
                with TableLockManager.acquire(mapping_config.table_name):
                    inserted, chunk_duplicates = insert_records(
                        engine,
                        mapping_config.table_name,
                        mapped_records,
                        config=mapping_config,
                        file_content=chunk_file_content,
                        file_name=file_name,
                        pre_mapped=True,
                    )
                insert_time_total += time.time() - insert_start
                first_chunk = False
            except Exception as exc:
                if import_id:
                    mark_chunk_failed(import_id, chunk_num, str(exc))
                raise
            else:
                if import_id:
                    mark_chunk_completed(import_id, chunk_num, errors_count=len(combined_errors))

            records_inserted_total += inserted
            duplicates_skipped_total += chunk_duplicates
            mapped_total_rows += len(mapped_records)

            if not uniqueness_columns and mapped_records:
                uniqueness_columns = _determine_uniqueness_columns(
                    mapping_config,
                    mapped_records[0],
                )

            # Progress update (best-effort; estimate assumes at least one more chunk)
            estimated_denominator = max(raw_total_rows + CHUNK_SIZE, 1)
            progress_pct = min(99, int((raw_total_rows / estimated_denominator) * 100))
            _update_job_progress(
                job_id,
                stage="mapping",
                progress=progress_pct,
                metadata={
                    "chunks_completed": chunk_num,
                    "rows_processed": mapped_total_rows,
                    "rows_inserted": records_inserted_total,
                    "duplicates_skipped": duplicates_skipped_total,
                    "source": source_type,
                },
            )
    except Exception as exc:
        _mark_mapping_failed(
            import_id,
            job_id,
            str(exc),
            status="failed",
            errors_count=mapping_errors_count,
        )
        raise

    type_mismatch_summary = sorted(type_mismatch_agg.values(), key=lambda item: item["column"])
    try:
        chunk_status_summary = summarize_chunk_status(import_id)
    except Exception as exc:
        logger.debug("Unable to summarize chunk status for import %s: %s", import_id, exc)

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
    if chunk_status_summary:
        metadata_payload["mapping_chunk_status"] = chunk_status_summary

    _update_job_progress(
        job_id,
        stage="mapping",
        progress=100,
        metadata={
            "chunks_completed": chunk_num if 'chunk_num' in locals() else 0,
            "rows_processed": mapped_total_rows,
            "rows_inserted": records_inserted_total,
            "duplicates_skipped": duplicates_skipped_total,
            "source": source_type,
        },
    )

    expected_data_rows = row_count_info.data_rows if row_count_info else None
    if expected_data_rows is not None and expected_data_rows != raw_total_rows:
        handled_rows = records_inserted_total + duplicates_skipped_total + intra_file_duplicates_skipped
        row_count_warning = (
            f"Parsed {raw_total_rows} rows from streaming CSV, but the file scan suggests {expected_data_rows} data rows. "
            f"Inserted {records_inserted_total}; skipped {duplicates_skipped_total} duplicates "
            f"(intra-file {intra_file_duplicates_skipped}); total handled {handled_rows}."
        )
        logger.warning("Row count check warning (streaming CSV): %s", row_count_warning)

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
        metadata=(
            {**metadata_payload, "row_count_warning": row_count_warning}
            if metadata_payload or row_count_warning
            else None
        ),
    )

    return {
        "success": True,
        "records_processed": records_inserted_total,
        "duplicates_skipped": duplicates_skipped_total,
        "intra_file_duplicates_skipped": intra_file_duplicates_skipped,
        "table_name": mapping_config.table_name,
        "mapping_errors": mapping_errors_sample or None,
        "type_mismatch_summary": type_mismatch_summary,
        "duration_seconds": duration,
        "llm_followup": (
            f"{followup_message}\n\n{row_count_warning}"
            if followup_message and row_count_warning
            else row_count_warning
            if row_count_warning
            else followup_message or None
        ),
        "needs_user_input": needs_user_input or bool(row_count_warning),
        "duplicate_rows": duplicate_rows or None,
        "duplicate_rows_count": duplicate_total if duplicate_total else None,
        "import_id": import_id,
        "row_count_warning": row_count_warning,
    }


def _map_chunk(
    chunk_records: List[Dict[str, Any]],
    config: MappingConfig,
    chunk_num: int,
    import_id: Optional[str] = None,
    row_offset: int = 0,
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

    if import_id:
        mark_chunk_in_progress(import_id, chunk_num)
    
    try:
        mapped_records, errors = map_data(
            chunk_records,
            config,
            row_offset=row_offset,
        )
        for error in errors:
            if isinstance(error, dict):
                error.setdefault("chunk_number", chunk_num)
        chunk_time = time.time() - chunk_start
        records_per_sec = len(mapped_records) / chunk_time if chunk_time > 0 else 0
        logger.info(f"⏱️  Chunk {chunk_num}: Mapped {len(mapped_records)} records in {chunk_time:.2f}s ({records_per_sec:.0f} rec/sec, {len(errors)} errors)")
        if import_id:
            mark_chunk_completed(import_id, chunk_num, errors_count=len(errors))
        return (chunk_num, mapped_records, errors)
    except Exception as e:
        logger.error(f"Error mapping chunk {chunk_num}: {e}")
        if import_id:
            mark_chunk_failed(import_id, chunk_num, str(e))
        raise


def _map_chunks_parallel(
    raw_chunks: List[List[Dict[str, Any]]],
    config: MappingConfig,
    max_workers: Optional[int] = None,
    timeout_seconds: Optional[int] = None,
    job_id: Optional[str] = None,
    import_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Map multiple chunks in parallel and aggregate results.
    
    Args:
        raw_chunks: List of record chunks to map
        config: Mapping configuration
        max_workers: Maximum number of parallel workers
        timeout_seconds: Optional timeout for the mapping stage in seconds
        import_id: Optional import tracking id for chunk-level persistence
    
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
    total_chunks = len(raw_chunks)
    completed_chunks = 0
    records_mapped_running = 0
    errors_running = 0

    deadline = time.time() + timeout_seconds if timeout_seconds and timeout_seconds > 0 else None
    timeout_message = (
        f"Mapping stage timed out after {timeout_seconds} seconds. "
        "Check mapping rules or reduce dataset size."
    ) if timeout_seconds else None

    def _mark_failed_chunks(chunk_numbers: List[int], reason: str) -> None:
        if not import_id or not chunk_numbers:
            return
        for chunk_number in chunk_numbers:
            try:
                mark_chunk_failed(import_id, chunk_number, reason)
            except Exception:
                logger.warning("Failed to record failed status for chunk %s", chunk_number)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunk mapping tasks while preserving source row offsets for error traceability
        future_to_chunk = {}
        running_offset = 0
        for chunk_num, chunk_records in enumerate(raw_chunks):
            future = executor.submit(
                _map_chunk,
                chunk_records,
                config,
                chunk_num + 1,
                import_id,
                running_offset,
            )
            future_to_chunk[future] = chunk_num
            running_offset += len(chunk_records)
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
                    _mark_failed_chunks(
                        pending_chunks,
                        timeout_message or f"Mapping stage timed out after {timeout_seconds} seconds.",
                    )
                    logger.error(
                        "Parallel mapping timed out after %d seconds; pending chunks: %s",
                        timeout_seconds,
                        pending_chunks or "none",
                    )
                    for future in pending_futures:
                        future.cancel()
                    raise TimeoutError(
                        timeout_message
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
                _mark_failed_chunks(
                    pending_chunks,
                    timeout_message or f"Mapping stage timed out after {timeout_seconds} seconds.",
                )
                logger.error(
                    "Parallel mapping timed out after %d seconds; pending chunks: %s",
                    timeout_seconds,
                    pending_chunks or "none",
                )
                for future in pending_futures:
                    future.cancel()
                raise TimeoutError(
                    timeout_message
                ) from exc

            if not done:
                pending_chunks = [
                    future_to_chunk[future] + 1
                    for future in pending_futures
                    if not future.done()
                ]
                _mark_failed_chunks(
                    pending_chunks,
                    timeout_message or f"Mapping stage timed out after {timeout_seconds} seconds.",
                )
                logger.error(
                    "Parallel mapping timed out after %d seconds; pending chunks: %s",
                    timeout_seconds,
                    pending_chunks or "none",
                )
                for future in pending_futures:
                    future.cancel()
                raise TimeoutError(
                    timeout_message
                )

            for future in done:
                chunk_num = future_to_chunk[future]
                try:
                    result_chunk_num, mapped_records, errors = future.result()
                    chunk_results[result_chunk_num] = (mapped_records, errors)
                    records_mapped_running += len(mapped_records)
                    errors_running += len(errors)
                    logger.info(f"Chunk {result_chunk_num} mapping completed")
                    completed_chunks += 1
                    progress_pct = int((completed_chunks / total_chunks) * 100) if total_chunks else None
                    _update_job_progress(
                        job_id,
                        stage="mapping",
                        progress=progress_pct,
                        metadata={
                            "chunks_completed": completed_chunks,
                            "total_chunks": total_chunks,
                            "records_mapped_so_far": records_mapped_running,
                            "errors_so_far": errors_running,
                            "parallel_workers": max_workers,
                            "timeout_seconds": timeout_seconds,
                        },
                    )
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
    pre_mapped: bool = False,
    job_id: Optional[str] = None,
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
    chunk_status_summary: Dict[str, int] = {}
    widened_columns: List[Dict[str, Any]] = []
    
    try:
        type_mismatch_summary: List[Dict[str, Any]] = []
        # Detect file type
        file_type = detect_file_type(file_name)
        row_count_info: Optional[RowCountResult] = None
        row_count_warning: Optional[str] = None
        csv_has_header: Optional[bool] = None
        
        # Calculate file hash and size
        file_hash = calculate_file_hash(file_content)
        file_size = len(file_content)

        # Fail fast on exact file duplicates so mapping status doesn't drift from execution state
        dup_cfg = mapping_config.duplicate_check
        should_check_file = (
            mapping_config.check_duplicates
            and dup_cfg.enabled
            and dup_cfg.check_file_level
            and not dup_cfg.force_import
        )
        if should_check_file:
            engine = get_engine()
            create_file_imports_table_if_not_exists(engine)
            if check_file_already_imported(engine, file_hash, mapping_config.table_name):
                logger.warning(
                    "Preflight duplicate detected for file '%s' (hash=%s) targeting table '%s'",
                    file_name,
                    file_hash[:8],
                    mapping_config.table_name,
                )
                raise FileAlreadyImportedException(
                    file_hash,
                    mapping_config.table_name,
                    dup_cfg.error_message,
                )
        
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

        if file_type == "csv":
            try:
                csv_has_header = detect_csv_header(file_content)
            except Exception as exc:  # pragma: no cover - defensive
                logger.info("CSV header detection failed, defaulting to header present: %s", exc)
                csv_has_header = True
        if not streaming_csv:
            row_count_info = _count_file_rows(file_content, file_type, header_present=csv_has_header)
        else:
            # For streaming CSVs, still perform a lightweight row count to reconcile later
            if file_type == "csv":
                row_count_info = _count_file_rows(file_content, file_type, header_present=csv_has_header)
        expected_data_rows = row_count_info.data_rows if row_count_info else None

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
                job_id=job_id,
                row_count_info=row_count_info,
            )
        else:
            # Parse file normally
            records = process_file_content(file_content, file_type, has_header=csv_has_header)
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

        if (
            not streaming_csv
            and expected_data_rows is not None
            and raw_total_rows != expected_data_rows
        ):
            row_count_warning = (
                f"Parsed {raw_total_rows} rows, but the file scan suggests {expected_data_rows} data rows "
                f"(total rows: {row_count_info.total_rows if row_count_info else 'unknown'}, "
                f"header rows: {row_count_info.header_rows if row_count_info else 'unknown'})."
            )

            if file_type == "csv" and row_count_info.detected_header is not None:
                alt_has_header = not row_count_info.detected_header
                alt_records = process_csv(file_content, has_header=alt_has_header)
                alt_total_rows = len(alt_records)
                alt_expected_rows = None
                if row_count_info.total_rows is not None:
                    alt_expected_rows = max(row_count_info.total_rows - (1 if alt_has_header else 0), 0)

                if (
                    alt_expected_rows is not None
                    and alt_total_rows == alt_expected_rows
                    and _columns_cover_mapping(alt_records, mapping_config)
                ):
                    records = alt_records
                    raw_total_rows = alt_total_rows
                    row_count_warning = (
                        f"{row_count_warning} Reprocessed CSV assuming "
                        f"{'a header' if alt_has_header else 'no header'} to align row counts."
                    )
                elif alt_expected_rows is not None and alt_total_rows == alt_expected_rows:
                    row_count_warning = (
                        f"{row_count_warning} Switching header handling would align counts, "
                        "but the current mapping does not match the alternative columns. "
                        "Confirm the header position or re-run mapping detection."
                    )

            if file_type == "excel" and row_count_info.header_row_index is not None:
                header_row_idx = row_count_info.header_row_index
                adjusted_df = None
                try:
                    adjusted_df = pd.read_excel(
                        io.BytesIO(file_content),
                        engine="openpyxl",
                        header=header_row_idx,
                    )
                except Exception:
                    try:
                        adjusted_df = pd.read_excel(io.BytesIO(file_content), header=header_row_idx)
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.warning("Excel row-count reprocess failed: %s", exc)

                if adjusted_df is not None:
                    adjusted_records = adjusted_df.to_dict("records")
                    for record in adjusted_records:
                        for key, value in record.items():
                            if pd.isna(value):
                                record[key] = None
                    adjusted_row_count = len(adjusted_records)
                    expected_rows = row_count_info.data_rows
                    if (
                        expected_rows is not None
                        and adjusted_row_count == expected_rows
                        and _columns_cover_mapping(adjusted_records, mapping_config)
                    ):
                        records = adjusted_records
                        raw_total_rows = adjusted_row_count
                        row_count_warning = (
                            f"{row_count_warning} Reprocessed Excel starting at row {header_row_idx + 1} to align counts."
                        )
                    elif expected_rows is not None and adjusted_row_count == expected_rows:
                        row_count_warning = (
                            f"{row_count_warning} Counts align when starting at row {header_row_idx + 1}, "
                            "but the detected columns differ from the provided mapping. "
                            "Confirm the header row or ask the assistant to re-evaluate the sheet layout."
                        )
                    elif expected_rows is not None and adjusted_row_count != expected_rows:
                        row_count_warning = (
                            f"{row_count_warning} Tried reprocessing from row {header_row_idx + 1} but still saw {adjusted_row_count} rows."
                        )

        if row_count_warning:
            logger.warning("Row count check warning: %s", row_count_warning)

        preprocess_errors: List[Dict[str, Any]] = []
        if not pre_mapped:
            records, preprocess_errors = apply_row_transformations(
                records,
                mapping_config,
                row_offset=0,
            )

        # Optional quick in-file dedupe before mapping
        records, intra_file_duplicates_skipped = _dedupe_records_in_memory(
            records,
            mapping_config,
            import_id=import_id
        )
        total_rows = len(records)
        if expected_data_rows is not None and raw_total_rows != expected_data_rows and row_count_warning:
            handled_rows = total_rows + intra_file_duplicates_skipped
            row_count_warning = (
                f"{row_count_warning} After de-duplication, {total_rows} unique rows remain "
                f"(in-file duplicates skipped: {intra_file_duplicates_skipped}); total handled {handled_rows}."
            )

        # Start mapping status tracking
        update_mapping_status(import_id, 'in_progress')
        
        # Map data - skip if already pre-mapped
        if pre_mapped:
            initialize_mapping_chunks(import_id, 1)
            mark_chunk_completed(import_id, 1, errors_count=0)
            # Records are already mapped, skip mapping phase
            mapped_records = records
            mapping_errors = []
            map_time = 0.0
            logger.info(f"Using {len(mapped_records)} pre-mapped records (skipped mapping)")
            _update_job_progress(
                job_id,
                stage="mapping",
                progress=100,
                metadata={
                    "total_chunks": 1,
                    "chunks_completed": 1,
                    "rows_to_map": total_rows,
                    "source": source_type,
                    "pre_mapped": True,
                },
            )
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
                initialize_mapping_chunks(import_id, total_chunks)
                logger.info(f"Split {total_rows} records into {total_chunks} chunks for parallel mapping")
                _update_job_progress(
                    job_id,
                    stage="mapping",
                    progress=0,
                    metadata={
                        "total_chunks": total_chunks,
                        "rows_to_map": total_rows,
                        "parallel_workers": MAP_PARALLEL_MAX_WORKERS,
                        "source": source_type,
                    },
                    )
                
                # Determine number of workers (configurable via MAP_PARALLEL_MAX_WORKERS)
                max_workers = MAP_PARALLEL_MAX_WORKERS
                logger.info(f"Using {max_workers} parallel workers for mapping")

                # Map chunks in parallel
                timeout_seconds = MAP_STAGE_TIMEOUT_SECONDS if MAP_STAGE_TIMEOUT_SECONDS > 0 else None
                mapped_records, mapping_errors = _map_chunks_parallel(
                    chunks,
                    mapping_config,
                    max_workers,
                    timeout_seconds=timeout_seconds,
                    job_id=job_id,
                    import_id=import_id,
                )
            else:
                # Use sequential mapping for small datasets
                logger.info(f"Using sequential mapping for {total_rows} records")
                initialize_mapping_chunks(import_id, 1)
                mark_chunk_in_progress(import_id, 1)
                timeout_seconds = MAP_STAGE_TIMEOUT_SECONDS if MAP_STAGE_TIMEOUT_SECONDS > 0 else None
                if timeout_seconds:
                    logger.info("Enforcing mapping timeout of %d seconds for sequential mapping", timeout_seconds)
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(map_data, records, mapping_config, row_offset=0)
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
                        mark_chunk_failed(
                            import_id,
                            1,
                            f"Mapping stage timed out after {timeout_seconds} seconds."
                        )
                        raise TimeoutError(
                            f"Mapping stage timed out after {timeout_seconds} seconds. "
                            "Check mapping rules or reduce dataset size."
                        ) from exc
                    except Exception as exc:
                        mark_chunk_failed(import_id, 1, str(exc))
                        raise
                mark_chunk_completed(
                    import_id,
                    1,
                    errors_count=len(preprocess_errors) + len(mapping_errors),
                )
                _update_job_progress(
                    job_id,
                    stage="mapping",
                    progress=100,
                    metadata={
                        "total_chunks": 1,
                        "chunks_completed": 1,
                        "rows_to_map": total_rows,
                        "source": source_type,
                    },
                )
            type_mismatch_summary = _summarize_type_mismatches(mapping_errors)
            
            map_time = time.time() - map_start
            records_per_sec = len(mapped_records) / map_time if map_time > 0 else 0
            logger.info(f"⏱️  TIMING: Mapping completed in {map_time:.2f}s ({len(mapped_records)} records, {records_per_sec:.0f} rec/sec)")

        try:
            chunk_status_summary = summarize_chunk_status(import_id)
        except Exception as exc:
            logger.warning("Unable to summarize chunk status for import %s: %s", import_id, exc)
        
        # Cache mapped records for potential re-use (e.g., if user retries with same config)
        # This is done in main.py's records_cache, but we log it here for visibility
        logger.info(f"Mapped records ready for caching (if file hash available)")
        
        # Track mapping completion and errors
        combined_errors = preprocess_errors + mapping_errors
        if combined_errors:
            logger.warning(f"Mapping errors encountered: {len(combined_errors)} errors")
            
            # Convert error strings to structured format for storage
            error_records = []
            for i, error_msg in enumerate(combined_errors):
                if isinstance(error_msg, dict):
                    error_message = error_msg.get("message", str(error_msg))
                    error_type = error_msg.get("type", "mapping_error")
                    source_field = error_msg.get("column") or error_msg.get("source_field")
                    target_field = error_msg.get("target_field")
                    source_value = error_msg.get("value")
                    record_number = error_msg.get("record_number") or i + 1
                    chunk_number = error_msg.get("chunk_number") or 1
                else:
                    error_message = str(error_msg)
                    error_type = "mapping_error"
                    source_field = None
                    target_field = None
                    source_value = None
                    record_number = i + 1
                    chunk_number = 1

                error_records.append({
                    'record_number': record_number,
                    'error_type': error_type,
                    'error_message': error_message,
                    'source_field': source_field,
                    'target_field': target_field,
                    'source_value': source_value,
                    'chunk_number': chunk_number
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
                errors_count=len(combined_errors),
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

        widened_columns = _widen_integer_columns_for_overflow(mapped_records, mapping_config)
        if widened_columns:
            logger.info(
                "Upgraded integer columns to BIGINT due to observed range: %s",
                widened_columns
            )
        
        # Create table if needed
        engine = get_engine()
        inspector = inspect(engine)
        table_exists = inspector.has_table(mapping_config.table_name)
        
        # Acquire table lock for safe sequential insertion
        # This prevents race conditions when multiple files target the same table in parallel
        insert_start = time.time()
        with TableLockManager.acquire(mapping_config.table_name):
            if not table_exists or import_strategy == "NEW_TABLE":
                create_table_if_not_exists(engine, mapping_config)
                logger.info(f"Created table: {mapping_config.table_name}")
            
            # Insert records
            try:
                records_inserted, duplicates_skipped = insert_records(
                    engine,
                    mapping_config.table_name,
                    mapped_records,
                    config=mapping_config,
                    file_content=file_content,
                    file_name=file_name,
                    pre_mapped=True,
                )
            except ValueError as exc:
                error_text = str(exc)
                if "Numeric overflow" in error_text:
                    mapping_errors = [
                        {
                            "type": "numeric_overflow",
                            "message": error_text,
                        }
                    ]
                    llm_followup = (
                        "A numeric overflow occurred during insertion. "
                        "Update the mapping to widen overflowing columns to BIGINT or DECIMAL and retry."
                    )
                    duration = time.time() - start_time
                    complete_import_tracking(
                        import_id=import_id,
                        status="failed",
                        total_rows_in_file=raw_total_rows,
                        rows_processed=raw_total_rows,
                        rows_inserted=0,
                        rows_skipped=0,
                        duplicates_found=0,
                        validation_errors=len(mapping_errors),
                        duration_seconds=duration,
                        parsing_time_seconds=parse_time,
                        duplicate_check_time_seconds=None,
                        insert_time_seconds=None,
                        error_message=error_text,
                        metadata={
                            "mapping_errors": mapping_errors,
                            "widened_columns": widened_columns,
                        },
                    )
                    if job_id:
                        update_import_job(
                            job_id,
                            status="waiting_user",
                            stage="analysis",
                            error_message=error_text,
                        )
                    return {
                        "success": False,
                        "message": error_text,
                        "records_processed": 0,
                        "duplicates_skipped": 0,
                        "table_name": mapping_config.table_name,
                        "import_id": import_id,
                        "mapping_errors": mapping_errors,
                        "llm_followup": llm_followup,
                        "needs_user_input": True,
                        "type_mismatch_summary": type_mismatch_summary,
                    }
                raise
            
            # Manage table metadata (inside lock to ensure consistency)
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
                    
        insert_time = time.time() - insert_start
        logger.info(f"Inserted {records_inserted} records in {insert_time:.2f}s (skipped {duplicates_skipped} duplicates)")
        
        # Complete import tracking with structured metadata
        duration = time.time() - start_time
        metadata_payload: Dict[str, Any] = {}
        if type_mismatch_summary:
            metadata_payload["type_mismatch_summary"] = type_mismatch_summary
        if intra_file_duplicates_skipped:
            metadata_payload["intra_file_duplicates_skipped"] = intra_file_duplicates_skipped
        if chunk_status_summary:
            metadata_payload["mapping_chunk_status"] = chunk_status_summary
        if widened_columns:
            metadata_payload["widened_columns"] = widened_columns
        if row_count_warning:
            metadata_payload["row_count_warning"] = row_count_warning

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
            row_count_warning,
            duplicate_followup,
            _build_type_mismatch_followup(mapping_config.table_name, type_mismatch_summary)
        ]
        followup_message = "\n\n".join([part for part in followup_parts if part]) if any(followup_parts) else ""
        needs_user_input = duplicates_skipped > 0 or bool(row_count_warning)

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
            "mapping_errors": combined_errors if combined_errors else [],
            "type_mismatch_summary": type_mismatch_summary,
            "duration_seconds": duration,
            "llm_followup": followup_message or None,
            "needs_user_input": needs_user_input,
            "duplicate_rows": duplicate_rows if duplicate_rows else None,
            "duplicate_rows_count": duplicate_total if duplicate_total else None,
            "import_id": import_id,
            "row_count_warning": row_count_warning,
        }
        
    except FileAlreadyImportedException as e:
        if import_id:
            _mark_mapping_failed(import_id, job_id, str(e))
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
            _mark_mapping_failed(import_id, job_id, str(e))
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
            _mark_mapping_failed(import_id, job_id, str(e))
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

from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import io
import logging
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
import numbers
from app.api.schemas.shared import MappingConfig
from app.utils.date import parse_flexible_date, detect_date_column

logger = logging.getLogger(__name__)


def _build_mapping_error(
    *,
    error_type: str,
    message: str,
    column: Optional[str] = None,
    expected_type: Optional[str] = None,
    value: Optional[Any] = None,
    record_number: Optional[int] = None,
) -> Dict[str, Any]:
    """Create a structured error payload for downstream processing."""
    error_payload: Dict[str, Any] = {
        "type": error_type,
        "message": message
    }
    if column is not None:
        error_payload["column"] = column
    if expected_type is not None:
        error_payload["expected_type"] = expected_type
    if record_number is not None:
        error_payload["record_number"] = record_number
    if value is not None:
        if isinstance(value, (int, float, str, bool)):
            error_payload["value"] = value
        else:
            error_payload["value"] = str(value)
    return error_payload


def map_data(
    records: List[Dict[str, Any]],
    config: MappingConfig,
    *,
    row_offset: int = 0,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Map input data according to the configuration.
    
    Optimized for performance:
    - Pre-computes mapping items once (not per record)
    - Uses list comprehension for fast path (no rules)
    - Minimizes dictionary operations
    - Automatically converts date columns based on schema type

    Returns:
    Tuple of (mapped_records, list_of_all_errors)
    """
    all_errors: List[Dict[str, Any]] = []
    
    # Pre-compute mapping items ONCE (not N times in loop)
    # Convert to tuple for faster iteration
    mapping_items = tuple(config.mappings.items())
    if not mapping_items:
        raise ValueError("Mapping configuration contains no column mappings; aborting to prevent empty inserts.")

    rules = config.rules or {}
    pre_map_transformations = rules.get("column_transformations", [])
    has_pre_map_transformations = bool(pre_map_transformations)

    source_fields = {src for _, src in mapping_items if src}
    if not source_fields:
        raise ValueError("Mapping configuration is missing source column references; cannot map records safely.")

    if records:
        observed_columns: set = set()
        for record in records[:50]:  # Sample a subset to keep checks cheap
            try:
                effective_record = (
                    _apply_column_transformations(record, pre_map_transformations)
                    if has_pre_map_transformations
                    else record
                )
                observed_columns.update(effective_record.keys())
            except Exception:
                continue
        observed_columns.discard(None)
        if source_fields.isdisjoint(observed_columns):
            raise ValueError(
                "Mapped source columns are missing from the transformed data. "
                f"Expected at least one of {sorted(source_fields)}, "
                f"but saw columns {sorted(observed_columns)[:10]}."
            )
    
    # Identify date/timestamp/integer columns from schema for automatic conversion
    date_columns = set()
    integer_columns = set()
    numeric_columns = set()
    if config.db_schema:
        for col_name, col_type in config.db_schema.items():
            if not col_type:
                continue
            col_type_upper = col_type.upper()
            if 'TIMESTAMP' in col_type_upper or 'DATE' in col_type_upper:
                date_columns.add(col_name)
            if 'INT' in col_type_upper:
                integer_columns.add(col_name)
                numeric_columns.add(col_name)
            if any(keyword in col_type_upper for keyword in ('DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL')):
                numeric_columns.add(col_name)
    
    # Check if we need to apply rules or date conversions
    has_rules = bool(rules)
    has_date_columns = bool(date_columns)
    has_integer_columns = bool(integer_columns)
    has_numeric_columns = bool(numeric_columns - integer_columns)
    
    # Fast path: no rules and no date/integer columns to convert
    if not has_rules and not has_date_columns and not has_integer_columns and not has_numeric_columns and not has_pre_map_transformations:
        # Use list comprehension - significantly faster than append loop
        mapped_records = [
            {output_col: record.get(input_field) 
             for output_col, input_field in mapping_items}
            for record in records
        ]
        return mapped_records, all_errors
    
    # Process records with rules and/or date conversion
    mapped_records = []
    for idx, record in enumerate(records, start=row_offset + 1):
        record_number = record.get("_source_record_number") or idx
        source_record = record
        if has_pre_map_transformations:
            source_record = _apply_column_transformations(record, pre_map_transformations)
        
        # Use dict comprehension for mapping (faster than loop with assignment)
        mapped_record = {output_col: source_record.get(input_field) 
                        for output_col, input_field in mapping_items}
        # Normalize integer columns to avoid float artifacts like 840.0
        if integer_columns:
            for col_name in integer_columns:
                if col_name not in mapped_record:
                    continue
                value = mapped_record[col_name]
                if isinstance(value, bool):
                    continue  # bool should not be coerced
                if value is None:
                    continue
                if isinstance(value, (int,)):
                    continue
                if isinstance(value, Decimal):
                    if value == value.to_integral():
                        mapped_record[col_name] = int(value)
                    else:
                        message = f"Non-integer decimal value '{value}' detected for integer column '{col_name}'. Value set to None."
                        all_errors.append(_build_mapping_error(
                            error_type="type_mismatch",
                            message=message,
                            column=col_name,
                            expected_type=config.db_schema.get(col_name),
                            value=value,
                            record_number=record_number,
                        ))
                        logger.warning(message)
                        mapped_record[col_name] = None
                    continue
                if isinstance(value, numbers.Real):
                    if pd.isna(value):
                        mapped_record[col_name] = None
                        continue
                    if float(value).is_integer():
                        mapped_record[col_name] = int(value)
                    else:
                        message = f"Non-integer numeric value '{value}' detected for integer column '{col_name}'. Value set to None."
                        all_errors.append(_build_mapping_error(
                            error_type="type_mismatch",
                            message=message,
                            column=col_name,
                            expected_type=config.db_schema.get(col_name),
                            value=value,
                            record_number=record_number,
                        ))
                        logger.warning(message)
                        mapped_record[col_name] = None
                    continue
                if isinstance(value, str):
                    value_str = value.strip()
                    if not value_str:
                        mapped_record[col_name] = None
                        continue
                    normalized_str = value_str.replace(',', '')
                    if normalized_str.startswith('$'):
                        normalized_str = normalized_str[1:]
                    if normalized_str.startswith('(') and normalized_str.endswith(')'):
                        normalized_str = f"-{normalized_str[1:-1]}"
                    try:
                        numeric_value = Decimal(normalized_str)
                    except InvalidOperation:
                        message = f"Non-numeric value '{value}' detected for integer column '{col_name}'. Value set to None."
                        all_errors.append(_build_mapping_error(
                            error_type="type_mismatch",
                            message=message,
                            column=col_name,
                            expected_type=config.db_schema.get(col_name),
                            value=value,
                            record_number=record_number,
                        ))
                        logger.warning(message)
                        mapped_record[col_name] = None
                        continue
                    if numeric_value == numeric_value.to_integral():
                        mapped_record[col_name] = int(numeric_value)
                    else:
                        message = f"Value '{value}' is not an integer for column '{col_name}'. Value set to None."
                        all_errors.append(_build_mapping_error(
                            error_type="type_mismatch",
                            message=message,
                            column=col_name,
                            expected_type=config.db_schema.get(col_name),
                            value=value,
                            record_number=record_number,
                        ))
                        logger.warning(message)
                        mapped_record[col_name] = None
                    continue
                # Unsupported type for integer column
                mapped_record[col_name] = None
        # Normalize other numeric columns (DECIMAL/NUMERIC/FLOAT/DOUBLE/REAL) from strings
        if numeric_columns:
            for col_name in numeric_columns - integer_columns:
                if col_name not in mapped_record:
                    continue
                value = mapped_record[col_name]
                if value is None or isinstance(value, bool):
                    continue
                if isinstance(value, (int, float, Decimal)):
                    continue
                if isinstance(value, numbers.Real):
                    if pd.isna(value):
                        mapped_record[col_name] = None
                    continue
                if isinstance(value, str):
                    value_str = value.strip()
                    if not value_str:
                        mapped_record[col_name] = None
                        continue
                    normalized_str = value_str.replace(',', '')
                    if normalized_str.startswith('$'):
                        normalized_str = normalized_str[1:]
                    if normalized_str.startswith('(') and normalized_str.endswith(')'):
                        normalized_str = f"-{normalized_str[1:-1]}"
                    try:
                        mapped_record[col_name] = Decimal(normalized_str)
                    except InvalidOperation:
                        message = f"Non-numeric value '{value}' detected for numeric column '{col_name}'. Value set to None."
                        all_errors.append(_build_mapping_error(
                            error_type="type_mismatch",
                            message=message,
                            column=col_name,
                            expected_type=config.db_schema.get(col_name),
                            value=value,
                            record_number=record_number,
                        ))
                        logger.warning(message)
                        mapped_record[col_name] = None

        # Convert any integral numeric values (even DECIMAL columns) to ints for display consistency
        for col_name, value in list(mapped_record.items()):
            if value is None:
                continue
            if isinstance(value, bool):
                continue  # preserve boolean True/False
            if isinstance(value, int):
                continue
            if isinstance(value, Decimal):
                if value == value.to_integral():
                    mapped_record[col_name] = int(value)
                continue
            if isinstance(value, numbers.Real):
                # pandas may surface NaN as float; guard before converting
                if pd.isna(value):
                    mapped_record[col_name] = None
                    continue
                if float(value).is_integer():
                    mapped_record[col_name] = int(value)
                else:
                    mapped_record[col_name] = float(value)

        # Apply automatic date conversion for TIMESTAMP/DATE columns
        if has_date_columns:
            for col_name in date_columns:
                if col_name in mapped_record:
                    original_value = mapped_record[col_name]
                    if original_value is not None:
                        # Defensive check: skip obviously non-date values
                        value_str = str(original_value).strip()

                        # Skip if value contains email pattern
                        if '@' in value_str:
                            logger.info(f"Skipping date conversion for '{col_name}': value '{value_str}' appears to be an email")
                            mapped_record[col_name] = original_value
                            continue

                        # Skip if value looks like a name (single word with capital letter, no numbers)
                        if value_str and value_str[0].isupper() and value_str.isalpha() and len(value_str) < 30:
                            logger.info(f"Skipping date conversion for '{col_name}': value '{value_str}' appears to be a name")
                            mapped_record[col_name] = original_value
                            continue

                        # Try to convert the date
                        converted_value = parse_flexible_date(
                            original_value, log_context=f"{col_name}"
                        )
                        if converted_value is None and value_str:
                            # Conversion failed for non-empty value
                            message = f"Failed to convert datetime field '{col_name}' with value '{original_value}'"
                            all_errors.append(_build_mapping_error(
                                error_type="datetime_conversion",
                                message=message,
                                column=col_name,
                                expected_type=config.db_schema.get(col_name),
                                value=original_value,
                                record_number=idx,
                            ))
                            logger.debug(message)
                        mapped_record[col_name] = converted_value
        
        # Apply rules if present
        if has_rules:
            mapped_record, record_errors = apply_rules(mapped_record, rules)
            all_errors.extend(record_errors)
        
        mapped_records.append(mapped_record)

    return mapped_records, all_errors


def _apply_column_transformations(
    record: Dict[str, Any],
    transformations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Apply pre-mapping column transformations to a single record.

    Returns a new dictionary when modifications are needed; otherwise returns the original record.
    """
    updated_record: Optional[Dict[str, Any]] = None

    for transformation in transformations:
        if not transformation or not isinstance(transformation, dict):
            continue

        t_type = transformation.get("type")
        if not t_type:
            continue

        # Lazily copy when the first transformation needs to mutate data
        if updated_record is None:
            updated_record = dict(record)

        if t_type == "split_multi_value_column":
            _apply_split_multi_value(updated_record, record, transformation)
        elif t_type == "compose_international_phone":
            _apply_compose_international_phone(updated_record, record, transformation)
        elif t_type == "split_international_phone":
            _apply_split_international_phone(updated_record, record, transformation)
        elif t_type == "regex_replace":
            _apply_regex_replace(updated_record, record, transformation)
        elif t_type == "merge_columns":
            _apply_merge_columns(updated_record, record, transformation)
        elif t_type == "explode_list_column":
            _apply_explode_list_column(updated_record, record, transformation)
        else:
            logger.debug("Unknown column transformation type '%s' skipped", t_type)

    return updated_record if updated_record is not None else record


def _apply_regex_replace(
    destination: Dict[str, Any],
    source_record: Dict[str, Any],
    transformation: Dict[str, Any],
) -> None:
    """Regex substitution (with optional capture outputs) on a column before mapping."""
    pattern = transformation.get("pattern")
    replacement = transformation.get("replacement", "")
    source_column = transformation.get("source_column") or transformation.get("column")
    target_column = (
        transformation.get("target_column")
        or transformation.get("target_field")
        or source_column
    )
    outputs = transformation.get("outputs") or transformation.get("targets") or []
    skip_on_no_match = transformation.get("skip_on_no_match", False)

    if not pattern or not source_column or not target_column:
        return

    try:
        compiled = re.compile(pattern)
    except re.error:
        logger.debug("Invalid regex pattern for regex_replace: %s", pattern)
        return

    value = source_record.get(source_column)
    if value is None:
        destination[target_column] = None
        return
    if isinstance(value, float) and pd.isna(value):
        destination[target_column] = None
        return

    text = str(value)
    match = compiled.search(text)

    if outputs:
        if not match:
            if skip_on_no_match:
                # Preserve original value when skip_on_no_match is True
                destination[target_column] = value
                return
            # When no match and skip_on_no_match is False, preserve original value instead of setting to None
            # This prevents data loss when regex patterns don't match
            logger.debug(
                "regex_replace: Pattern '%s' did not match value '%s' in column '%s'. Preserving original value.",
                pattern, text[:50], source_column
            )
            destination[target_column] = value
            for output in outputs:
                name = output.get("name") or output.get("field") or output.get("column")
                if name and name != target_column:
                    destination[name] = None
            return

        for output in outputs:
            if not isinstance(output, dict):
                continue
            name = output.get("name") or output.get("field") or output.get("column")
            if not name:
                continue
            group_id = output.get("group") or output.get("index")
            if group_id is None:
                destination[name] = match.group(0)
                continue
            try:
                destination[name] = match.group(group_id)
            except IndexError:
                destination[name] = output.get("default")
            except Exception:
                destination[name] = output.get("default")
        return

    if not match:
        if skip_on_no_match:
            # Preserve original value
            destination[target_column] = value
        else:
            # Still apply replacement even if no match (will return original text)
            destination[target_column] = compiled.sub(replacement, text)
        return

    destination[target_column] = compiled.sub(replacement, text)


def _apply_split_multi_value(
    destination: Dict[str, Any],
    source_record: Dict[str, Any],
    transformation: Dict[str, Any],
) -> None:
    source_column = transformation.get("source_column") or transformation.get("column")
    outputs = transformation.get("outputs") or transformation.get("targets") or []
    delimiter = transformation.get("delimiter")
    strip_whitespace = transformation.get("strip_whitespace", True)

    if not source_column or not outputs:
        return

    raw_value = source_record.get(source_column)
    if raw_value in (None, "", "null"):
        for output in outputs:
            name = output.get("name") or output.get("field") or output.get("column")
            if name:
                destination[name] = None
        return

    values = _parse_multi_value_list(raw_value, delimiter=delimiter, strip_whitespace=strip_whitespace)
    for output in outputs:
        if not isinstance(output, dict):
            continue
        index = output.get("index", 0)
        name = output.get("name") or output.get("field") or output.get("column")
        if name is None:
            continue
        value = values[index] if index < len(values) else output.get("default")
        destination[name] = value


def _parse_multi_value_list(raw_value: Any, *, delimiter: Optional[str] = None, strip_whitespace: bool = True) -> List[Any]:
    """Normalize a multi-value cell into a list using JSON, delimiter, or comma/semicolon fallback."""
    if isinstance(raw_value, list):
        values = raw_value
    elif isinstance(raw_value, str):
        trimmed = raw_value.strip() if strip_whitespace else raw_value
        if not trimmed:
            return []
        # Try JSON first
        try:
            parsed = json.loads(trimmed)
            if isinstance(parsed, list):
                values = parsed
            else:
                values = [parsed]
        except (json.JSONDecodeError, TypeError, ValueError):
            values = None

        if values is None:
            if delimiter:
                regex = re.escape(delimiter)
                parts = re.split(regex, trimmed)
            else:
                email_tokens = re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+", trimmed, flags=re.IGNORECASE)
                if len(email_tokens) > 1:
                    parts = email_tokens
                else:
                    parts = re.split(r"[;,]", trimmed)
            values = [part.strip() if strip_whitespace else part for part in parts if part.strip() or not strip_whitespace]
            if not values:
                values = [trimmed]
    else:
        values = [raw_value]

    normalized: List[Any] = []
    for val in values:
        if isinstance(val, str) and strip_whitespace:
            val = val.strip()
        if val in ("", None):
            continue
        normalized.append(val)
    return normalized


def _apply_compose_international_phone(
    destination: Dict[str, Any],
    source_record: Dict[str, Any],
    transformation: Dict[str, Any],
) -> None:
    target_column = (
        transformation.get("target_column")
        or transformation.get("target_field")
        or transformation.get("column")
    )
    if not target_column:
        return

    components = transformation.get("components") or transformation.get("parts") or []
    component_map: Dict[str, Optional[str]]
    if isinstance(components, dict):
        component_map = {
            "country_code": components.get("country_code"),
            "area_code": components.get("area_code"),
            "subscriber_number": components.get("subscriber_number"),
            "extension": components.get("extension"),
        }
    else:
        component_map = {}
        for item in components:
            if not isinstance(item, dict):
                continue
            role = item.get("role")
            column = item.get("column") or item.get("source") or item.get("field")
            if role and column:
                component_map[role] = column

    composed = _compose_e164_phone(
        source_record.get(component_map.get("country_code")) if component_map.get("country_code") else None,
        source_record.get(component_map.get("area_code")) if component_map.get("area_code") else None,
        source_record.get(component_map.get("subscriber_number")) if component_map.get("subscriber_number") else None,
        source_record.get(component_map.get("extension")) if component_map.get("extension") else None,
    )
    destination[target_column] = composed


def _apply_split_international_phone(
    destination: Dict[str, Any],
    source_record: Dict[str, Any],
    transformation: Dict[str, Any],
) -> None:
    source_column = transformation.get("source_column") or transformation.get("column")
    outputs = transformation.get("outputs") or transformation.get("targets") or []

    if not source_column or not outputs:
        return

    split = _split_international_phone(source_record.get(source_column))

    for output in outputs:
        if not isinstance(output, dict):
            continue
        role = output.get("role")
        name = output.get("name") or output.get("field") or output.get("column")
        if not role or not name:
            continue
        destination[name] = split.get(role) if split else None

    if transformation.get("preserve_original") is False and split:
        destination[source_column] = None


def _apply_merge_columns(
    destination: Dict[str, Any],
    source_record: Dict[str, Any],
    transformation: Dict[str, Any],
) -> None:
    """Concatenate multiple source columns into a single target column."""
    sources = transformation.get("sources") or transformation.get("columns") or []
    target_column = transformation.get("target_column") or transformation.get("target_field") or transformation.get("column")
    separator = transformation.get("separator", " ")
    strip_whitespace = transformation.get("strip_whitespace", True)
    skip_nulls = transformation.get("skip_nulls", True)
    null_replacement = transformation.get("null_replacement", "")

    if not sources or not target_column:
        return

    pieces: List[str] = []
    for src in sources:
        value = source_record.get(src)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            if skip_nulls:
                continue
            value = null_replacement
        text = str(value)
        if strip_whitespace:
            text = text.strip()
        if skip_nulls and not text:
            continue
        pieces.append(text)

    destination[target_column] = separator.join(pieces) if pieces else None


def _apply_explode_list_column(
    destination: Dict[str, Any],
    source_record: Dict[str, Any],
    transformation: Dict[str, Any],
) -> None:
    """
    Split a list-like cell into fixed output columns without duplicating rows.
    """
    source_column = transformation.get("source_column") or transformation.get("column")
    outputs = transformation.get("outputs") or transformation.get("targets") or []
    delimiter = transformation.get("delimiter")
    strip_whitespace = transformation.get("strip_whitespace", True)
    dedupe_values = transformation.get("dedupe_values", True)
    case_insensitive_dedupe = transformation.get("case_insensitive_dedupe", True)

    if not source_column or not outputs:
        return

    raw_value = source_record.get(source_column)
    values = _parse_multi_value_list(raw_value, delimiter=delimiter, strip_whitespace=strip_whitespace)

    if dedupe_values and values:
        seen = set()
        normalized_values: List[Any] = []
        for val in values:
            key = val.lower() if case_insensitive_dedupe and isinstance(val, str) else val
            if key in seen:
                continue
            seen.add(key)
            normalized_values.append(val)
        values = normalized_values

    for output in outputs:
        if not isinstance(output, dict):
            continue
        index = output.get("index", 0)
        name = output.get("name") or output.get("field") or output.get("column")
        if name is None:
            continue
        value = values[index] if index < len(values) else output.get("default")
        destination[name] = value


def _compose_e164_phone(
    country_code: Optional[Any],
    area_code: Optional[Any],
    subscriber_number: Optional[Any],
    extension: Optional[Any] = None,
) -> Optional[str]:
    digits_country = _extract_digits(country_code)
    digits_area = _extract_digits(area_code)
    digits_subscriber = _extract_digits(subscriber_number)

    if not digits_country and not digits_subscriber:
        return None

    full_number = f"+{digits_country or ''}{digits_area}{digits_subscriber}"
    if full_number == "+":
        return None

    ext_digits = _extract_digits(extension)
    if ext_digits:
        full_number = f"{full_number}x{ext_digits}"

    return full_number


def _split_international_phone(value: Any) -> Dict[str, str]:
    if value in (None, "", "null"):
        return {}

    text = str(value).strip()
    if not text:
        return {}

    digits_only = _extract_digits(text)
    if not digits_only:
        return {}

    tokens = re.split(r"[^\d]+", text.lstrip("+"))
    tokens = [token for token in tokens if token]

    if tokens:
        country_code = tokens[0]
        subscriber_token = "".join(tokens[1:]) if len(tokens) > 1 else ""
        if not subscriber_token and len(digits_only) > len(country_code):
            subscriber_token = digits_only[len(country_code):]
    else:
        if len(digits_only) <= 10:
            return {}
        country_code = digits_only[:-10]
        subscriber_token = digits_only[-10:]

    if not subscriber_token:
        return {}

    result: Dict[str, str] = {
        "country_code": country_code,
        "subscriber_number": subscriber_token,
    }

    # Attempt to infer area code if subscriber appears longer than 10 digits
    if len(subscriber_token) > 10:
        result["area_code"] = subscriber_token[: len(subscriber_token) - 10]
        result["subscriber_number"] = subscriber_token[-10:]

    return result


def _extract_digits(value: Optional[Any]) -> str:
    if value is None:
        return ""
    return re.sub(r"\D", "", str(value))


def apply_rules_vectorized(df: pd.DataFrame, rules: Dict[str, Any]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Apply transformation rules to the DataFrame using vectorized operations.
    Much faster than row-by-row processing.

    Returns:
        Tuple of (transformed_dataframe, list_of_errors)
    """
    errors: List[Dict[str, Any]] = []
    transformations = rules.get('transformations', [])
    datetime_transformations = rules.get('datetime_transformations', [])

    # Apply general transformations (vectorized)
    for transformation in transformations:
        if transformation.get('type') == 'uppercase':
            field = transformation.get('field')
            if field in df.columns:
                # Vectorized uppercase operation
                df[field] = df[field].astype(str).str.upper()

    # Apply datetime transformations (vectorized)
    for dt_transformation in datetime_transformations:
        field = dt_transformation.get('field')
        source_format = dt_transformation.get('source_format')
        
        if field in df.columns:
            # Vectorized datetime conversion
            if source_format and source_format != "auto":
                # Try explicit format first
                converted = pd.to_datetime(df[field], format=source_format, errors='coerce')
                # If many failed, try auto-detection as fallback
                if converted.isna().sum() > len(df) * 0.5:  # If >50% failed
                    converted = pd.to_datetime(df[field], errors='coerce')
            else:
                # Auto-detect format
                converted = pd.to_datetime(df[field], errors='coerce')
            
            # Count conversion failures for non-empty values
            original_non_empty = df[field].notna() & (df[field].astype(str).str.strip() != '')
            conversion_failed = original_non_empty & converted.isna()
            failed_count = conversion_failed.sum()
            
            if failed_count > 0:
                message = f"Failed to convert {failed_count} datetime values in field '{field}'"
                errors.append(_build_mapping_error(
                    error_type="datetime_conversion",
                    message=message,
                    column=field
                ))
                logger.warning(message)
            
            # Format to ISO 8601
            # For date-only values, use date format; for datetime, use full ISO format
            df[field] = converted.apply(lambda x: 
                x.strftime('%Y-%m-%d') if pd.notna(x) and x.time() == datetime.min.time()
                else x.isoformat() if pd.notna(x)
                else None
            )

    return df, errors


def apply_rules(record: Dict[str, Any], rules: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Apply transformation rules to the record (legacy row-by-row version).

    Returns:
        Tuple of (transformed_record, list_of_errors)
    """
    errors: List[Dict[str, Any]] = []
    transformations = rules.get('transformations', [])
    datetime_transformations = rules.get('datetime_transformations', [])

    # Apply general transformations
    for transformation in transformations:
        # Example: {"type": "uppercase", "field": "name"}
        if transformation.get('type') == 'uppercase':
            field = transformation.get('field')
            if field in record and record[field]:
                record[field] = record[field].upper()

    # Apply datetime transformations
    for dt_transformation in datetime_transformations:
        field = dt_transformation.get('field')
        source_format = dt_transformation.get('source_format')
        target_format = dt_transformation.get('target_format', 'ISO8601')

        if field in record:
            original_value = record[field]
            standardized_value = standardize_datetime(original_value, source_format)

            if standardized_value is None and original_value is not None and str(original_value).strip():
                # Conversion failed for a non-empty value
                message = f"Failed to convert datetime field '{field}' with value '{original_value}'"
                errors.append(_build_mapping_error(
                    error_type="datetime_conversion",
                    message=message,
                    column=field,
                    value=original_value
                ))
                logger.warning(message)

            # Always update the record (None for failed conversions, standardized value for success)
            record[field] = standardized_value

    return record, errors


def standardize_datetime(value: Any, source_format: Optional[str] = None) -> Optional[str]:
    """
    Standardize datetime values to ISO 8601 format using flexible date parsing.

    Args:
        value: The datetime value to standardize
        source_format: Optional strftime format string (e.g., '%m/%d/%Y %I:%M %p')
                      If None, will attempt to infer the format

    Returns:
        ISO 8601 formatted string (YYYY-MM-DDTHH:MM:SSZ) or None if conversion fails or value is empty
    """
    # Use the new flexible date parser which handles multiple formats
    # and always returns ISO 8601 with timezone
    return parse_flexible_date(value, log_context="standardize_datetime")


def detect_column_type(series: pd.Series, has_datetime_transformation: bool = False) -> str:
    """Detect the appropriate SQL type for a pandas Series with conservative approach for data consolidation."""
    # Try to infer type from sample values
    sample_values = series.dropna().head(100)  # Sample first 100 non-null values
    INT32_MAX = 2_147_483_647
    INT64_MAX = 9_223_372_036_854_775_807

    if len(sample_values) == 0:
        return "TEXT"

    # Check if pandas has already detected this as datetime (e.g., from Excel date serial numbers)
    if pd.api.types.is_datetime64_any_dtype(series):
        # Series is already datetime-like, so it should be TIMESTAMP
        return "TIMESTAMP"

    # Convert to string for pattern matching
    sample_str = sample_values.astype(str)
    
    # Check for phone number patterns (must be TEXT, not NUMERIC)
    # Common formats: 415.610.7325, 415-610-7325, (415) 610-7325, etc.
    import re
    phone_patterns = [
        r'^\d{3}\.\d{3}\.\d{4}$',  # 415.610.7325
        r'^\d{3}-\d{3}-\d{4}$',    # 415-610-7325
        r'^\(\d{3}\)\s*\d{3}-\d{4}$',  # (415) 610-7325
        r'^\d{3}\s+\d{3}\s+\d{4}$',  # 415 610 7325
        r'^\+?\d{1,3}[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',  # International formats
    ]
    for pattern in phone_patterns:
        if sample_str.str.match(pattern, na=False).any():
            return "TEXT"
    
    # Check for percentage values (e.g., "98%", "2%")
    if sample_str.str.contains('%', regex=False, na=False).any():
        return "TEXT"
    
    # Check for email patterns
    if sample_str.str.contains('@', regex=False, na=False).any():
        return "TEXT"

    # Check if all values are numeric - use DECIMAL for all numeric data for maximum flexibility
    try:
        numeric_series = pd.to_numeric(sample_values, errors='raise')
        # Determine whether these are whole numbers and their magnitude
        has_fractional = not numeric_series.apply(float.is_integer).all()
        if has_fractional:
            return "DECIMAL"

        max_abs = numeric_series.abs().max()
        if max_abs > INT64_MAX:
            # Exceeds BIGINT — fall back to DECIMAL for safety
            return "DECIMAL"
        if max_abs > INT32_MAX:
            return "BIGINT"
        return "INTEGER"
    except (ValueError, TypeError):
        pass

    # Check if they look like dates
    try:
        pd.to_datetime(sample_values, errors='raise', format='mixed')

        # Test if our flexible date parser can handle these values
        conversion_success = True
        for val in sample_values.head(10):  # Test first 10 values
            if parse_flexible_date(val, log_failures=False) is None and pd.notna(val):
                conversion_success = False
                break

        if conversion_success:
            # Our date parser can handle these values, use TIMESTAMP
            # This enables automatic date conversion during mapping
            return "TIMESTAMP"
        else:
            # Date parsing uncertain, use TEXT for safety
            return "TEXT"

    except (ValueError, TypeError):
        pass

    # Use TEXT for all string columns to avoid length issues
    return "TEXT"


def detect_mapping_from_file(file_content: bytes, file_name: str, return_records: bool = False, has_header: Optional[bool] = None) -> tuple[str, MappingConfig, List[str], int, Optional[List[Dict[str, Any]]]]:
    """
    Detect mapping configuration from CSV or Excel file content.
    
    Args:
        file_content: Raw file content
        file_name: Name of the file
        return_records: If True, also return the parsed records to avoid re-parsing
        has_header: For CSV files, explicitly specify if file has headers (None = auto-detect)
    
    Returns:
        tuple: (file_type, mapping_config, columns_found, rows_sampled, records)
        Note: records will be None if return_records=False
    """
    # Detect file type
    if file_name.endswith('.csv'):
        file_type = 'csv'
        # Use has_header parameter if provided, otherwise pandas will auto-detect
        if has_header is None:
            df = pd.read_csv(io.BytesIO(file_content))
        elif has_header:
            df = pd.read_csv(io.BytesIO(file_content), header=0)
        else:
            # No header - read without header and generate column names
            df = pd.read_csv(io.BytesIO(file_content), header=None)
            df.columns = [f'col_{i}' for i in range(len(df.columns))]
    elif file_name.endswith(('.xlsx', '.xls')):
        file_type = 'excel'
        # Try openpyxl first (works for both .xlsx and .xls in many cases)
        try:
            df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
        except Exception:
            # Fallback to default pandas engine
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except Exception as e:
                raise ValueError(f"Could not read Excel file: {str(e)}")
    else:
        raise ValueError("Unsupported file type. Only CSV and Excel files are supported.")

    # Get column names
    columns_found = df.columns.tolist()
    rows_sampled = len(df)

    # Generate table name from filename (remove extension and sanitize)
    table_name = file_name.split('.')[0].replace('-', '_').replace(' ', '_').lower()
    # Ensure it's a valid SQL identifier
    import re
    table_name = re.sub(r'[^a-zA-Z0-9_]', '', table_name)
    if not table_name:
        table_name = 'auto_detected_table'
    if not table_name[0].isalpha():
        table_name = 'table_' + table_name

    # Add timestamp to make table name unique for testing
    import time
    table_name = f"{table_name}_{int(time.time())}"

    # Detect schema types
    db_schema = {}
    mappings = {}

    # Check if datetime transformations are defined (this would be passed in from LLM/user)
    # For now, we'll assume no transformations are defined during auto-detection
    # The LLM integration point would be to ask for format when datetime is detected
    has_datetime_transformations = False

    for col in columns_found:
        # Clean column name for SQL
        clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col)
        if not clean_col[0].isalpha() and clean_col[0] != '_':
            clean_col = 'col_' + clean_col

        db_schema[clean_col] = detect_column_type(df[col], has_datetime_transformations)
        mappings[clean_col] = col  # Map clean name to original name

    # Convert DataFrame to records if requested (to avoid re-parsing later)
    records = None
    if return_records:
        records = df.to_dict('records')
        # Convert pandas NaT values to None for database compatibility
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
    
    return file_type, MappingConfig(
        table_name=table_name,
        db_schema=db_schema,
        mappings=mappings,
        rules={}
    ), columns_found, rows_sampled, records

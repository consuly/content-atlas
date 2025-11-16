"""
Date parsing utilities for flexible date format handling.

This module provides utilities to parse dates from various formats and
standardize them to ISO 8601 format for consistent database storage.
"""

import pandas as pd
from typing import Any, Optional
import re
from datetime import datetime
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)

FAILED_SAMPLE_LIMIT = 5
SUPPRESSION_NOTICE_EVERY = 100

_failure_stats: dict = {}


def _record_parse_failure(value: Any, context: Optional[str], error: Exception) -> None:
    """
    Collect failure stats and emit limited logs (sampled warnings + periodic summaries).
    """
    key = context or "default"
    stats = _failure_stats.setdefault(key, {"count": 0, "samples": []})
    stats["count"] += 1
    count = stats["count"]

    if len(stats["samples"]) < FAILED_SAMPLE_LIMIT:
        stats["samples"].append(value)
        logger.warning("Failed to parse date%s value '%s': %s", f" ({key})" if context else "", value, error)
        return

    # Emit a single summary when suppression starts, then periodically.
    if count == FAILED_SAMPLE_LIMIT + 1 or count % SUPPRESSION_NOTICE_EVERY == 0:
        logger.info(
            "Suppressed additional date parse warnings after %d failures%s; sample values=%s",
            count,
            f" ({key})" if context else "",
            stats["samples"],
        )


def parse_flexible_date(value: Any, *, log_context: Optional[str] = None, log_failures: bool = True) -> Optional[str]:
    """
    Parse a date value from various formats and return ISO 8601 string.
    
    Supports formats:
    - ISO 8601: "2024-09-04T23:09:18Z"
    - DD/MM/YYYY: "20/10/2025"
    - MM/DD/YYYY: "10/20/2025"
    - YYYY-MM-DD: "2025-10-20"
    - And many others via pandas inference
    
    Args:
        value: Date value in any supported format
        
    Returns:
        ISO 8601 formatted string (YYYY-MM-DDTHH:MM:SSZ) or None if parsing fails
    """
    # Handle None/NaN
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    
    # Handle empty strings
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    
    parse_attempts = []
    dt = None
    
    if isinstance(value, str):
        numeric_match = re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', value)
        if numeric_match:
            date_segment = numeric_match.group(0)
            parts = re.split(r'[/-]', date_segment)
            try:
                first = int(parts[0])
                second = int(parts[1])
            except ValueError:
                first = second = -1  # Trigger fallback behaviour

            # Decide whether day-first is more plausible
            if first > 12 and second <= 31:
                dayfirst_preferred = True
            elif second > 12 and first <= 12:
                dayfirst_preferred = False
            elif first <= 12 and second <= 12:
                dayfirst_preferred = settings.date_default_dayfirst
            else:
                dayfirst_preferred = settings.date_default_dayfirst

            preferred_label = "dayfirst" if dayfirst_preferred else "monthfirst"
            parse_attempts.append((
                preferred_label,
                lambda v, df=dayfirst_preferred: pd.to_datetime(v, utc=True, dayfirst=df, errors='raise')
            ))

            # Always try the alternate interpretation as a fallback
            alternate = not dayfirst_preferred
            parse_attempts.append((
                "alternate_monthfirst" if dayfirst_preferred else "alternate_dayfirst",
                lambda v, df=alternate: pd.to_datetime(v, utc=True, dayfirst=df, errors='raise')
            ))

    # Fallback: let pandas infer the format (default behavior)
    parse_attempts.append(("default", lambda v: pd.to_datetime(v, utc=True, errors='raise')))
    
    last_error = None
    for attempt_name, attempt in parse_attempts:
        try:
            dt = attempt(value)
            break
        except Exception as exc:
            last_error = exc
            continue
    
    if dt is None:
        if log_failures:
            error_to_log = last_error or Exception("Unable to determine format")
            _record_parse_failure(value, log_context, error_to_log)
        return None
    
    # Convert to ISO 8601 format with Z suffix
    # If timezone-aware, convert to UTC and format
    if dt.tzinfo is not None:
        dt = dt.tz_convert('UTC')
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        # If timezone-naive, assume UTC and add Z
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def detect_date_column(values: list) -> bool:
    """
    Detect if a column contains date values based on pattern analysis.
    
    Args:
        values: List of sample values from the column
        
    Returns:
        True if column appears to contain dates, False otherwise
    """
    if not values:
        return False
    
    # Remove None/NaN values
    non_null_values = [v for v in values if v is not None and not (isinstance(v, float) and pd.isna(v))]
    
    if not non_null_values:
        return False
    
    # Need at least 50% of values to parse as dates
    successful_parses = 0
    total_checked = min(len(non_null_values), 20)  # Check up to 20 values
    
    for value in non_null_values[:total_checked]:
        if parse_flexible_date(value, log_failures=False) is not None:
            successful_parses += 1
    
    # If 50% or more parse successfully, consider it a date column
    return (successful_parses / total_checked) >= 0.5


def infer_date_format(values: list) -> Optional[str]:
    """
    Infer the date format used in a column by analyzing sample values.
    
    Args:
        values: List of sample values from the column
        
    Returns:
        String describing the detected format, or None if no clear format
    """
    if not values:
        return None
    
    # Remove None/NaN values
    non_null_values = [str(v) for v in values if v is not None and not (isinstance(v, float) and pd.isna(v))]
    
    if not non_null_values:
        return None
    
    # Check for common patterns
    sample = non_null_values[0]
    
    # ISO 8601 with timezone
    if re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', sample):
        return "ISO 8601"
    
    # DD/MM/YYYY
    if re.match(r'\d{2}/\d{2}/\d{4}', sample):
        return "DD/MM/YYYY"
    
    # MM/DD/YYYY
    if re.match(r'\d{2}/\d{2}/\d{4}', sample):
        return "MM/DD/YYYY or DD/MM/YYYY"
    
    # YYYY-MM-DD
    if re.match(r'\d{4}-\d{2}-\d{2}', sample):
        return "YYYY-MM-DD"
    
    return "Unknown format"


def standardize_date_column(values: list) -> list:
    """
    Standardize all date values in a column to ISO 8601 format.
    
    Args:
        values: List of date values in various formats
        
    Returns:
        List of ISO 8601 formatted strings (or None for unparseable values)
    """
    return [parse_flexible_date(v) for v in values]

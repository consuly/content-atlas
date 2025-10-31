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

logger = logging.getLogger(__name__)


def parse_flexible_date(value: Any) -> Optional[str]:
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
    if isinstance(value, str) and value.strip() == "":
        return None
    
    try:
        # Let pandas infer the format (infer_datetime_format is now default behavior)
        dt = pd.to_datetime(value, utc=True)
        
        # Convert to ISO 8601 format with Z suffix
        # If timezone-aware, convert to UTC and format
        if dt.tzinfo is not None:
            dt = dt.tz_convert('UTC')
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            # If timezone-naive, assume UTC and add Z
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
    except Exception as e:
        logger.warning(f"Failed to parse date value '{value}': {e}")
        return None


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
        if parse_flexible_date(value) is not None:
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

"""
Phone number standardization utilities for flexible phone format handling.

This module provides utilities to standardize phone numbers from various formats
and output them in different standardized formats (E.164, international, national, etc.).
"""

import re
from typing import Any, Optional, Dict
import logging

logger = logging.getLogger(__name__)


def standardize_phone(
    value: Any,
    *,
    default_country_code: Optional[str] = None,
    output_format: str = "e164",
    preserve_extension: bool = False,
    strip_leading_zeros: bool = True,
    min_digits: int = 7,
    max_digits: int = 15,
) -> Optional[str]:
    """
    Standardize phone numbers to various output formats.
    
    Handles various input formats:
    - (415) 555-1234
    - 415.555.1234
    - 415-555-1234
    - +1 415 555 1234
    - +44 20 7946 1234
    - 555-1234 x123 (with extension)
    
    Args:
        value: Phone number in any format
        default_country_code: Country code to add if missing (e.g., "1" for US, "44" for UK)
                             If None, no country code is added to numbers without one
        output_format: Output format - "e164", "international", "national", "digits_only"
        preserve_extension: If True, preserve extension (e.g., "x123") in output
        strip_leading_zeros: If True, remove leading zeros from local numbers
        min_digits: Minimum number of digits to consider valid (default 7)
        max_digits: Maximum number of digits to consider valid (default 15)
    
    Returns:
        Standardized phone string or None if invalid/empty
        
    Output formats:
        - e164: +14155551234
        - international: +1 415 555 1234
        - national: (415) 555-1234
        - digits_only: 4155551234
    """
    # Handle None/empty
    if value is None or value == "":
        return None
    
    # Convert to string and strip whitespace
    text = str(value).strip()
    if not text:
        return None
    
    # Extract extension if present (x123, ext 123, extension 123)
    extension = None
    if preserve_extension:
        extension_match = re.search(r'(?:x|ext|extension)[\s.]?(\d+)', text, re.IGNORECASE)
        if extension_match:
            extension = extension_match.group(1)
            # Remove extension from text for processing
            text = text[:extension_match.start()].strip()
    
    # Extract all digits
    digits = re.sub(r'\D', '', text)
    
    if not digits:
        return None
    
    # Strip leading zeros if requested
    if strip_leading_zeros:
        digits = digits.lstrip('0')
    
    # Validate digit count
    if len(digits) < min_digits or len(digits) > max_digits:
        logger.debug(
            f"Phone number '{value}' has {len(digits)} digits, "
            f"expected between {min_digits} and {max_digits}"
        )
        return None
    
    # Detect if number already has country code (starts with + or has 11+ digits)
    has_country_code = text.startswith('+') or len(digits) > 10
    
    # Parse country code and local number
    country_code = None
    local_number = digits
    
    if has_country_code:
        # Try to extract country code
        country_code, local_number = _extract_country_code(digits)
    elif default_country_code:
        # Add default country code if provided
        country_code = default_country_code
        local_number = digits
    
    # Format based on output_format
    if output_format == "digits_only":
        result = digits
    elif output_format == "e164":
        if country_code:
            result = f"+{country_code}{local_number}"
        else:
            result = local_number
    elif output_format == "international":
        if country_code:
            formatted_local = _format_local_number(local_number, country_code)
            result = f"+{country_code} {formatted_local}"
        else:
            result = _format_local_number(local_number, None)
    elif output_format == "national":
        result = _format_local_number(local_number, country_code)
    else:
        logger.warning(f"Unknown output_format '{output_format}', defaulting to e164")
        if country_code:
            result = f"+{country_code}{local_number}"
        else:
            result = local_number
    
    # Append extension if preserved
    if extension and preserve_extension:
        result = f"{result}x{extension}"
    
    return result


def _extract_country_code(digits: str) -> tuple[Optional[str], str]:
    """
    Extract country code from a string of digits.
    
    Returns:
        Tuple of (country_code, local_number)
    """
    # Common country code lengths: 1-3 digits
    # Try to intelligently detect based on known patterns
    
    # If starts with 1 and has 11 digits total, it's likely +1 (US/Canada)
    if digits.startswith('1') and len(digits) == 11:
        return ('1', digits[1:])
    
    # If starts with 44 and has 12+ digits, likely UK
    if digits.startswith('44') and len(digits) >= 12:
        return ('44', digits[2:])
    
    # If starts with 33 and has 11+ digits, likely France
    if digits.startswith('33') and len(digits) >= 11:
        return ('33', digits[2:])
    
    # If starts with 49 and has 11+ digits, likely Germany
    if digits.startswith('49') and len(digits) >= 11:
        return ('49', digits[2:])
    
    # If starts with 81 and has 11+ digits, likely Japan
    if digits.startswith('81') and len(digits) >= 11:
        return ('81', digits[2:])
    
    # If starts with 86 and has 12+ digits, likely China
    if digits.startswith('86') and len(digits) >= 12:
        return ('86', digits[2:])
    
    # If starts with 91 and has 12+ digits, likely India
    if digits.startswith('91') and len(digits) >= 12:
        return ('91', digits[2:])
    
    # If starts with 61 and has 11+ digits, likely Australia
    if digits.startswith('61') and len(digits) >= 11:
        return ('61', digits[2:])
    
    # If starts with 7 and has 11 digits, likely Russia
    if digits.startswith('7') and len(digits) == 11:
        return ('7', digits[1:])
    
    # Default: if more than 10 digits, assume first 1-3 are country code
    if len(digits) > 10:
        # Try 3-digit country code first
        if len(digits) >= 13:
            return (digits[:3], digits[3:])
        # Try 2-digit country code
        elif len(digits) >= 12:
            return (digits[:2], digits[2:])
        # Try 1-digit country code
        else:
            return (digits[:1], digits[1:])
    
    # No country code detected
    return (None, digits)


def _format_local_number(local_number: str, country_code: Optional[str]) -> str:
    """
    Format local number based on country conventions.
    
    Args:
        local_number: The local phone number digits
        country_code: The country code (if known)
    
    Returns:
        Formatted local number string
    """
    # US/Canada formatting (10 digits)
    if country_code in ('1', None) and len(local_number) == 10:
        return f"({local_number[:3]}) {local_number[3:6]}-{local_number[6:]}"
    
    # UK formatting (10 digits after country code)
    if country_code == '44' and len(local_number) == 10:
        return f"{local_number[:2]} {local_number[2:6]} {local_number[6:]}"
    
    # Generic formatting for other countries
    # Split into groups of 3-4 digits
    if len(local_number) <= 7:
        # Short numbers: 555-1234
        if len(local_number) == 7:
            return f"{local_number[:3]}-{local_number[3:]}"
        return local_number
    elif len(local_number) == 8:
        # 8 digits: 5555-1234
        return f"{local_number[:4]}-{local_number[4:]}"
    elif len(local_number) == 9:
        # 9 digits: 555-555-123
        return f"{local_number[:3]}-{local_number[3:6]}-{local_number[6:]}"
    elif len(local_number) == 10:
        # 10 digits: (555) 555-1234
        return f"({local_number[:3]}) {local_number[3:6]}-{local_number[6:]}"
    else:
        # Longer numbers: group by 3s with spaces
        parts = []
        for i in range(0, len(local_number), 3):
            parts.append(local_number[i:i+3])
        return ' '.join(parts)


def detect_phone_column(values: list) -> bool:
    """
    Detect if a column contains phone number values based on pattern analysis.
    
    Args:
        values: List of sample values from the column
        
    Returns:
        True if column appears to contain phone numbers, False otherwise
    """
    if not values:
        return False
    
    # Remove None values
    non_null_values = [v for v in values if v is not None]
    
    if not non_null_values:
        return False
    
    # Phone number patterns
    phone_patterns = [
        r'^\d{3}[-.\s]\d{3}[-.\s]\d{4}$',  # 415-555-1234, 415.555.1234, 415 555 1234
        r'^\(\d{3}\)\s*\d{3}[-.\s]?\d{4}$',  # (415) 555-1234, (415)555-1234
        r'^\+?\d{1,3}[\s.-]?\(?\d{2,4}\)?[\s.-]?\d{3,4}[\s.-]?\d{4}$',  # International formats
        r'^\d{10}$',  # 4155551234
        r'^\d{3}\.\d{3}\.\d{4}$',  # 415.555.1234
    ]
    
    # Need at least 50% of values to match phone patterns
    matches = 0
    total_checked = min(len(non_null_values), 20)  # Check up to 20 values
    
    for value in non_null_values[:total_checked]:
        value_str = str(value).strip()
        for pattern in phone_patterns:
            if re.match(pattern, value_str):
                matches += 1
                break
    
    # If 50% or more match phone patterns, consider it a phone column
    return (matches / total_checked) >= 0.5


def validate_phone(value: Any, *, min_digits: int = 7, max_digits: int = 15) -> bool:
    """
    Validate if a value is a valid phone number.
    
    Args:
        value: Value to validate
        min_digits: Minimum number of digits required
        max_digits: Maximum number of digits allowed
        
    Returns:
        True if valid phone number, False otherwise
    """
    if value is None or value == "":
        return False
    
    text = str(value).strip()
    if not text:
        return False
    
    # Extract digits
    digits = re.sub(r'\D', '', text)
    
    # Check digit count
    return min_digits <= len(digits) <= max_digits

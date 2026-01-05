"""
Preset regex validators for common data validation patterns.

This module provides a comprehensive set of pre-defined regex patterns
that can be used for validating common data types during imports.
"""

import re
from typing import Optional, Tuple


# Preset regex patterns for common validations
PRESET_PATTERNS = {
    # Contact & Communication
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    "email_strict": r"^(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])$",
    "phone": r"^\+?[\d\s\-\.\(\)]{7,20}$",  # Loose matching - default
    "phone_us": r"^(\+?1[\s.-]?)?(\([0-9]{3}\)|[0-9]{3})[\s.-]?[0-9]{3}[\s.-]?[0-9]{4}$",
    "phone_international": r"^\+[1-9]\d{6,14}$",  # E.164 format
    
    # Identifiers & Codes
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "ssn": r"^\d{3}-\d{2}-\d{4}$",
    "ein": r"^\d{2}-\d{7}$",
    "postal_code": r"^[A-Za-z0-9\s-]{3,10}$",
    "postal_code_us": r"^\d{5}(-\d{4})?$",
    "postal_code_ca": r"^[A-Za-z]\d[A-Za-z][\s-]?\d[A-Za-z]\d$",
    
    # Web & Network
    "url": r"^https?://[^\s/$.?#].[^\s]*$",
    "domain": r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$",
    "ipv4": r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
    "ipv6": r"^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:))$",
    
    # Financial
    "credit_card": r"^(?:\d{4}[-\s]?){3}\d{4}$",
    "currency_usd": r"^\$?[\d,]+(\.\d{2})?$",
    "iban": r"^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$",
    
    # Data Formats
    "date_iso": r"^\d{4}-\d{2}-\d{2}$",
    "date_us": r"^(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/\d{4}$",
    "time_24h": r"^([01]\d|2[0-3]):([0-5]\d)(:([0-5]\d))?$",
    "hex_color": r"^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$",
    "slug": r"^[a-z0-9]+(?:-[a-z0-9]+)*$",
    
    # Custom Business IDs
    "alphanumeric_id": r"^[A-Za-z0-9]+$",
    "sku": r"^[A-Za-z0-9\-_]+$",
}


# Human-readable descriptions for each preset
PRESET_DESCRIPTIONS = {
    "email": "Standard email format (permissive)",
    "email_strict": "RFC 5322 compliant email",
    "phone": "Loose phone number (7-20 digits with any separators)",
    "phone_us": "US phone number format",
    "phone_international": "E.164 international format (+country code)",
    "uuid": "UUID v4 format",
    "ssn": "US Social Security Number (###-##-####)",
    "ein": "US Employer Identification Number (##-#######)",
    "postal_code": "Postal code (alphanumeric)",
    "postal_code_us": "US ZIP code (5 or 9 digits)",
    "postal_code_ca": "Canadian postal code",
    "url": "HTTP/HTTPS URL",
    "domain": "Domain name",
    "ipv4": "IPv4 address",
    "ipv6": "IPv6 address",
    "credit_card": "Credit card number (16 digits with optional separators)",
    "currency_usd": "US Dollar amount",
    "iban": "International Bank Account Number",
    "date_iso": "ISO 8601 date (YYYY-MM-DD)",
    "date_us": "US date format (MM/DD/YYYY)",
    "time_24h": "24-hour time format (HH:MM or HH:MM:SS)",
    "hex_color": "Hex color code (#RGB or #RRGGBB)",
    "slug": "URL-safe slug (lowercase, hyphens)",
    "alphanumeric_id": "Alphanumeric identifier",
    "sku": "Product SKU (alphanumeric with hyphens/underscores)",
}


def get_preset_pattern(preset_name: str) -> Optional[str]:
    """
    Get the regex pattern for a preset validator.
    
    Args:
        preset_name: Name of the preset validator
        
    Returns:
        Regex pattern string or None if preset not found
    """
    return PRESET_PATTERNS.get(preset_name)


def get_preset_description(preset_name: str) -> Optional[str]:
    """
    Get the human-readable description for a preset validator.
    
    Args:
        preset_name: Name of the preset validator
        
    Returns:
        Description string or None if preset not found
    """
    return PRESET_DESCRIPTIONS.get(preset_name)


def validate_with_preset(
    value: str,
    preset_name: str,
    allow_null: bool = True
) -> Tuple[bool, Optional[str]]:
    """
    Validate a value against a preset pattern.
    
    Args:
        value: Value to validate
        preset_name: Name of the preset validator
        allow_null: Whether to allow null/empty values
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        if allow_null:
            return True, None
        return False, "Value is required"
    
    pattern = get_preset_pattern(preset_name)
    if pattern is None:
        return False, f"Unknown preset validator: {preset_name}"
    
    str_val = str(value).strip()
    
    try:
        if not re.match(pattern, str_val):
            description = get_preset_description(preset_name)
            return False, f"Value '{str_val}' does not match {description or preset_name} format"
        return True, None
    except re.error as e:
        return False, f"Regex error for preset '{preset_name}': {str(e)}"


def list_available_presets() -> dict:
    """
    Get a list of all available preset validators with their descriptions.
    
    Returns:
        Dictionary mapping preset names to descriptions
    """
    return PRESET_DESCRIPTIONS.copy()

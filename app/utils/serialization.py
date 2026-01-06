from typing import Any
from decimal import Decimal
from datetime import datetime, date

def _make_json_safe(value: Any) -> Any:
    """
    Convert Python objects into JSON-serialisable structures, preserving
    as much fidelity as possible.
    """
    if isinstance(value, dict):
        return {key: _make_json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_make_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_make_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_make_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        # Keep integers as ints, otherwise convert to string to avoid precision loss
        if value == value.to_integral():
            return int(value)
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode(errors="ignore")
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    # Fallback to string representation for unsupported types
    return str(value)

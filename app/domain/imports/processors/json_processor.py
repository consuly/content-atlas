import json
from typing import List, Dict, Any


def process_json(file_content: bytes) -> List[Dict[str, Any]]:
    """Process JSON file and return list of dictionaries."""
    data = json.loads(file_content.decode('utf-8'))

    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return [data]
    else:
        raise ValueError("JSON must contain an object or array of objects")

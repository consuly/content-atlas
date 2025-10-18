from typing import List, Dict, Any
from .schemas import MappingConfig


def map_data(records: List[Dict[str, Any]], config: MappingConfig) -> List[Dict[str, Any]]:
    """Map input data according to the configuration."""
    mapped_records = []

    for record in records:
        mapped_record = {}
        for output_col, input_field in config.mappings.items():
            mapped_record[output_col] = record.get(input_field)

        # Apply rules if any
        if config.rules:
            mapped_record = apply_rules(mapped_record, config.rules)

        mapped_records.append(mapped_record)

    return mapped_records


def apply_rules(record: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    """Apply transformation rules to the record."""
    # For now, basic implementation
    # Can extend with more complex rules
    transformations = rules.get('transformations', [])

    for transformation in transformations:
        # Example: {"type": "uppercase", "field": "name"}
        if transformation.get('type') == 'uppercase':
            field = transformation.get('field')
            if field in record and record[field]:
                record[field] = record[field].upper()

    return record

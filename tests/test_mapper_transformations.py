import pytest

from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig
from app.domain.imports.mapper import map_data, standardize_datetime, apply_rules


def _base_config(mappings, rules=None, db_schema=None):
    return MappingConfig(
        table_name="test_table",
        db_schema=db_schema or {},
        mappings=mappings,
        rules=rules or {},
        unique_columns=[],
        duplicate_check=DuplicateCheckConfig()
    )


def test_map_data_splits_multi_value_column():
    records = [
        {"contact_id": 1, "emails": '["primary@example.com","secondary@example.com"]'},
        {"contact_id": 2, "emails": '["solo@example.com"]'},
    ]

    rules = {
        "column_transformations": [
            {
                "type": "split_multi_value_column",
                "source_column": "emails",
                "outputs": [
                    {"name": "email_primary", "index": 0},
                    {"name": "email_secondary", "index": 1, "default": None},
                ],
            }
        ]
    }

    config = _base_config(
        mappings={
            "email_one": "email_primary",
            "email_two": "email_secondary",
        },
        rules=rules,
    )

    mapped, errors = map_data(records, config)
    assert not errors

    assert mapped[0]["email_one"] == "primary@example.com"
    assert mapped[0]["email_two"] == "secondary@example.com"
    assert mapped[1]["email_one"] == "solo@example.com"
    assert mapped[1]["email_two"] is None


def test_map_data_composes_international_phone():
    records = [
        {
            "country_code": "+1",
            "area_code": "555",
            "phone_number": "1234567",
            "extension": "101",
        }
    ]

    rules = {
        "column_transformations": [
            {
                "type": "compose_international_phone",
                "target_column": "international_phone_e164",
                "components": [
                    {"role": "country_code", "column": "country_code"},
                    {"role": "area_code", "column": "area_code"},
                    {"role": "subscriber_number", "column": "phone_number"},
                    {"role": "extension", "column": "extension"},
                ],
            }
        ]
    }

    config = _base_config(
        mappings={"international_phone": "international_phone_e164"},
        rules=rules,
    )

    mapped, errors = map_data(records, config)
    assert not errors
    assert mapped[0]["international_phone"] == "+15551234567x101"


def test_map_data_splits_international_phone():
    records = [
        {"international_phone": "+44 20 7946 1234"},
        {"international_phone": "+1-415-555-7890"},
    ]

    rules = {
        "column_transformations": [
            {
                "type": "split_international_phone",
                "source_column": "international_phone",
                "outputs": [
                    {"name": "intl_country", "role": "country_code"},
                    {"name": "intl_subscriber", "role": "subscriber_number"},
                ],
            }
        ]
    }

    config = _base_config(
        mappings={
            "country_code": "intl_country",
            "subscriber_number": "intl_subscriber",
        },
        rules=rules,
    )

    mapped, errors = map_data(records, config)
    assert not errors
    assert mapped[0]["country_code"] == "44"
    assert mapped[0]["subscriber_number"] == "2079461234"
    assert mapped[1]["country_code"] == "1"
    assert mapped[1]["subscriber_number"] == "4155557890"


def test_map_data_requires_column_mappings():
    records = [{"name": "Alice"}]
    config = _base_config(mappings={})

    with pytest.raises(ValueError, match="no column mappings"):
        map_data(records, config)


def test_map_data_errors_when_sources_missing_from_rows():
    records = [{"transformed_col": "value"}]
    config = _base_config(mappings={"target": "original_field"})

    with pytest.raises(ValueError, match="source columns are missing"):
        map_data(records, config)


def test_column_regex_replace_runs_before_mapping():
    records = [
        {"raw_phone": "(555) 111-2222"},
        {"raw_phone": "+1 333 444 5555"},
    ]

    rules = {
        "column_transformations": [
            {
                "type": "regex_replace",
                "source_column": "raw_phone",
                "target_column": "clean_phone",
                "pattern": "[^0-9]",
                "replacement": "",
            }
        ]
    }

    config = _base_config(
        mappings={"phone": "clean_phone"},
        rules=rules,
    )

    mapped, errors = map_data(records, config)
    assert not errors
    assert mapped[0]["phone"] == "5551112222"
    assert mapped[1]["phone"] == "13334445555"


def test_column_regex_replace_outputs_groups():
    records = [
        {"raw_email": "alpha@example.com"},
        {"raw_email": "beta@test.org"},
    ]

    rules = {
        "column_transformations": [
            {
                "type": "regex_replace",
                "source_column": "raw_email",
                "pattern": r"^([^@]+)@(.+)$",
                "outputs": [
                    {"name": "user_part", "group": 1},
                    {"name": "domain_part", "group": 2},
                ],
            }
        ]
    }

    config = _base_config(
        mappings={"user": "user_part", "domain": "domain_part"},
        rules=rules,
    )

    mapped, errors = map_data(records, config)
    assert not errors
    assert mapped[0]["user"] == "alpha"
    assert mapped[0]["domain"] == "example.com"
    assert mapped[1]["domain"] == "test.org"


def test_merge_columns_combines_sources_with_defaults():
    records = [
        {"first": "Alice", "last": "Smith"},
        {"first": "Bob", "last": None},
    ]

    rules = {
        "column_transformations": [
            {
                "type": "merge_columns",
                "sources": ["first", "last"],
                "target_column": "full_name",
                "separator": " ",
            }
        ]
    }

    config = _base_config(
        mappings={"name": "full_name"},
        rules=rules,
    )

    mapped, errors = map_data(records, config)
    assert not errors
    assert mapped[0]["name"] == "Alice Smith"
    assert mapped[1]["name"] == "Bob"


def test_explode_list_column_splits_into_outputs_without_row_duplication():
    records = [
        {"tags": "red, blue"},
        {"tags": ["green", "green", "yellow"]},
    ]

    rules = {
        "column_transformations": [
            {
                "type": "explode_list_column",
                "source_column": "tags",
                "outputs": [
                    {"name": "tag_primary", "index": 0},
                    {"name": "tag_secondary", "index": 1},
                ],
                "delimiter": ",",
                "dedupe_values": True,
            }
        ]
    }

    config = _base_config(
        mappings={"tag_primary": "tag_primary", "tag_secondary": "tag_secondary"},
        rules=rules,
    )

    mapped, errors = map_data(records, config)
    assert not errors
    assert mapped[0]["tag_primary"] == "red"
    assert mapped[0]["tag_secondary"] == "blue"
    assert mapped[1]["tag_primary"] == "green"
    assert mapped[1]["tag_secondary"] == "yellow"


def test_datetime_standardization():
    """Test datetime standardization functionality."""

    # Test standardize_datetime function with various formats
    test_cases = [
        # (input, expected_output)
        # Note: parse_flexible_date returns ISO 8601 with 'Z' suffix for UTC
        ('Thu, 9th Oct, 2025 at 8:11pm', '2025-10-09T20:11:00Z'),
        ('9/10/2025 20h11', '2025-09-10T20:11:00Z'),  # pandas interprets 9/10 as Sep 10 (monthfirst)
        ('10/09/25 8:11pm', '2025-10-09T20:11:00Z'),
        ('2025-10-09 20:11', '2025-10-09T20:11:00Z'),
        ('10/09/2025', '2025-10-09T00:00:00Z'),  # date only gets time added
        ('2025-10-09', '2025-10-09T00:00:00Z'),  # date only gets time added
        (None, None),
        ('', None),
        ('invalid date', None),
    ]

    for input_val, expected in test_cases:
        result = standardize_datetime(input_val)
        assert result == expected, f"Failed for input {repr(input_val)}: got {repr(result)}, expected {repr(expected)}"

    # Test with explicit format
    result = standardize_datetime('10/09/2025 8:11 PM', '%m/%d/%Y %I:%M %p')
    assert result == '2025-10-09T20:11:00Z'

    # Test apply_rules with datetime transformations
    record = {
        'event_date': '10/09/2025 8:11 PM',
        'name': 'Test Event'
    }

    rules = {
        'datetime_transformations': [
            {
                'field': 'event_date',
                'source_format': '%m/%d/%Y %I:%M %p',
                'target_format': 'ISO8601'
            }
        ]
    }

    transformed_record, errors = apply_rules(record, rules)
    assert transformed_record['event_date'] == '2025-10-09T20:11:00Z'
    assert transformed_record['name'] == 'Test Event'
    assert len(errors) == 0

    # Test error handling
    record_with_error = {
        'event_date': 'invalid datetime value',
        'name': 'Test Event'
    }

    transformed_record, errors = apply_rules(record_with_error, rules)
    assert transformed_record['event_date'] is None  # Failed conversion
    assert len(errors) == 1
    assert errors[0]["type"] == "datetime_conversion"
    assert 'Failed to convert datetime field' in errors[0]["message"]

    # Test map_data with datetime transformations
    records = [
        {'event_date': '10/09/2025 8:11 PM', 'name': 'Event 1'},
        {'event_date': '2025-10-10', 'name': 'Event 2'},  # date only
        {'event_date': 'invalid', 'name': 'Event 3'},  # invalid
    ]

    config = _base_config(
        mappings={'event_date': 'event_date', 'name': 'name'},
        rules=rules,
        db_schema={'event_date': 'TIMESTAMP', 'name': 'TEXT'}
    )

    mapped_records, all_errors = map_data(records, config)

    # Check successful conversions
    assert mapped_records[0]['event_date'] == '2025-10-09T20:11:00Z'
    assert mapped_records[1]['event_date'] == '2025-10-10T00:00:00Z'
    assert mapped_records[2]['event_date'] is None  # Failed conversion

    # Check that errors were collected
    assert len(all_errors) == 1
    assert all_errors[0]["type"] == "datetime_conversion"
    assert 'Failed to convert datetime field' in all_errors[0]["message"]

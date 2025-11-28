import pytest

from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig
from app.domain.imports.mapper import map_data


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

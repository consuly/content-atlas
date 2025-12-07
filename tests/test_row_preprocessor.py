from app.api.schemas.shared import DuplicateCheckConfig, MappingConfig
from app.domain.imports.mapper import map_data
from app.domain.imports.preprocessor import apply_row_transformations


def _config_with_rules(rules):
    return MappingConfig(
        table_name="contacts",
        db_schema={},
        mappings={},
        rules=rules,
        unique_columns=[],
        duplicate_check=DuplicateCheckConfig(),
    )


def test_explode_columns_creates_one_row_per_email_and_drops_sources():
    records = [
        {"name": "Alice", "email_1": "alice@example.com", "email_2": "alice+alt@example.com", "city": "SF"},
        {"name": "Bob", "email_1": None, "email_2": "   ", "city": "NYC"},
    ]

    rules = {
        "row_transformations": [
            {
                "type": "explode_columns",
                "source_columns": ["email_1", "email_2"],
                "target_column": "email",
                "drop_source_columns": True,
            }
        ]
    }

    config = _config_with_rules(rules)

    transformed, errors, stats = apply_row_transformations(records, config, row_offset=0)
    assert errors == []
    assert len(transformed) == 2
    emails = {row["email"] for row in transformed}
    assert emails == {"alice@example.com", "alice+alt@example.com"}
    assert all("email_1" not in row and "email_2" not in row for row in transformed)
    assert all(row["_source_record_number"] == 1 for row in transformed)


def test_record_numbers_survive_explosion_for_mapping_errors():
    records = [
        {
            "email_primary": "one@example.com",
            "email_secondary": "two@example.com",
            "age": "abc",  # triggers integer coercion error for every exploded row
        }
    ]

    rules = {
        "row_transformations": [
            {
                "type": "explode_columns",
                "source_columns": ["email_primary", "email_secondary"],
                "target_column": "email",
                "drop_source_columns": True,
            }
        ]
    }

    pre_config = _config_with_rules(rules)
    exploded, preprocess_errors, _ = apply_row_transformations(records, pre_config, row_offset=0)
    assert preprocess_errors == []
    assert len(exploded) == 2
    assert all(row["_source_record_number"] == 1 for row in exploded)

    map_config = MappingConfig(
        table_name="contacts",
        db_schema={"age": "INTEGER", "email": "VARCHAR"},
        mappings={"email": "email", "age": "age"},
        rules={},
        unique_columns=[],
        duplicate_check=DuplicateCheckConfig(),
    )

    _, mapping_errors = map_data(exploded, map_config)
    assert len(mapping_errors) == 2
    assert all(err.get("record_number") == 1 for err in mapping_errors)


def test_filter_rows_with_include_and_exclude_patterns():
    records = [
        {"email": "good@example.com", "status": "active"},
        {"email": "skip@example.com", "status": "inactive"},
        {"email": "other@example.com", "status": "archived"},
    ]

    rules = {
        "row_transformations": [
            {
                "type": "filter_rows",
                "include_regex": "@example\\.com",
                "exclude_regex": "inactive",
                "columns": ["email", "status"],
            }
        ]
    }
    config = _config_with_rules(rules)

    transformed, errors, _ = apply_row_transformations(records, config, row_offset=0)
    assert errors == []
    assert len(transformed) == 2
    emails = {row["email"] for row in transformed}
    assert emails == {"good@example.com", "other@example.com"}


def test_filter_rows_skips_when_columns_missing():
    records = [
        {"Email 1": "one@example.com"},
        {"Email 1": "two@example.com"},
    ]

    rules = {
        "row_transformations": [
            {
                "type": "filter_rows",
                "include_regex": "@example\\.com",
                "columns": ["email"],  # not present in records
            }
        ]
    }
    config = _config_with_rules(rules)

    transformed, errors, _ = apply_row_transformations(records, config, row_offset=0)
    assert transformed == records  # no rows dropped
    assert any("none of the requested columns exist" in err["message"] for err in errors)


def test_explode_columns_skips_when_all_sources_missing():
    records = [{"name": "Alice"}, {"name": "Bob"}]
    rules = {
        "row_transformations": [
            {
                "type": "explode_columns",
                "source_columns": ["email"],  # missing in records
                "target_column": "email",
            }
        ]
    }
    config = _config_with_rules(rules)

    transformed, errors, _ = apply_row_transformations(records, config, row_offset=0)
    assert transformed == records  # passthrough when nothing to explode
    assert any("none of the requested source columns exist" in err["message"] for err in errors)


def test_regex_replace_cleans_phone_numbers():
    records = [
        {"phone_raw": "(555) 123-4567", "name": "A"},
        {"phone_raw": "+1 555 987 6543 ext 2", "name": "B"},
    ]
    rules = {
        "row_transformations": [
            {
                "type": "regex_replace",
                "pattern": "[^0-9]",
                "replacement": "",
                "columns": ["phone_raw"],
            }
        ]
    }
    config = _config_with_rules(rules)

    transformed, errors, _ = apply_row_transformations(records, config, row_offset=0)
    assert errors == []
    assert [row["phone_raw"] for row in transformed] == ["5551234567", "155598765432"]


def test_regex_replace_outputs_capture_groups():
    records = [
        {"email": "one@example.com"},
        {"email": "two@sub.domain.org"},
    ]
    rules = {
        "row_transformations": [
            {
                "type": "regex_replace",
                "pattern": r"^([^@]+)@(.+)$",
                "columns": ["email"],
                "outputs": [
                    {"name": "email_user", "group": 1},
                    {"name": "email_domain", "group": 2},
                ],
            }
        ]
    }
    config = _config_with_rules(rules)

    transformed, errors, _ = apply_row_transformations(records, config, row_offset=0)
    assert errors == []
    assert transformed[0]["email_user"] == "one"
    assert transformed[0]["email_domain"] == "example.com"
    assert transformed[1]["email_domain"] == "sub.domain.org"


def test_conditional_transform_only_applies_to_matching_rows():
    records = [
        {"email": "keep@example.com", "notes": "ok"},
        {"email": "bad@example.com", "notes": "remove me"},
    ]

    rules = {
        "row_transformations": [
            {
                "type": "conditional_transform",
                "include_regex": "bad",
                "columns": ["email", "notes"],
                "actions": [
                    {
                        "type": "regex_replace",
                        "pattern": "bad",
                        "replacement": "good",
                        "columns": ["email"],
                    },
                ],
            }
        ]
    }
    config = _config_with_rules(rules)

    transformed, errors, _ = apply_row_transformations(records, config, row_offset=0)
    assert errors == []
    assert len(transformed) == 2
    emails = {row["email"] for row in transformed}
    assert "good@example.com" in emails
    assert "keep@example.com" in emails


def test_explode_list_rows_expands_list_column_and_dedupes():
    records = [
        {"name": "Alice", "emails": "a@example.com; a@example.com; b@example.com"},
        {"name": "Bob", "emails": None},
    ]
    rules = {
        "row_transformations": [
            {
                "type": "explode_list_rows",
                "source_column": "emails",
                "target_column": "email",
                "delimiter": ";",
                "dedupe_values": True,
                "drop_source_column": True,
            }
        ]
    }
    config = _config_with_rules(rules)

    transformed, errors, _ = apply_row_transformations(records, config, row_offset=0)
    assert errors == []
    assert len(transformed) == 2
    assert {row["email"] for row in transformed} == {"a@example.com", "b@example.com"}
    assert all("emails" not in row for row in transformed)


def test_concat_columns_respects_skip_nulls_defaults():
    records = [
        {"first": "Alice", "last": "Smith"},
        {"first": "Bob", "last": None},
    ]
    rules = {
        "row_transformations": [
            {
                "type": "concat_columns",
                "sources": ["first", "last"],
                "target_column": "full_name",
                "separator": " ",
            }
        ]
    }
    config = _config_with_rules(rules)

    transformed, errors, _ = apply_row_transformations(records, config, row_offset=0)
    assert errors == []
    assert transformed[0]["full_name"] == "Alice Smith"
    assert transformed[1]["full_name"] == "Bob"

from app.integrations.auto_import import _synthesize_multi_value_rules
from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig
from app.domain.imports.preprocessor import apply_row_transformations


def test_synthesizes_explode_for_numbered_columns_when_instruction_requests_split():
    column_mapping = {
        "Email 1": "email",
        "First Name": "first_name",
    }
    column_transformations = []
    row_transformations = []
    records = [
        {"Email 1": "a@example.com", "Email 2": "b@example.com", "First Name": "Alice"},
        {"Email 1": "c@example.com", "Email 2": None, "First Name": "Charlie"},
    ]

    new_mapping, new_col_xforms, new_row_xforms = _synthesize_multi_value_rules(
        column_mapping,
        column_transformations,
        row_transformations,
        records,
        "multiple emails per row, one per row",
    )

    assert new_mapping["email"] == "email"
    assert "Email 1" not in new_mapping
    assert new_col_xforms == []
    explode = next(rt for rt in new_row_xforms if rt.get("type") == "explode_columns")
    assert explode["target_column"] == "email"
    assert set(explode["source_columns"]) == {"Email 1", "Email 2"}


def test_synthesizes_split_and_explode_for_delimited_column():
    column_mapping = {"primary_email": "email"}
    column_transformations = []
    row_transformations = []
    records = [
        {"primary_email": "one@example.com, two@example.com"},
        {"primary_email": "three@example.com"},
    ]

    new_mapping, new_col_xforms, new_row_xforms = _synthesize_multi_value_rules(
        column_mapping,
        column_transformations,
        row_transformations,
        records,
        "split multiple emails into one per row",
    )

    assert new_mapping["email"] == "email"
    split = next(ct for ct in new_col_xforms if ct.get("type") == "split_multi_value_column")
    outputs = [out["name"] for out in split["outputs"]]
    assert outputs[:2] == ["primary_email_item_1", "primary_email_item_2"]
    explode = next(rt for rt in new_row_xforms if rt.get("type") == "explode_columns")
    assert explode["target_column"] == "email"
    assert set(explode["source_columns"]) >= set(outputs[:2])


def test_collapses_numbered_targets_to_base_email_and_explodes():
    column_mapping = {
        "Email 1": "email_1",
        "Email 2": "email_2",
        "Primary Email": "email_3",
        "First Name": "first_name",
    }
    records = [
        {
            "Email 1": "one@example.com",
            "Email 2": "two@example.com",
            "Primary Email": "prime@example.com",
            "First Name": "Alice",
        }
    ]

    new_mapping, new_col_xforms, new_row_xforms = _synthesize_multi_value_rules(
        column_mapping,
        [],
        [],
        records,
        "one email per row",
    )

    assert "email" in new_mapping
    assert all(key.startswith("email_") is False for key in new_mapping)
    explode = next(rt for rt in new_row_xforms if rt.get("type") == "explode_columns")
    assert explode["target_column"] == "email"
    assert set(explode["source_columns"]) == {"Email 1", "Email 2", "Primary Email"}
    assert new_col_xforms == []


def test_ignores_non_email_delimited_columns_when_instruction_is_email_specific():
    column_mapping = {
        "Email 1": "email",
        "location": "location",
    }
    records = [
        {"Email 1": "a@example.com", "location": "Zurich, Switzerland"},
        {"Email 1": "b@example.com", "location": "Basel, Switzerland"},
    ]

    new_mapping, new_col_xforms, new_row_xforms = _synthesize_multi_value_rules(
        column_mapping,
        [],
        [],
        records,
        "multiple emails per row, one per row",
    )

    # Email mapping should be rewritten to exploded target
    assert new_mapping.get("email") == "email"
    # No transformations should be added for location even though it contains commas.
    assert all(
        rt.get("target_column") != "location"
        for rt in new_row_xforms
        if isinstance(rt, dict)
    )
    assert all(
        ct.get("source_column") != "location"
        for ct in new_col_xforms
        if isinstance(ct, dict)
    )

def test_handles_fan_in_multiple_sources_same_target():
    column_mapping = {
        "Email 1": "email",
        "Email 2": "email",
        "Personal Email": "email",
        "First Name": "first_name",
    }
    records = [
        {
            "Email 1": "a@example.com",
            "Email 2": "b@example.com",
            "Personal Email": "c@example.com",
            "First Name": "Alice",
        }
    ]

    new_mapping, new_col_xforms, new_row_xforms = _synthesize_multi_value_rules(
        column_mapping,
        [],
        [],
        records,
        "one email per row",
    )

    assert new_mapping["email"] == "email"
    explode = next(rt for rt in new_row_xforms if rt.get("type") == "explode_columns")
    assert explode["target_column"] == "email"
    assert set(explode["source_columns"]) == {"Email 1", "Email 2", "Personal Email"}
    assert new_col_xforms == []


def test_directives_enable_explicit_splitting_when_auto_is_disabled():
    column_mapping = {"primary_email": "email"}
    column_transformations = []
    row_transformations = []
    records = [
        {"primary_email": "one@example.com; two@example.com"},
        {"primary_email": "three@example.com"},
    ]

    new_mapping, new_col_xforms, new_row_xforms = _synthesize_multi_value_rules(
        column_mapping,
        column_transformations,
        row_transformations,
        records,
        "split multiple emails into one per row",
        multi_value_directives=[
            {"source_column": "primary_email", "target_column": "email", "delimiter": "semicolon"},
        ],
        require_explicit_multi_value=True,
    )

    # Should rewrite mapping to exploded target and create directive-driven transforms.
    assert new_mapping["email"] == "email"
    split = next(ct for ct in new_col_xforms if ct.get("type") == "split_multi_value_column")
    assert split.get("delimiter") == ";"
    outputs = [out["name"] for out in split["outputs"]]
    assert outputs[:2] == ["primary_email_item_1", "primary_email_item_2"]
    explode = next(rt for rt in new_row_xforms if rt.get("type") == "explode_columns")
    assert explode["target_column"] == "email"
    assert set(explode["source_columns"]) == set(outputs)


def test_row_transformations_can_use_split_outputs_before_mapping():
    records = [
        {"emails": "one@example.com; two@example.com", "name": "Alice"},
    ]
    column_mapping = {"emails": "email", "name": "name"}

    cm, col_xforms, row_xforms = _synthesize_multi_value_rules(
        column_mapping,
        [],
        [],
        records,
        "split multiple emails into one per row",
    )

    mapping_config = MappingConfig(
        table_name="tmp",
        db_schema={"email": "TEXT", "name": "TEXT"},
        mappings={v: k for k, v in cm.items()},
        rules={
            "column_transformations": col_xforms,
            "row_transformations": row_xforms,
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=False,
            check_file_level=False,
            allow_duplicates=True,
            uniqueness_columns=[],
        ),
    )

    transformed, errors = apply_row_transformations(records, mapping_config)
    assert errors == []
    emails = sorted([row["email"] for row in transformed])
    assert emails == ["one@example.com", "two@example.com"]


def test_splits_multiple_emails_when_detected_in_single_cell():
    records = [
        {"email_address": "alpha@example.com beta@example.com", "name": "Alpha"},
        {"email_address": "gamma@example.com;delta@example.com", "name": "Gamma"},
    ]
    column_mapping = {"email_address": "email", "name": "name"}

    cm, col_xforms, row_xforms = _synthesize_multi_value_rules(
        column_mapping,
        [],
        [],
        records,
        "explode multiple emails into one row each",
    )

    mapping_config = MappingConfig(
        table_name="tmp",
        db_schema={"email": "TEXT", "name": "TEXT"},
        mappings={v: k for k, v in cm.items()},
        rules={
            "column_transformations": col_xforms,
            "row_transformations": row_xforms,
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=False,
            check_file_level=False,
            allow_duplicates=True,
            uniqueness_columns=[],
        ),
    )

    transformed, errors = apply_row_transformations(records, mapping_config)
    assert errors == []
    emails = sorted([row["email"] for row in transformed])
    assert emails == [
        "alpha@example.com",
        "beta@example.com",
        "delta@example.com",
        "gamma@example.com",
    ]


def test_fallback_includes_unmapped_email_columns_when_instruction_requests_split():
    column_mapping = {"Primary Email": "email", "First Name": "first_name"}
    records = [
        {
            "Primary Email": "prime@example.com",
            "Email 1": "extra@example.com",
            "Email 2": None,
            "First Name": "Alex",
        }
    ]

    cm, col_xforms, row_xforms = _synthesize_multi_value_rules(
        column_mapping,
        [],
        [],
        records,
        "one email per row",
    )

    assert col_xforms == []
    explode = next(rt for rt in row_xforms if rt.get("type") == "explode_columns")
    assert explode["target_column"] == "email"
    assert {"Primary Email", "Email 1"}.issubset(set(explode["source_columns"]))
    assert cm["email"] == "email"

"""
Tests for detecting column-level transformations required during LLM-driven
structure analysis. These scenarios ensure the agent can recommend pre-processing
steps such as splitting list columns or normalizing phone numbers before mapping.
"""

from types import SimpleNamespace
from pathlib import Path
from typing import Optional

from app.domain.imports.processors.csv_processor import (
    extract_raw_csv_rows,
    process_csv,
)
from app.domain.queries.analyzer import (
    AnalysisContext,
    AnalysisMode,
    ConflictResolutionMode,
    analyze_raw_csv_structure,
    make_import_decision,
)


FIXTURE_DIR = Path("tests/csv")


def _build_runtime_from_csv(relative_path: str) -> SimpleNamespace:
    file_path = FIXTURE_DIR / relative_path
    content = file_path.read_bytes()

    # Generate both parsed records and raw rows to mirror real analysis context
    parsed_records = process_csv(content)
    raw_rows = extract_raw_csv_rows(content, num_rows=10)

    context = AnalysisContext(
        file_sample=parsed_records[:5],  # typical sampling behavior
        file_metadata={
            "file_name": file_path.name,
            "raw_csv_rows": raw_rows,
            "total_rows": len(parsed_records),
        },
        existing_schema={},
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.LLM_DECIDE,
    )
    return SimpleNamespace(context=context)


def _get_transformations(result: dict) -> list:
    return result.get("transformations_needed") or []


def test_detects_multi_value_email_column():
    runtime = _build_runtime_from_csv("email-multi-value.csv")
    result = analyze_raw_csv_structure.func(runtime)

    transformations = _get_transformations(result)
    assert transformations, "Expected transformations for multi-value emails"

    payload = next(
        (item for item in transformations if item.get("type") == "split_multi_value_column"),
        None,
    )

    assert payload is not None, "Missing split_multi_value_column recommendation"
    assert payload.get("column") == "emails"
    assert payload.get("item_type") == "email"
    assert payload.get("max_items") == 3  # largest row has 3 emails


def test_detects_phone_component_combination():
    runtime = _build_runtime_from_csv("phones-split-columns.csv")
    result = analyze_raw_csv_structure.func(runtime)

    transformations = _get_transformations(result)
    assert transformations, "Expected transformations for phone component combination"

    payload = next(
        (item for item in transformations if item.get("type") == "compose_international_phone"),
        None,
    )
    assert payload is not None, "Missing compose_international_phone recommendation"

    components = {component["role"]: component["column"] for component in payload.get("components", [])}
    assert components.get("country_code") == "country_code"
    assert components.get("subscriber_number") == "phone_number"
    if "area_code" in components:
        assert components["area_code"] == "area_code"
    if "extension" in components:
        assert components["extension"] == "extension"
    assert payload.get("target_format") == "E.164"
    assert payload.get("reasoning")


def test_detects_international_phone_split():
    runtime = _build_runtime_from_csv("phones-international.csv")
    result = analyze_raw_csv_structure.func(runtime)

    transformations = _get_transformations(result)
    assert transformations, "Expected transformations for international phone splitting"

    payload = next(
        (item for item in transformations if item.get("type") == "split_international_phone"),
        None,
    )
    assert payload is not None, "Missing split_international_phone recommendation"
    assert payload.get("column") == "international_phone"

    output_columns = payload.get("output_columns", [])
    roles = {column.get("role") for column in output_columns}
    assert {"country_code", "subscriber_number"}.issubset(roles)
    assert payload.get("reasoning")


def _build_analysis_context(metadata: Optional[dict] = None) -> AnalysisContext:
    return AnalysisContext(
        file_sample=[{"placeholder": "value"}],
        file_metadata=metadata or {"file_type": "csv"},
        existing_schema={},
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.LLM_DECIDE,
    )


def test_make_import_decision_records_explicit_transformations():
    context = _build_analysis_context()
    runtime = SimpleNamespace(context=context)

    transformations = [
        {
            "type": "split_multi_value_column",
            "source_column": "emails",
            "outputs": [
                {"name": "email_primary", "index": 0},
                {"name": "email_secondary", "index": 1, "default": None},
            ],
        }
    ]

    result = make_import_decision.func(
        strategy="NEW_TABLE",
        target_table="contacts_import",
        reasoning="Requires preprocessing of array emails before mapping.",
        purpose_short="Contact emails",
        column_mapping={"emails": "email_primary"},
        runtime=runtime,
        unique_columns=["email_primary"],
        has_header=True,
        expected_column_types={"emails": "TEXT"},
        column_transformations=transformations,
    )

    assert result["success"] is True
    stored = context.file_metadata["llm_decision"]["column_transformations"]
    assert stored == transformations


def test_make_import_decision_defaults_to_detected_transformations_when_omitted():
    detected = [
        {
            "type": "compose_international_phone",
            "target_column": "international_phone_e164",
            "components": [
                {"role": "country_code", "column": "country_code"},
                {"role": "area_code", "column": "area_code"},
                {"role": "subscriber_number", "column": "phone_number"},
            ],
        }
    ]
    context = _build_analysis_context({"file_type": "csv", "detected_transformations": detected})
    runtime = SimpleNamespace(context=context)

    result = make_import_decision.func(
        strategy="NEW_TABLE",
        target_table="contacts_import",
        reasoning="Compose phones into single column before mapping.",
        purpose_short="Contact phones",
        column_mapping={"phone_number": "phone"},
        runtime=runtime,
        unique_columns=["phone"],
        has_header=True,
        expected_column_types={"phone_number": "TEXT"},
    )

    assert result["success"] is True
    stored = context.file_metadata["llm_decision"]["column_transformations"]
    assert stored == detected


def test_make_import_decision_rejects_malformed_add_column_migration():
    """Guard against bad schema_migrations payloads before execution."""
    context = _build_analysis_context({"file_type": "csv"})
    runtime = SimpleNamespace(context=context)

    result = make_import_decision.func(
        strategy="EXTEND_TABLE",
        target_table="contacts_import",
        reasoning="Extend table with new column.",
        purpose_short="Contact enrichment",
        column_mapping={"first_name": "first_name"},
        runtime=runtime,
        unique_columns=["first_name"],
        has_header=True,
        expected_column_types={"first_name": "TEXT"},
        schema_migrations=[{"action": "add_column"}],  # missing new_column block
    )

    assert "error" in result
    assert "add_column migration must include new_column" in result["error"]


def test_make_import_decision_accepts_well_formed_add_column_migration():
    """Valid add_column migrations should pass validation and be stored."""
    context = _build_analysis_context({"file_type": "csv"})
    runtime = SimpleNamespace(context=context)
    migrations = [
        {"action": "add_column", "new_column": {"name": "company_website", "type": "TEXT"}}
    ]

    result = make_import_decision.func(
        strategy="EXTEND_TABLE",
        target_table="contacts_import",
        reasoning="Extend table with website column.",
        purpose_short="Contact enrichment",
        column_mapping={"first_name": "first_name"},
        runtime=runtime,
        unique_columns=["first_name"],
        has_header=True,
        expected_column_types={"first_name": "TEXT"},
        schema_migrations=migrations,
    )

    assert result["success"] is True
    assert context.file_metadata["llm_decision"]["schema_migrations"] == migrations


def test_make_import_decision_infers_filter_rows_regex():
    """Filter rows with missing regex gets an inferred include_regex to avoid mapper errors."""
    context = _build_analysis_context({"file_type": "csv"})
    runtime = SimpleNamespace(context=context)

    row_transformations = [
        {
            "type": "filter_rows",
            "columns": ["primary_email"],
        }
    ]

    result = make_import_decision.func(
        strategy="NEW_TABLE",
        target_table="contacts_import",
        reasoning="Need to drop rows without usable emails.",
        purpose_short="Contact emails",
        column_mapping={"Primary Email": "primary_email"},
        runtime=runtime,
        unique_columns=["primary_email"],
        has_header=True,
        expected_column_types={"Primary Email": "TEXT"},
        row_transformations=row_transformations,
    )

    assert result["success"] is True
    stored = context.file_metadata["llm_decision"]["row_transformations"]
    payload = next((rt for rt in stored if rt.get("type") == "filter_rows"), None)
    assert payload is not None, "filter_rows transformation should be preserved"
    assert payload.get("include_regex"), "include_regex should be inferred when missing"
    assert payload.get("exclude_regex") is None
    assert payload.get("columns") == ["primary_email"]

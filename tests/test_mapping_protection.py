import pytest

from app.api.schemas.shared import (
    MappingConfig,
    RESERVED_SYSTEM_TABLES,
    ensure_safe_table_name,
)


def _base_mapping(table_name: str = "contacts") -> MappingConfig:
    return MappingConfig(
        table_name=table_name,
        db_schema={"name": "VARCHAR"},
        mappings={"name": "Full Name"},
        rules={}
    )


def test_mapping_config_auto_renames_reserved_table_names():
    """Reserved system table names should be auto-remapped, not rejected."""
    for table in RESERVED_SYSTEM_TABLES:
        config = _base_mapping(table_name=table)
        assert config.table_name == ensure_safe_table_name(table)
        assert config.table_name != table


def test_mapping_config_allows_non_reserved_names():
    """Regular table names should be accepted."""
    config = _base_mapping(table_name="marketing_contacts")
    assert config.table_name == "marketing_contacts"


def test_mapping_config_rejects_blank_names():
    """Empty or whitespace-only names should still raise errors."""
    with pytest.raises(ValueError):
        _base_mapping(table_name="")
    with pytest.raises(ValueError):
        _base_mapping(table_name="   ")

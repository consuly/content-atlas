import pytest

from app.api.schemas.shared import MappingConfig, RESERVED_SYSTEM_TABLES


def _base_mapping(table_name: str = "contacts") -> MappingConfig:
    return MappingConfig(
        table_name=table_name,
        db_schema={"name": "VARCHAR"},
        mappings={"name": "Full Name"},
        rules={}
    )


def test_mapping_config_rejects_reserved_table_names():
    """System tables should be off-limits for new mappings."""
    for table in RESERVED_SYSTEM_TABLES:
        with pytest.raises(ValueError):
            _base_mapping(table_name=table)


def test_mapping_config_allows_non_reserved_names():
    """Regular table names should be accepted."""
    config = _base_mapping(table_name="marketing_contacts")
    assert config.table_name == "marketing_contacts"

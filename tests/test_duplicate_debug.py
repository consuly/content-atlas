"""Quick test to debug duplicate detection issue."""
import pytest
from sqlalchemy import text
from app.db.session import get_engine
from app.db.models import insert_records
from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig


def test_simple_duplicate_detection():
    """Test that duplicate detection works with simple records."""
    engine = get_engine()
    
    # Clean up
    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS test_duplicates CASCADE'))
        conn.execute(text('DELETE FROM import_history WHERE table_name = :table_name'), 
                    {"table_name": "test_duplicates"})
    
    # Create import tracking using the proper function
    from app.domain.imports.history import start_import_tracking
    import_id = start_import_tracking(
        source_type="csv",
        file_name="test.csv",
        table_name="test_duplicates"
    )
    
    # Create config
    config = MappingConfig(
        table_name="test_duplicates",
        mappings={
            "name": "name",
            "company": "company"
        },
        db_schema={
            "name": "TEXT",
            "company": "TEXT"
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=True,
            check_file_level=False,
            uniqueness_columns=["name", "company"],
            allow_duplicates=False
        )
    )
    
    # Create the table
    from app.db.models import create_table_if_not_exists
    create_table_if_not_exists(engine, config)
    
    # Insert first batch
    records1 = [
        {"name": "Devon Low", "company": "Kollective"},
        {"name": "Jorge Garcia", "company": "The Design Agency"},
        {"name": "John Doe", "company": "Acme Corp"}
    ]
    
    inserted, skipped = insert_records(
        engine,
        "test_duplicates",
        records1,
        config,
        import_id=import_id,
        has_active_import=True,
        pre_mapped=True
    )
    
    print(f"\nFirst batch: {inserted} inserted, {skipped} skipped")
    assert inserted == 3
    assert skipped == 0
    
    # Insert second batch with 2 duplicates
    records2 = [
        {"name": "Devon Low", "company": "Kollective"},  # Duplicate
        {"name": "Jorge Garcia", "company": "The Design Agency"},  # Duplicate
        {"name": "Jane Smith", "company": "New Corp"}  # New
    ]
    
    inserted, skipped = insert_records(
        engine,
        "test_duplicates",
        records2,
        config,
        import_id=import_id,
        has_active_import=True,
        pre_mapped=True
    )
    
    print(f"Second batch: {inserted} inserted, {skipped} skipped")
    assert skipped == 2, f"Expected 2 duplicates, got {skipped}"
    assert inserted == 1, f"Expected 1 new record, got {inserted}"
    
    # Verify final count
    with engine.connect() as conn:
        count = conn.execute(text('SELECT COUNT(*) FROM test_duplicates')).scalar()
        print(f"Final count: {count}")
        assert count == 4  # 3 from first batch + 1 from second batch

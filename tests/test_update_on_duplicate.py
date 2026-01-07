"""
Tests for update-on-duplicate feature with rollback capability.
"""
import pytest
from app.db.models import insert_records, create_table_if_not_exists
from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig
from app.domain.imports.history import (
    start_import_tracking,
    complete_import_tracking,
    get_import_history
)
from app.domain.imports.rollback import (
    list_row_updates,
    get_row_update_detail,
    rollback_single_update,
    rollback_import_updates
)
from sqlalchemy import text


def test_update_on_duplicate_basic(test_engine):
    """Test basic update-on-duplicate functionality."""
    table_name = "test_customers_update"
    
    # Cleanup any leftover table from previous runs
    with test_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
    
    # Create mapping config with update_on_duplicate enabled
    config = MappingConfig(
        table_name=table_name,
        db_schema={
            "email": "VARCHAR(255)",
            "name": "VARCHAR(255)",
            "phone": "VARCHAR(50)"
        },
        mappings={
            "email": "email",
            "name": "name",
            "phone": "phone"
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=True,
            uniqueness_columns=["email"],
            update_on_duplicate=True
        )
    )
    
    # Start import tracking
    import_id = start_import_tracking(
        source_type="test",
        file_name="test_update.csv",
        table_name=table_name
    )
    
    # Create table
    create_table_if_not_exists(test_engine, config)
    
    # Insert initial records
    initial_records = [
        {"email": "john@example.com", "name": "John Doe", "phone": "555-0001"},
        {"email": "jane@example.com", "name": "Jane Smith", "phone": "555-0002"}
    ]
    
    inserted, skipped = insert_records(
        test_engine,
        table_name,
        initial_records,
        config=config,
        import_id=import_id,
        has_active_import=True
    )
    
    assert inserted == 2
    assert skipped == 0
    
    # Complete first import
    complete_import_tracking(
        import_id=import_id,
        status="success",
        total_rows_in_file=2,
        rows_processed=2,
        rows_inserted=2
    )
    
    # Start second import with duplicate records (updated values)
    import_id_2 = start_import_tracking(
        source_type="test",
        file_name="test_update_2.csv",
        table_name=table_name
    )
    
    updated_records = [
        {"email": "john@example.com", "name": "John Updated", "phone": "555-9999"},
        {"email": "jane@example.com", "name": "Jane Updated", "phone": "555-8888"},
        {"email": "bob@example.com", "name": "Bob New", "phone": "555-7777"}
    ]
    
    inserted, skipped = insert_records(
        test_engine,
        table_name,
        updated_records,
        config=config,
        import_id=import_id_2,
        has_active_import=True
    )
    
    # Should insert 1 new record (Bob) and update 2 existing records
    assert inserted == 1  # Only Bob is new
    assert skipped == 2  # John and Jane were duplicates (but updated)
    
    # Verify updates were recorded
    updates, total_count = list_row_updates(import_id_2, limit=100)
    assert total_count == 2  # Two updates recorded
    assert len(updates) == 2
    
    # Verify the data was actually updated
    with test_engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT name, phone FROM "{table_name}" WHERE email = :email'),
            {"email": "john@example.com"}
        ).fetchone()
        assert result[0] == "John Updated"
        assert result[1] == "555-9999"
    
    # Verify rows_updated counter in import_history
    history = get_import_history(import_id=import_id_2, limit=1)
    assert history[0]["rows_updated"] == 2
    
    # Cleanup
    with test_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))


def test_update_on_duplicate_specific_columns(test_engine):
    """Test updating only specific columns."""
    table_name = "test_customers_partial_update"
    
    config = MappingConfig(
        table_name=table_name,
        db_schema={
            "email": "VARCHAR(255)",
            "name": "VARCHAR(255)",
            "phone": "VARCHAR(50)",
            "address": "VARCHAR(255)"
        },
        mappings={
            "email": "email",
            "name": "name",
            "phone": "phone",
            "address": "address"
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=True,
            uniqueness_columns=["email"],
            update_on_duplicate=True,
            update_columns=["phone"]  # Only update phone
        )
    )
    
    import_id = start_import_tracking(
        source_type="test",
        file_name="test.csv",
        table_name=table_name
    )
    
    create_table_if_not_exists(test_engine, config)
    
    # Insert initial record
    initial_records = [
        {"email": "john@example.com", "name": "John Doe", "phone": "555-0001", "address": "123 Main St"}
    ]
    
    insert_records(
        test_engine,
        table_name,
        initial_records,
        config=config,
        import_id=import_id,
        has_active_import=True
    )
    
    # Second import with updated values
    import_id_2 = start_import_tracking(
        source_type="test",
        file_name="test2.csv",
        table_name=table_name
    )
    
    updated_records = [
        {"email": "john@example.com", "name": "John Changed", "phone": "555-9999", "address": "456 Oak Ave"}
    ]
    
    insert_records(
        test_engine,
        table_name,
        updated_records,
        config=config,
        import_id=import_id_2,
        has_active_import=True
    )
    
    # Verify only phone was updated
    with test_engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT name, phone, address FROM "{table_name}" WHERE email = :email'),
            {"email": "john@example.com"}
        ).fetchone()
        assert result[0] == "John Doe"  # Name unchanged
        assert result[1] == "555-9999"  # Phone updated
        assert result[2] == "123 Main St"  # Address unchanged
    
    # Verify only phone is in updated_columns
    updates, _ = list_row_updates(import_id_2, limit=1)
    assert updates[0]["updated_columns"] == ["phone"]
    
    # Cleanup
    with test_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))


def test_rollback_single_update(test_engine):
    """Test rolling back a single update."""
    table_name = "test_customers_rollback"
    
    config = MappingConfig(
        table_name=table_name,
        db_schema={
            "email": "VARCHAR(255)",
            "name": "VARCHAR(255)"
        },
        mappings={
            "email": "email",
            "name": "name"
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=True,
            uniqueness_columns=["email"],
            update_on_duplicate=True
        )
    )
    
    import_id = start_import_tracking(
        source_type="test",
        file_name="test.csv",
        table_name=table_name
    )
    
    create_table_if_not_exists(test_engine, config)
    
    # Insert and then update
    initial_records = [{"email": "john@example.com", "name": "John Original"}]
    insert_records(test_engine, table_name, initial_records, config=config, import_id=import_id, has_active_import=True)
    
    import_id_2 = start_import_tracking(source_type="test", file_name="test2.csv", table_name=table_name)
    updated_records = [{"email": "john@example.com", "name": "John Updated"}]
    insert_records(test_engine, table_name, updated_records, config=config, import_id=import_id_2, has_active_import=True)
    
    # Get the update ID
    updates, _ = list_row_updates(import_id_2, limit=1)
    update_id = updates[0]["id"]
    
    # Rollback the update
    result = rollback_single_update(
        import_id=import_id_2,
        update_id=update_id,
        rolled_back_by="test_user"
    )
    
    assert result["success"] is True
    assert result["update"]["rolled_back_at"] is not None
    
    # Verify data was restored
    with test_engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT name FROM "{table_name}" WHERE email = :email'),
            {"email": "john@example.com"}
        ).fetchone()
        assert result[0] == "John Original"
    
    # Verify can't rollback again
    with pytest.raises(ValueError, match="already been rolled back"):
        rollback_single_update(import_id=import_id_2, update_id=update_id)
    
    # Cleanup
    with test_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))


def test_rollback_with_conflict_detection(test_engine):
    """Test rollback conflict detection when row has been modified."""
    table_name = "test_customers_conflict"
    
    config = MappingConfig(
        table_name=table_name,
        db_schema={
            "email": "VARCHAR(255)",
            "name": "VARCHAR(255)"
        },
        mappings={
            "email": "email",
            "name": "name"
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=True,
            uniqueness_columns=["email"],
            update_on_duplicate=True
        )
    )
    
    import_id = start_import_tracking(source_type="test", file_name="test.csv", table_name=table_name)
    create_table_if_not_exists(test_engine, config)
    
    # Insert initial record
    initial_records = [{"email": "john@example.com", "name": "John Original"}]
    insert_records(test_engine, table_name, initial_records, config=config, import_id=import_id, has_active_import=True)
    
    # First update
    import_id_2 = start_import_tracking(source_type="test", file_name="test2.csv", table_name=table_name)
    updated_records = [{"email": "john@example.com", "name": "John Updated"}]
    insert_records(test_engine, table_name, updated_records, config=config, import_id=import_id_2, has_active_import=True)
    
    updates, _ = list_row_updates(import_id_2, limit=1)
    update_id = updates[0]["id"]
    row_id = updates[0]["row_id"]
    
    # Manually change the row (simulating external modification)
    with test_engine.begin() as conn:
        conn.execute(
            text(f'UPDATE "{table_name}" SET name = :name WHERE _row_id = :row_id'),
            {"name": "John Modified Externally", "row_id": row_id}
        )
    
    # Try to rollback - should detect conflict
    result = rollback_single_update(
        import_id=import_id_2,
        update_id=update_id,
        force=False
    )
    
    assert result["success"] is False
    assert result["conflict"] is not None
    assert "Conflict detected" in result["message"]
    
    # Force rollback should work
    result = rollback_single_update(
        import_id=import_id_2,
        update_id=update_id,
        force=True
    )
    
    assert result["success"] is True
    assert result["conflict"] is not None  # Conflict still reported but overridden
    
    # Cleanup
    with test_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))


def test_rollback_all_updates(test_engine):
    """Test rolling back all updates from an import."""
    table_name = "test_customers_rollback_all"
    
    config = MappingConfig(
        table_name=table_name,
        db_schema={
            "email": "VARCHAR(255)",
            "name": "VARCHAR(255)"
        },
        mappings={
            "email": "email",
            "name": "name"
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=True,
            uniqueness_columns=["email"],
            update_on_duplicate=True
        )
    )
    
    import_id = start_import_tracking(source_type="test", file_name="test.csv", table_name=table_name)
    create_table_if_not_exists(test_engine, config)
    
    # Insert initial records
    initial_records = [
        {"email": "john@example.com", "name": "John Original"},
        {"email": "jane@example.com", "name": "Jane Original"}
    ]
    insert_records(test_engine, table_name, initial_records, config=config, import_id=import_id, has_active_import=True)
    
    # Update both records
    import_id_2 = start_import_tracking(source_type="test", file_name="test2.csv", table_name=table_name)
    updated_records = [
        {"email": "john@example.com", "name": "John Updated"},
        {"email": "jane@example.com", "name": "Jane Updated"}
    ]
    insert_records(test_engine, table_name, updated_records, config=config, import_id=import_id_2, has_active_import=True)
    
    # Verify 2 updates were recorded
    updates, total_count = list_row_updates(import_id_2)
    assert total_count == 2
    
    # Rollback all
    result = rollback_import_updates(
        import_id=import_id_2,
        rolled_back_by="test_user"
    )
    
    assert result["success"] is True
    assert result["updates_rolled_back"] == 2
    
    # Verify both records were restored
    with test_engine.connect() as conn:
        john = conn.execute(
            text(f'SELECT name FROM "{table_name}" WHERE email = :email'),
            {"email": "john@example.com"}
        ).fetchone()
        assert john[0] == "John Original"
        
        jane = conn.execute(
            text(f'SELECT name FROM "{table_name}" WHERE email = :email'),
            {"email": "jane@example.com"}
        ).fetchone()
        assert jane[0] == "Jane Original"
    
    # Cleanup
    with test_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))


def test_update_detail_includes_before_after(test_engine):
    """Test that update detail includes before and after values."""
    table_name = "test_customers_detail"
    
    config = MappingConfig(
        table_name=table_name,
        db_schema={
            "email": "VARCHAR(255)",
            "name": "VARCHAR(255)",
            "phone": "VARCHAR(50)"
        },
        mappings={
            "email": "email",
            "name": "name",
            "phone": "phone"
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=True,
            uniqueness_columns=["email"],
            update_on_duplicate=True
        )
    )
    
    import_id = start_import_tracking(source_type="test", file_name="test.csv", table_name=table_name)
    create_table_if_not_exists(test_engine, config)
    
    # Insert and update
    initial_records = [{"email": "john@example.com", "name": "John Original", "phone": "555-0001"}]
    insert_records(test_engine, table_name, initial_records, config=config, import_id=import_id, has_active_import=True)
    
    import_id_2 = start_import_tracking(source_type="test", file_name="test2.csv", table_name=table_name)
    updated_records = [{"email": "john@example.com", "name": "John Updated", "phone": "555-9999"}]
    insert_records(test_engine, table_name, updated_records, config=config, import_id=import_id_2, has_active_import=True)
    
    # Get update detail
    updates, _ = list_row_updates(import_id_2, limit=1)
    update_id = updates[0]["id"]
    
    detail = get_row_update_detail(import_id=import_id_2, update_id=update_id)
    
    # Verify before/after values
    assert detail["update"]["previous_values"]["name"] == "John Original"
    assert detail["update"]["previous_values"]["phone"] == "555-0001"
    assert detail["update"]["new_values"]["name"] == "John Updated"
    assert detail["update"]["new_values"]["phone"] == "555-9999"
    assert set(detail["update"]["updated_columns"]) == {"name", "phone"}
    
    # Verify current row is included
    assert detail["current_row"] is not None
    assert detail["current_row"]["name"] == "John Updated"
    
    # Cleanup
    with test_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))


def test_no_update_when_disabled(test_engine):
    """Test that duplicates are skipped when update_on_duplicate is False."""
    table_name = "test_customers_no_update"
    
    config = MappingConfig(
        table_name=table_name,
        db_schema={
            "email": "VARCHAR(255)",
            "name": "VARCHAR(255)"
        },
        mappings={
            "email": "email",
            "name": "name"
        },
        duplicate_check=DuplicateCheckConfig(
            enabled=True,
            uniqueness_columns=["email"],
            update_on_duplicate=False  # Disabled
        )
    )
    
    import_id = start_import_tracking(source_type="test", file_name="test.csv", table_name=table_name)
    create_table_if_not_exists(test_engine, config)
    
    # Insert initial record
    initial_records = [{"email": "john@example.com", "name": "John Original"}]
    insert_records(test_engine, table_name, initial_records, config=config, import_id=import_id, has_active_import=True)
    
    # Try to update (should skip)
    import_id_2 = start_import_tracking(source_type="test", file_name="test2.csv", table_name=table_name)
    updated_records = [{"email": "john@example.com", "name": "John Updated"}]
    
    inserted, skipped = insert_records(
        test_engine,
        table_name,
        updated_records,
        config=config,
        import_id=import_id_2,
        has_active_import=True
    )
    
    assert inserted == 0  # Nothing inserted
    assert skipped == 1  # Duplicate skipped
    
    # Verify no updates recorded
    updates, total_count = list_row_updates(import_id_2)
    assert total_count == 0
    
    # Verify data not changed
    with test_engine.connect() as conn:
        result = conn.execute(
            text(f'SELECT name FROM "{table_name}" WHERE email = :email'),
            {"email": "john@example.com"}
        ).fetchone()
        assert result[0] == "John Original"
    
    # Cleanup
    with test_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

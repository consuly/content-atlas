import os

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("SKIP_DB_INIT", "1")

from app.domain.imports.schema_migrations import (
    apply_schema_migrations,
    SchemaMigrationError,
)


def test_replace_column_creates_new_text_column_and_renames_old():
    engine = create_engine("sqlite:///:memory:")
    table_name = "client_data"

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    budget INTEGER
                )
                '''
            )
        )
        conn.execute(
            text(
                f'''
                INSERT INTO "{table_name}" (id, budget)
                VALUES (1, 100), (2, NULL)
                '''
            )
        )

    migrations = [
        {
            "action": "replace_column",
            "old_column": "budget",
            "new_column": {
                "name": "budget_text",
                "type": "TEXT",
                "copy_data": True,
                "rename_old_column_to": "budget_legacy",
            },
        }
    ]

    results = apply_schema_migrations(engine, table_name, migrations)
    assert results == [
        {
            "action": "replace_column",
            "old_column": "budget",
            "new_column": "budget_text",
            "status": "applied",
        }
    ]

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f'''
                SELECT budget_text, budget_legacy
                FROM "{table_name}"
                ORDER BY id
                '''
            )
        ).fetchall()

    assert rows == [("100", 100), (None, None)]

    # Idempotent re-run should be reported as already applied.
    results_repeat = apply_schema_migrations(engine, table_name, migrations)
    assert results_repeat == [
        {
            "action": "replace_column",
            "old_column": "budget",
            "new_column": "budget_text",
            "status": "already_applied",
        }
    ]


def test_replace_column_legacy_payload_converts_type_and_preserves_name():
    engine = create_engine("sqlite:///:memory:")
    table_name = "client_data"

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    company_linkedin_id INTEGER
                )
                '''
            )
        )
        conn.execute(
            text(
                f'''
                INSERT INTO "{table_name}" (id, company_linkedin_id)
                VALUES (1, 71678219), (2, NULL)
                '''
            )
        )

    migrations = [
        {
            "action": "replace_column",
            "column_name": "company_linkedin_id",
            "new_type": "TEXT",
        }
    ]

    results = apply_schema_migrations(engine, table_name, migrations)
    assert results == [
        {
            "action": "replace_column",
            "old_column": "company_linkedin_id",
            "new_column": "company_linkedin_id",
            "status": "applied",
        }
    ]

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f'''
                SELECT company_linkedin_id, company_linkedin_id_legacy
                FROM "{table_name}"
                ORDER BY id
                '''
            )
        ).fetchall()

    assert rows == [("71678219", 71678219), (None, None)]

    results_repeat = apply_schema_migrations(engine, table_name, migrations)
    assert results_repeat == [
        {
            "action": "replace_column",
            "old_column": "company_linkedin_id",
            "new_column": "company_linkedin_id",
            "status": "already_applied",
        }
    ]


def test_replace_column_final_name_renames_new_column():
    engine = create_engine("sqlite:///:memory:")
    table_name = "client_data"

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    budget INTEGER
                )
                '''
            )
        )
        conn.execute(
            text(
                f'''
                INSERT INTO "{table_name}" (id, budget)
                VALUES (1, 500)
                '''
            )
        )

    migrations = [
        {
            "action": "replace_column",
            "old_column": "budget",
            "new_column": {
                "name": "budget_text",
                "final_name": "budget",
                "type": "TEXT",
                "rename_old_column_to": "budget_numeric",
            },
        }
    ]

    apply_schema_migrations(engine, table_name, migrations)

    with engine.connect() as conn:
        columns = [
            row[1]
            for row in conn.execute(
                text(f'PRAGMA table_info("{table_name}")')
            ).fetchall()
        ]
        rows = conn.execute(
            text(
                f'''
                SELECT budget, budget_numeric
                FROM "{table_name}"
                '''
            )
        ).fetchall()

    assert "budget_text" not in columns
    assert "budget" in columns
    assert rows == [("500", 500)]
def test_add_column_creates_new_column_and_populates_from_existing():
    engine = create_engine("sqlite:///:memory:")
    table_name = "client_data"

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    budget INTEGER
                )
                '''
            )
        )
        conn.execute(
            text(
                f'''
                INSERT INTO "{table_name}" (id, budget)
                VALUES (1, 100), (2, NULL)
                '''
            )
        )

    migrations = [
        {
            "action": "add_column",
            "new_column": {
                "name": "budget_text",
                "type": "TEXT",
                "copy_from": "budget",
            },
        }
    ]

    results = apply_schema_migrations(engine, table_name, migrations)
    assert results == [
        {
            "action": "add_column",
            "new_column": "budget_text",
            "status": "applied",
        }
    ]

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f'''
                SELECT budget, budget_text
                FROM "{table_name}"
                ORDER BY id
                '''
            )
        ).fetchall()

    assert rows == [(100, "100"), (None, None)]

    # Idempotent re-run should be reported as already applied.
    results_repeat = apply_schema_migrations(engine, table_name, migrations)
    assert results_repeat == [
        {
            "action": "add_column",
            "new_column": "budget_text",
            "status": "already_applied",
        }
    ]


def test_schema_migration_missing_table_raises():
    engine = create_engine("sqlite:///:memory:")
    migrations = [
        {
            "action": "replace_column",
            "old_column": "missing",
            "new_column": {"name": "missing_text", "type": "TEXT"},
        }
    ]

    with pytest.raises(SchemaMigrationError):
        apply_schema_migrations(engine, "does_not_exist", migrations)


def test_add_column_missing_name_raises():
    engine = create_engine("sqlite:///:memory:")
    table_name = "client_data"

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY
                )
                '''
            )
        )

    migrations = [
        {
            "action": "add_column",
            "new_column": {"type": "TEXT"},
        }
    ]

    with pytest.raises(SchemaMigrationError):
        apply_schema_migrations(engine, table_name, migrations)


def test_rename_column_changes_name_and_is_idempotent():
    engine = create_engine("sqlite:///:memory:")
    table_name = "client_data"

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    budget_text TEXT
                )
                '''
            )
        )

    migrations = [
        {
            "action": "rename_column",
            "old_column": "budget_text",
            "new_name": "budget_notes",
        }
    ]

    results = apply_schema_migrations(engine, table_name, migrations)
    assert results == [
        {
            "action": "rename_column",
            "old_column": "budget_text",
            "new_name": "budget_notes",
            "status": "applied",
        }
    ]

    with engine.connect() as conn:
        columns = [
            row[1]
            for row in conn.execute(
                text(f'PRAGMA table_info("{table_name}")')
            ).fetchall()
        ]

    assert "budget_text" not in columns
    assert "budget_notes" in columns

    results_repeat = apply_schema_migrations(engine, table_name, migrations)
    assert results_repeat == [
        {
            "action": "rename_column",
            "old_column": "budget_text",
            "new_name": "budget_notes",
            "status": "already_applied",
        }
    ]


def test_rename_column_missing_destination_raises():
    engine = create_engine("sqlite:///:memory:")
    table_name = "client_data"

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
                '''
            )
        )

    migrations = [
        {
            "action": "rename_column",
            "old_column": "name",
        }
    ]

    with pytest.raises(SchemaMigrationError):
        apply_schema_migrations(engine, table_name, migrations)


def test_drop_column_removes_column_and_is_idempotent():
    engine = create_engine("sqlite:///:memory:")
    table_name = "client_data"

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY,
                    legacy_field TEXT
                )
                '''
            )
        )

    migrations = [
        {
            "action": "drop_column",
            "column": "legacy_field",
        }
    ]

    results = apply_schema_migrations(engine, table_name, migrations)
    assert results == [
        {
            "action": "drop_column",
            "column": "legacy_field",
            "status": "applied",
        }
    ]

    with engine.connect() as conn:
        columns = [
            row[1]
            for row in conn.execute(
                text(f'PRAGMA table_info("{table_name}")')
            ).fetchall()
        ]

    assert "legacy_field" not in columns

    results_repeat = apply_schema_migrations(engine, table_name, migrations)
    assert results_repeat == [
        {
            "action": "drop_column",
            "column": "legacy_field",
            "status": "already_applied",
        }
    ]


def test_drop_column_missing_name_raises():
    engine = create_engine("sqlite:///:memory:")
    table_name = "client_data"

    with engine.begin() as conn:
        conn.execute(
            text(
                f'''
                CREATE TABLE "{table_name}" (
                    id INTEGER PRIMARY KEY
                )
                '''
            )
        )

    migrations = [
        {
            "action": "drop_column",
        }
    ]

    with pytest.raises(SchemaMigrationError):
        apply_schema_migrations(engine, table_name, migrations)

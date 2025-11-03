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


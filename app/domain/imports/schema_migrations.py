"""
Schema migration helpers invoked by LLM-driven import flows.

Currently supports column replacement operations where the assistant requests
that an existing column be superseded by a new column with a different type.
"""

from __future__ import annotations

from typing import Any, Dict, List

import logging
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class SchemaMigrationError(Exception):
    """Raised when a schema migration request cannot be applied."""


def _quote(identifier: str) -> str:
    """Return a double-quoted identifier for safe SQL usage."""
    return f'"{identifier}"'


def _default_using_expression(old_column: str, new_type: str) -> str:
    """Build a generic USING expression suitable for multiple SQL dialects."""
    quoted_old = _quote(old_column)
    new_type_upper = new_type.upper()
    return f"CAST({quoted_old} AS {new_type_upper})"


def _apply_replace_column(
    conn,
    table_name: str,
    migration: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Apply a single replace_column migration.

    Expected payload:
    {
        "action": "replace_column",
        "old_column": "amount",
        "new_column": {
            "name": "amount_text",
            "type": "TEXT",
            "copy_data": true,
            "rename_old_column_to": "amount_legacy",
            "drop_old_column": false,
            "using_expression": "CAST(\"amount\" AS TEXT)"
        }
    }
    """
    old_column = migration.get("old_column")
    new_column_spec = migration.get("new_column") or {}
    new_column_name = new_column_spec.get("name")
    new_column_type = new_column_spec.get("type")

    if not old_column or not new_column_name or not new_column_type:
        raise SchemaMigrationError(
            "replace_column migration requires 'old_column' and "
            "'new_column' with 'name' and 'type'"
        )

    rename_old_to = new_column_spec.get(
        "rename_old_column_to", f"{old_column}_legacy"
    )
    drop_old_column = new_column_spec.get("drop_old_column", False)
    copy_data = new_column_spec.get("copy_data", True)
    using_expression = new_column_spec.get("using_expression")

    inspector = inspect(conn)
    existing_columns = {
        col["name"]: col for col in inspector.get_columns(table_name)
    }

    result: Dict[str, Any] = {
        "action": "replace_column",
        "old_column": old_column,
        "new_column": new_column_name,
        "status": "pending",
    }

    old_exists = old_column in existing_columns
    new_exists = new_column_name in existing_columns
    legacy_exists = rename_old_to in existing_columns

    if new_exists and (not old_exists or legacy_exists):
        # Migration appears to have run already.
        result["status"] = "already_applied"
        return result

    if not old_exists:
        raise SchemaMigrationError(
            f"Cannot replace column '{old_column}' in table '{table_name}': "
            "column does not exist"
        )

    if not new_exists:
        add_sql = (
            f'ALTER TABLE {_quote(table_name)} '
            f'ADD COLUMN {_quote(new_column_name)} {new_column_type}'
        )
        logger.info(
            "Schema migration: adding column %s to %s as %s",
            new_column_name,
            table_name,
            new_column_type,
        )
        conn.execute(text(add_sql))

    if copy_data:
        expr = using_expression or _default_using_expression(
            old_column, new_column_type
        )
        update_sql = (
            f'UPDATE {_quote(table_name)} '
            f'SET {_quote(new_column_name)} = '
            f'CASE WHEN {_quote(new_column_name)} IS NULL THEN {expr} '
            f'ELSE {_quote(new_column_name)} END'
        )
        logger.info(
            "Schema migration: copying data from %s to %s on table %s",
            old_column,
            new_column_name,
            table_name,
        )
        conn.execute(text(update_sql))

    if drop_old_column:
        drop_sql = (
            f'ALTER TABLE {_quote(table_name)} '
            f'DROP COLUMN {_quote(old_column)}'
        )
        logger.info(
            "Schema migration: dropping old column %s from table %s",
            old_column,
            table_name,
        )
        conn.execute(text(drop_sql))
    else:
        # Rename legacy column to keep data but make room for new mapping.
        if rename_old_to and rename_old_to != old_column:
            if rename_old_to in existing_columns:
                logger.info(
                    "Schema migration: legacy column name %s already exists, "
                    "skipping rename of %s on table %s",
                    rename_old_to,
                    old_column,
                    table_name,
                )
            else:
                rename_sql = (
                    f'ALTER TABLE {_quote(table_name)} '
                    f'RENAME COLUMN {_quote(old_column)} '
                    f'TO {_quote(rename_old_to)}'
                )
                logger.info(
                    "Schema migration: renaming %s to %s on table %s",
                    old_column,
                    rename_old_to,
                    table_name,
                )
                conn.execute(text(rename_sql))

    result["status"] = "applied"
    return result


def apply_schema_migrations(
    engine: Engine,
    table_name: str,
    migrations: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Apply a sequence of schema migrations against the specified table.

    Currently supports the ``replace_column`` action. The function is designed
    to be idempotent – rerunning the same migration list should not raise
    errors once the desired state has been achieved.
    """
    if not migrations:
        return []

    logger.info(
        "Applying %d schema migration(s) to table '%s'",
        len(migrations),
        table_name,
    )

    results: List[Dict[str, Any]] = []
    with engine.begin() as conn:
        inspector = inspect(conn)
        if not inspector.has_table(table_name):
            raise SchemaMigrationError(
                f"Cannot apply migrations: table '{table_name}' does not exist"
            )

        for migration in migrations:
            action = (migration or {}).get("action")
            if action == "replace_column":
                result = _apply_replace_column(conn, table_name, migration)
                results.append(result)
            else:
                raise SchemaMigrationError(
                    f"Unsupported schema migration action: {action}"
                )

    return results


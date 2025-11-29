"""
Schema migration helpers invoked by LLM-driven import flows.

Currently supports column replacement, addition, rename, and drop operations
requested by the assistant to resolve schema mismatches discovered during
imports.
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


def _normalize_replace_column_payload(
    migration: Dict[str, Any]
) -> Dict[str, Any]:
    """Support legacy replace_column payloads by expanding shorthand keys."""

    normalized = {**migration}
    new_column_spec = dict(normalized.get("new_column") or {})

    column_alias = normalized.get("column_name")
    final_target_name = normalized.get("final_column_name")

    if column_alias and "old_column" not in normalized:
        normalized["old_column"] = column_alias

        if "name" not in new_column_spec:
            preferred_suffix = (
                "_text"
                if str(normalized.get("new_type", ""))
                .upper()
                .startswith("TEXT")
                else "_replacement"
            )
            new_column_spec["name"] = (
                normalized.get("new_column_name")
                or f"{column_alias}{preferred_suffix}"
            )

        if "type" not in new_column_spec:
            new_type = normalized.get("new_type")
            if not new_type:
                raise SchemaMigrationError(
                    "replace_column legacy payload requires 'new_type'"
                )
            new_column_spec["type"] = new_type

        if "rename_old_column_to" not in new_column_spec:
            new_column_spec["rename_old_column_to"] = (
                normalized.get("rename_old_column_to")
                or f"{column_alias}_legacy"
            )

        if "drop_old_column" not in new_column_spec:
            new_column_spec["drop_old_column"] = normalized.get(
                "drop_old_column", False
            )

        if "copy_data" not in new_column_spec:
            new_column_spec["copy_data"] = normalized.get("copy_data", True)

        if (
            "using_expression" not in new_column_spec
            and normalized.get("using_expression")
        ):
            new_column_spec["using_expression"] = normalized["using_expression"]

        if final_target_name is None:
            final_target_name = column_alias

    if final_target_name is not None and "final_name" not in new_column_spec:
        new_column_spec["final_name"] = final_target_name

    normalized["new_column"] = new_column_spec
    return normalized


def _safe_numeric_cast_expression(
    old_column: str, new_type: str, dialect_name: str
) -> str:
    """
    Build a safe numeric cast that avoids blowing up on non-numeric strings.

    Postgres gets a regex guard, SQLite falls back to a glob-based check. Both
    paths return NULL instead of raising when the source value cannot be cast.
    """
    value_as_text = f"TRIM(CAST({_quote(old_column)} AS TEXT))"

    if dialect_name == "postgresql":
        return (
            f"CASE WHEN {value_as_text} ~ '^[+-]?[0-9]+(\\.[0-9]+)?$' "
            f"THEN CAST({value_as_text} AS {new_type}) ELSE NULL END"
        )

    if dialect_name == "sqlite":
        return (
            f"CASE WHEN {value_as_text} GLOB '*[^0-9.+-]*' "
            f"OR TRIM({value_as_text}) = '' "
            f"THEN NULL ELSE CAST({value_as_text} AS {new_type}) END"
        )

    # Default to a simple cast for other dialects.
    return f"CAST({value_as_text} AS {new_type})"


def _default_using_expression(
    old_column: str, new_type: str, dialect_name: str | None = None
) -> str:
    """Build a dialect-aware USING expression that tolerates bad data."""
    new_type_upper = new_type.upper()
    dialect = (dialect_name or "").lower()

    if new_type_upper.startswith(
        ("DECIMAL", "NUMERIC", "INT", "SMALLINT", "BIGINT")
    ):
        return _safe_numeric_cast_expression(
            old_column, new_type_upper, dialect
        )

    quoted_old = _quote(old_column)
    return f"CAST({quoted_old} AS {new_type_upper})"


def _apply_add_column(
    conn,
    table_name: str,
    migration: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Apply a single add_column migration.

    Expected payload:
    {
        "action": "add_column",
        "new_column": {
            "name": "budget_text",
            "type": "TEXT",
            "nullable": true,
            "default": null,
            "copy_from": "budget",
            "copy_data": true,
            "using_expression": "CAST(\"budget\" AS TEXT)"
        }
    }
    """
    new_column_spec = migration.get("new_column") or {}
    column_name = new_column_spec.get("name")
    column_type = new_column_spec.get("type")
    dialect_name = conn.engine.dialect.name

    if not column_name or not column_type:
        raise SchemaMigrationError(
            "add_column migration requires 'new_column' with 'name' and 'type'"
        )

    nullable = new_column_spec.get("nullable", True)
    default_value = new_column_spec.get("default")
    copy_from = new_column_spec.get("copy_from")
    using_expression = new_column_spec.get("using_expression")
    copy_data = new_column_spec.get(
        "copy_data", bool(copy_from) or bool(using_expression)
    )

    inspector = inspect(conn)
    existing_columns = {
        col["name"]: col for col in inspector.get_columns(table_name)
    }

    result: Dict[str, Any] = {
        "action": "add_column",
        "new_column": column_name,
        "status": "pending",
    }

    if column_name in existing_columns:
        result["status"] = "already_applied"
        return result

    clauses = [column_type]
    if default_value is not None:
        clauses.append(f"DEFAULT {default_value}")
    if not nullable:
        clauses.append("NOT NULL")

    add_sql = (
        f'ALTER TABLE {_quote(table_name)} '
        f'ADD COLUMN {_quote(column_name)} {" ".join(clauses)}'
    )
    logger.info(
        "Schema migration: adding column %s to %s as %s",
        column_name,
        table_name,
        column_type,
    )
    conn.execute(text(add_sql))

    if copy_data:
        if using_expression:
            expression = using_expression
        elif copy_from:
            expression = _default_using_expression(
                copy_from, column_type, dialect_name
            )
        else:
            raise SchemaMigrationError(
                "add_column migration set 'copy_data' but did not provide "
                "'copy_from' or 'using_expression'"
            )

        update_sql = (
            f'UPDATE {_quote(table_name)} '
            f'SET {_quote(column_name)} = {expression} '
            f'WHERE {_quote(column_name)} IS NULL'
        )
        logger.info(
            "Schema migration: populating %s on table %s using expression %s",
            column_name,
            table_name,
            expression,
        )
        conn.execute(text(update_sql))

    result["status"] = "applied"
    return result


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
            "final_name": "amount",  # optional rename after swap
            "type": "TEXT",
            "copy_data": true,
            "rename_old_column_to": "amount_legacy",
            "drop_old_column": false,
            "using_expression": "CAST(\"amount\" AS TEXT)"
        }
    }

    Legacy payloads such as
    {"action": "replace_column", "column_name": "amount", "new_type": "TEXT"}
    are normalized automatically into the richer structure above.
    """
    migration = _normalize_replace_column_payload(migration)
    original_old_column = migration.get("old_column")
    new_column_spec = migration.get("new_column") or {}
    temp_new_column_name = new_column_spec.get("name")
    final_new_column_name = (
        new_column_spec.get("final_name") or temp_new_column_name
    )
    new_column_type = new_column_spec.get("type")
    dialect_name = conn.engine.dialect.name

    if not original_old_column or not temp_new_column_name or not new_column_type:
        raise SchemaMigrationError(
            "replace_column migration requires 'old_column' and "
            "'new_column' with 'name' and 'type'"
        )

    rename_old_to = new_column_spec.get(
        "rename_old_column_to", f"{original_old_column}_legacy"
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
        "old_column": original_old_column,
        "new_column": final_new_column_name,
        "status": "pending",
    }

    old_exists = original_old_column in existing_columns
    temp_new_exists = temp_new_column_name in existing_columns
    final_new_exists = (
        final_new_column_name in existing_columns
        if final_new_column_name
        else False
    )
    legacy_exists = rename_old_to in existing_columns
    already_applied = (temp_new_exists or final_new_exists) and (
        not old_exists or legacy_exists
    )

    if already_applied:
        # Migration appears to have run already.
        result["status"] = "already_applied"
        return result

    if not old_exists:
        raise SchemaMigrationError(
            f"Cannot replace column '{original_old_column}' in table '{table_name}': "
            "column does not exist"
        )

    if not temp_new_exists:
        add_sql = (
            f'ALTER TABLE {_quote(table_name)} '
            f'ADD COLUMN {_quote(temp_new_column_name)} {new_column_type}'
        )
        logger.info(
            "Schema migration: adding column %s to %s as %s",
            temp_new_column_name,
            table_name,
            new_column_type,
        )
        conn.execute(text(add_sql))

    if copy_data:
        expr = using_expression or _default_using_expression(
            original_old_column, new_column_type, dialect_name
        )
        update_sql = (
            f'UPDATE {_quote(table_name)} '
            f'SET {_quote(temp_new_column_name)} = '
            f'CASE WHEN {_quote(temp_new_column_name)} IS NULL THEN {expr} '
            f'ELSE {_quote(temp_new_column_name)} END'
        )
        logger.info(
            "Schema migration: copying data from %s to %s on table %s",
            original_old_column,
            temp_new_column_name,
            table_name,
        )
        conn.execute(text(update_sql))

    if drop_old_column:
        drop_sql = (
            f'ALTER TABLE {_quote(table_name)} '
            f'DROP COLUMN {_quote(original_old_column)}'
        )
        logger.info(
            "Schema migration: dropping old column %s from table %s",
            original_old_column,
            table_name,
        )
        conn.execute(text(drop_sql))
    else:
        # Rename legacy column to keep data but make room for new mapping.
        if rename_old_to and rename_old_to != original_old_column:
            if rename_old_to in existing_columns:
                logger.info(
                    "Schema migration: legacy column name %s already exists, "
                    "skipping rename of %s on table %s",
                    rename_old_to,
                    original_old_column,
                    table_name,
                )
            else:
                rename_sql = (
                    f'ALTER TABLE {_quote(table_name)} '
                    f'RENAME COLUMN {_quote(original_old_column)} '
                    f'TO {_quote(rename_old_to)}'
                )
                logger.info(
                    "Schema migration: renaming %s to %s on table %s",
                    original_old_column,
                    rename_old_to,
                    table_name,
                )
                conn.execute(text(rename_sql))

    if final_new_column_name and final_new_column_name != temp_new_column_name:
        inspector = inspect(conn)
        current_columns = {
            col["name"]: col for col in inspector.get_columns(table_name)
        }
        if final_new_column_name in current_columns:
            raise SchemaMigrationError(
                f"Cannot rename column '{temp_new_column_name}' to "
                f"'{final_new_column_name}' in table '{table_name}': "
                "destination column already exists"
            )

        rename_sql = (
            f'ALTER TABLE {_quote(table_name)} '
            f'RENAME COLUMN {_quote(temp_new_column_name)} '
            f'TO {_quote(final_new_column_name)}'
        )
        logger.info(
            "Schema migration: renaming %s to %s on table %s",
            temp_new_column_name,
            final_new_column_name,
            table_name,
        )
        conn.execute(text(rename_sql))

    result["status"] = "applied"
    return result


def _apply_rename_column(
    conn,
    table_name: str,
    migration: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Apply a single rename_column migration.

    Expected payload:
    {
        "action": "rename_column",
        "old_column": "budget_text",
        "new_name": "budget_notes"
    }
    """
    old_column = migration.get("old_column")
    new_name = migration.get("new_name")

    if not old_column or not new_name:
        raise SchemaMigrationError(
            "rename_column migration requires 'old_column' and 'new_name'"
        )

    inspector = inspect(conn)
    existing_columns = {
        col["name"]: col for col in inspector.get_columns(table_name)
    }

    result: Dict[str, Any] = {
        "action": "rename_column",
        "old_column": old_column,
        "new_name": new_name,
        "status": "pending",
    }

    old_exists = old_column in existing_columns
    new_exists = new_name in existing_columns

    if new_exists and not old_exists:
        result["status"] = "already_applied"
        return result

    if not old_exists:
        raise SchemaMigrationError(
            f"Cannot rename column '{old_column}' in table '{table_name}': "
            "column does not exist"
        )

    if new_exists:
        raise SchemaMigrationError(
            f"Cannot rename column '{old_column}' to '{new_name}' in table "
            f"'{table_name}': destination column already exists"
        )

    rename_sql = (
        f'ALTER TABLE {_quote(table_name)} '
        f'RENAME COLUMN {_quote(old_column)} TO {_quote(new_name)}'
    )
    logger.info(
        "Schema migration: renaming %s to %s on table %s",
        old_column,
        new_name,
        table_name,
    )
    conn.execute(text(rename_sql))

    result["status"] = "applied"
    return result


def _apply_drop_column(
    conn,
    table_name: str,
    migration: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Apply a single drop_column migration.

    Expected payload:
    {
        "action": "drop_column",
        "column": "budget_legacy"
    }
    """
    column_name = migration.get("column")
    if not column_name:
        raise SchemaMigrationError(
            "drop_column migration requires 'column'"
        )

    inspector = inspect(conn)
    existing_columns = {
        col["name"]: col for col in inspector.get_columns(table_name)
    }

    result: Dict[str, Any] = {
        "action": "drop_column",
        "column": column_name,
        "status": "pending",
    }

    if column_name not in existing_columns:
        result["status"] = "already_applied"
        return result

    drop_sql = (
        f'ALTER TABLE {_quote(table_name)} '
        f'DROP COLUMN {_quote(column_name)}'
    )
    logger.info(
        "Schema migration: dropping column %s from table %s",
        column_name,
        table_name,
    )
    conn.execute(text(drop_sql))

    result["status"] = "applied"
    return result


def apply_schema_migrations(
    engine: Engine,
    table_name: str,
    migrations: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Apply a sequence of schema migrations against the specified table.

    Currently supports ``replace_column``, ``add_column``, ``rename_column``,
    and ``drop_column`` actions. The function is designed to be idempotent –
    rerunning the same migration list should not raise errors once the desired
    state has been achieved.
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
            elif action == "add_column":
                result = _apply_add_column(conn, table_name, migration)
                results.append(result)
            elif action == "rename_column":
                result = _apply_rename_column(conn, table_name, migration)
                results.append(result)
            elif action == "drop_column":
                result = _apply_drop_column(conn, table_name, migration)
                results.append(result)
            else:
                raise SchemaMigrationError(
                    f"Unsupported schema migration action: {action}"
                )

    return results

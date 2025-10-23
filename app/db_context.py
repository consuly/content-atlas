from typing import Dict, List, Any, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine
from .database import get_engine


def get_database_schema() -> Dict[str, Any]:
    """Get comprehensive database schema information for all user tables."""
    engine = get_engine()

    with engine.connect() as conn:
        # Get all user tables (excluding system tables)
        tables_result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name NOT IN ('spatial_ref_sys', 'geography_columns', 'geometry_columns',
                                 'raster_columns', 'raster_overviews')
            AND table_name NOT LIKE 'pg_%'
            ORDER BY table_name
        """))

        tables = [row[0] for row in tables_result]

        schema_info = {
            "tables": {},
            "relationships": []
        }

        for table_name in tables:
            # Get column information
            columns_result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name != 'id'  -- Exclude auto-generated id column
                ORDER BY ordinal_position
            """), {"table_name": table_name})

            columns = []
            for row in columns_result:
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2].upper() == 'YES',
                    "default": row[3]
                })

            # Get sample data (first 3 rows for context)
            try:
                sample_result = conn.execute(text(f"""
                    SELECT * FROM "{table_name}" LIMIT 3
                """))
                sample_data = [dict(zip(sample_result.keys(), row)) for row in sample_result]
            except Exception:
                sample_data = []

            # Get row count
            count_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            row_count = count_result.scalar()

            schema_info["tables"][table_name] = {
                "columns": columns,
                "sample_data": sample_data,
                "row_count": row_count
            }

        # Get foreign key relationships
        fk_result = conn.execute(text("""
            SELECT
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
        """))

        for row in fk_result:
            schema_info["relationships"].append({
                "table": row[0],
                "column": row[1],
                "references_table": row[2],
                "references_column": row[3]
            })

    return schema_info


def format_schema_for_prompt(schema_info: Dict[str, Any]) -> str:
    """Format database schema information into a readable prompt context."""
    lines = ["## Database Schema Overview\n"]

    lines.append(f"Total Tables: {len(schema_info['tables'])}\n")

    for table_name, table_info in schema_info["tables"].items():
        lines.append(f"### Table: {table_name}")
        lines.append(f"- Rows: {table_info['row_count']}")
        lines.append("- Columns:")

        for col in table_info["columns"]:
            nullable = "(nullable)" if col["nullable"] else "(required)"
            lines.append(f"  - {col['name']}: {col['type']} {nullable}")

        if table_info["sample_data"]:
            lines.append("- Sample Data:")
            for sample in table_info["sample_data"]:
                sample_str = ", ".join([f"{k}: {v}" for k, v in sample.items()])
                lines.append(f"  - {sample_str}")

        lines.append("")

    if schema_info["relationships"]:
        lines.append("### Relationships:")
        for rel in schema_info["relationships"]:
            lines.append(f"- {rel['table']}.{rel['column']} → {rel['references_table']}.{rel['references_column']}")
        lines.append("")

    return "\n".join(lines)


def get_related_tables(query: str, schema_info: Dict[str, Any]) -> List[str]:
    """Analyze query and suggest potentially related tables for JOINs."""
    query_lower = query.lower()
    related_tables = []

    # Simple keyword matching - could be enhanced with LLM analysis
    for table_name, table_info in schema_info["tables"].items():
        # Check if table name or column names appear in query
        if table_name.lower() in query_lower:
            related_tables.append(table_name)
            continue

        for col in table_info["columns"]:
            if col["name"].lower() in query_lower:
                related_tables.append(table_name)
                break

    return list(set(related_tables))  # Remove duplicates

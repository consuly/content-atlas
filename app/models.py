from sqlalchemy import text, MetaData
from sqlalchemy.engine import Engine
from typing import List, Dict, Any
from .schemas import MappingConfig


def create_table_if_not_exists(engine: Engine, config: MappingConfig):
    """Create table based on schema if it doesn't exist."""
    table_name = config.table_name
    columns = []

    for col_name, col_type in config.db_schema.items():
        columns.append(f'"{col_name}" {col_type}')

    columns_sql = ', '.join(columns)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        id SERIAL PRIMARY KEY,
        {columns_sql}
    );
    """

    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()


def insert_records(engine: Engine, table_name: str, records: List[Dict[str, Any]]):
    """Insert records into the table."""
    if not records:
        return 0

    columns = list(records[0].keys())
    placeholders = ', '.join([f':{col}' for col in columns])
    columns_sql = ', '.join([f'"{col}"' for col in columns])

    insert_sql = f"""
    INSERT INTO "{table_name}" ({columns_sql})
    VALUES ({placeholders});
    """

    with engine.connect() as conn:
        for record in records:
            # Clean None values or handle them
            clean_record = {k: v for k, v in record.items() if v is not None}
            conn.execute(text(insert_sql), clean_record)
        conn.commit()

    return len(records)

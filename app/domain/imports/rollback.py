"""
Rollback functionality for row updates made during imports.

This module provides the ability to:
- List all row updates from an import
- View detailed information about specific updates
- Rollback individual updates or all updates from an import
- Detect conflicts when current values differ from expected values
"""

import hashlib
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy import text
from datetime import datetime, timezone

from app.db.session import get_engine
from app.utils.serialization import _make_json_safe

logger = logging.getLogger(__name__)


def compute_row_hash(row_values: Dict[str, Any]) -> str:
    """
    Compute SHA-256 hash of row values for conflict detection.
    
    Args:
        row_values: Dictionary of column name -> value pairs
        
    Returns:
        SHA-256 hash hex string
    """
    # Sort keys for consistent hashing
    sorted_items = sorted(row_values.items())
    # Create a stable JSON representation
    json_str = json.dumps(sorted_items, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode()).hexdigest()


def record_row_update(
    import_id: str,
    table_name: str,
    row_id: int,
    previous_values: Dict[str, Any],
    new_values: Dict[str, Any],
    updated_columns: List[str]
) -> int:
    """
    Record a row update in the row_updates table.
    
    Args:
        import_id: UUID of the import
        table_name: Name of the table
        row_id: ID of the row that was updated
        previous_values: Values before the update
        new_values: Values after the update
        updated_columns: List of columns that were updated
        
    Returns:
        ID of the created row_updates record
    """
    engine = get_engine()
    
    # Compute hash of new values for conflict detection
    current_values_hash = compute_row_hash(new_values)
    
    # Make values JSON-safe
    safe_previous = _make_json_safe(previous_values)
    safe_new = _make_json_safe(new_values)
    
    insert_sql = text("""
        INSERT INTO row_updates (
            import_id, table_name, row_id,
            previous_values, new_values, updated_columns,
            current_values_hash
        ) VALUES (
            :import_id, :table_name, :row_id,
            :previous_values, :new_values, :updated_columns,
            :current_values_hash
        )
        RETURNING id
    """)
    
    try:
        with engine.begin() as conn:
            result = conn.execute(insert_sql, {
                "import_id": import_id,
                "table_name": table_name,
                "row_id": row_id,
                "previous_values": json.dumps(safe_previous),
                "new_values": json.dumps(safe_new),
                "updated_columns": updated_columns,
                "current_values_hash": current_values_hash
            })
            update_id = result.scalar()
            
        logger.info(f"Recorded row update {update_id} for row {row_id} in table '{table_name}'")
        return update_id
        
    except Exception as e:
        logger.error(f"Error recording row update: {str(e)}")
        raise


def list_row_updates(
    import_id: str,
    limit: int = 100,
    offset: int = 0,
    include_rolled_back: bool = False
) -> Tuple[List[Dict[str, Any]], int]:
    """
    List all row updates for an import.
    
    Args:
        import_id: UUID of the import
        limit: Maximum number of updates to return
        offset: Number of updates to skip
        include_rolled_back: If True, include already rolled back updates
        
    Returns:
        Tuple of (updates_list, total_count)
    """
    engine = get_engine()
    
    where_clause = "import_id = :import_id"
    if not include_rolled_back:
        where_clause += " AND rolled_back_at IS NULL"
    
    count_sql = f"""
        SELECT COUNT(*) FROM row_updates
        WHERE {where_clause}
    """
    
    query_sql = f"""
        SELECT 
            id, import_id, table_name, row_id,
            previous_values, new_values, updated_columns,
            current_values_hash, updated_at,
            rolled_back_at, rolled_back_by,
            rollback_conflict, rollback_conflict_details
        FROM row_updates
        WHERE {where_clause}
        ORDER BY updated_at DESC
        LIMIT :limit OFFSET :offset
    """
    
    params = {
        "import_id": import_id,
        "limit": limit,
        "offset": offset
    }
    
    updates = []
    total_count = 0
    
    try:
        with engine.connect() as conn:
            # Get total count
            total_count = conn.execute(text(count_sql), params).scalar() or 0
            
            # Get updates
            results = conn.execute(text(query_sql), params)
            
            for row in results:
                # Parse JSON fields
                previous_values = row[4]
                if isinstance(previous_values, str):
                    previous_values = json.loads(previous_values)
                previous_values = _make_json_safe(previous_values)
                
                new_values = row[5]
                if isinstance(new_values, str):
                    new_values = json.loads(new_values)
                new_values = _make_json_safe(new_values)
                
                rollback_conflict_details = row[12]
                if isinstance(rollback_conflict_details, str):
                    rollback_conflict_details = json.loads(rollback_conflict_details)
                if rollback_conflict_details:
                    rollback_conflict_details = _make_json_safe(rollback_conflict_details)
                
                updates.append({
                    "id": row[0],
                    "import_id": str(row[1]),
                    "table_name": row[2],
                    "row_id": row[3],
                    "previous_values": previous_values,
                    "new_values": new_values,
                    "updated_columns": row[6],
                    "current_values_hash": row[7],
                    "updated_at": row[8].isoformat() if row[8] else None,
                    "rolled_back_at": row[9].isoformat() if row[9] else None,
                    "rolled_back_by": row[10],
                    "has_conflict": row[11],
                    "rollback_conflict_details": rollback_conflict_details
                })
        
        return updates, total_count
        
    except Exception as e:
        logger.error(f"Error listing row updates: {str(e)}")
        raise


def get_row_update_detail(
    import_id: str,
    update_id: int
) -> Dict[str, Any]:
    """
    Get detailed information about a specific row update.
    
    Args:
        import_id: UUID of the import
        update_id: ID of the row update
        
    Returns:
        Dictionary with update details and current row values
    """
    engine = get_engine()
    
    query_sql = text("""
        SELECT 
            id, import_id, table_name, row_id,
            previous_values, new_values, updated_columns,
            current_values_hash, updated_at,
            rolled_back_at, rolled_back_by,
            rollback_conflict, rollback_conflict_details
        FROM row_updates
        WHERE import_id = :import_id AND id = :update_id
    """)
    
    try:
        with engine.connect() as conn:
            row = conn.execute(query_sql, {
                "import_id": import_id,
                "update_id": update_id
            }).fetchone()
            
            if not row:
                raise ValueError(f"Row update {update_id} not found for import {import_id}")
            
            # Parse JSON fields
            previous_values = row[4]
            if isinstance(previous_values, str):
                previous_values = json.loads(previous_values)
            previous_values = _make_json_safe(previous_values)
            
            new_values = row[5]
            if isinstance(new_values, str):
                new_values = json.loads(new_values)
            new_values = _make_json_safe(new_values)
            
            rollback_conflict_details = row[12]
            if isinstance(rollback_conflict_details, str):
                rollback_conflict_details = json.loads(rollback_conflict_details)
            if rollback_conflict_details:
                rollback_conflict_details = _make_json_safe(rollback_conflict_details)
            
            update_info = {
                "id": row[0],
                "import_id": str(row[1]),
                "table_name": row[2],
                "row_id": row[3],
                "previous_values": previous_values,
                "new_values": new_values,
                "updated_columns": row[6],
                "current_values_hash": row[7],
                "updated_at": row[8].isoformat() if row[8] else None,
                "rolled_back_at": row[9].isoformat() if row[9] else None,
                "rolled_back_by": row[10],
                "has_conflict": row[11],
                "rollback_conflict_details": rollback_conflict_details
            }
            
            # Fetch current row values
            table_name = row[2]
            row_id = row[3]
            
            current_row_sql = text(f'SELECT * FROM "{table_name}" WHERE _row_id = :row_id')
            current_row_result = conn.execute(current_row_sql, {"row_id": row_id}).mappings().fetchone()
            
            current_row = None
            if current_row_result:
                current_row = {
                    key: _make_json_safe(value)
                    for key, value in dict(current_row_result).items()
                    if not key.startswith("_")
                }
            
            return {
                "update": update_info,
                "current_row": current_row
            }
            
    except Exception as e:
        logger.error(f"Error retrieving row update detail: {str(e)}")
        raise


def rollback_single_update(
    import_id: str,
    update_id: int,
    rolled_back_by: Optional[str] = None,
    force: bool = False
) -> Dict[str, Any]:
    """
    Rollback a single row update.
    
    Args:
        import_id: UUID of the import
        update_id: ID of the row update to rollback
        rolled_back_by: User performing the rollback
        force: If True, rollback even if there's a conflict
        
    Returns:
        Dictionary with rollback result and any conflicts
    """
    engine = get_engine()
    
    # Get update details
    detail = get_row_update_detail(import_id, update_id)
    update = detail["update"]
    current_row = detail["current_row"]
    
    # Check if already rolled back
    if update["rolled_back_at"]:
        raise ValueError(f"Update {update_id} has already been rolled back")
    
    table_name = update["table_name"]
    row_id = update["row_id"]
    previous_values = update["previous_values"]
    updated_columns = update["updated_columns"]
    
    # Check for conflicts
    conflict = None
    if current_row:
        # Get only the columns that were updated
        current_relevant_values = {
            col: current_row.get(col)
            for col in updated_columns
        }
        current_hash = compute_row_hash(current_relevant_values)
        expected_hash = update["current_values_hash"]
        
        if current_hash != expected_hash:
            # Conflict detected
            new_values = update["new_values"]
            new_relevant_values = {col: new_values.get(col) for col in updated_columns}
            
            conflict = {
                "update_id": update_id,
                "row_id": row_id,
                "original_values": {col: previous_values.get(col) for col in updated_columns},
                "values_at_update": new_relevant_values,
                "current_values": current_relevant_values,
                "message": "Row has been modified since the update. Current values differ from expected values."
            }
            
            if not force:
                # Mark conflict and return without rolling back
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE row_updates
                            SET rollback_conflict = TRUE,
                                rollback_conflict_details = :conflict_details
                            WHERE id = :update_id
                        """), {
                            "update_id": update_id,
                            "conflict_details": json.dumps(conflict)
                        })
                except Exception as e:
                    logger.error(f"Error marking rollback conflict: {str(e)}")
                
                return {
                    "success": False,
                    "message": "Conflict detected. Use force=true to override.",
                    "update": update,
                    "conflict": conflict
                }
    
    # Perform rollback
    try:
        with engine.begin() as conn:
            # Restore previous values
            set_clauses = []
            params = {"row_id": row_id}
            
            for col in updated_columns:
                param_name = f"restore_{col}"
                set_clauses.append(f'"{col}" = :{param_name}')
                params[param_name] = previous_values.get(col)
            
            if set_clauses:
                restore_sql = text(f'''
                    UPDATE "{table_name}"
                    SET {", ".join(set_clauses)}
                    WHERE _row_id = :row_id
                ''')
                conn.execute(restore_sql, params)
            
            # Mark as rolled back
            conn.execute(text("""
                UPDATE row_updates
                SET rolled_back_at = NOW(),
                    rolled_back_by = :rolled_back_by,
                    rollback_conflict = :has_conflict,
                    rollback_conflict_details = :conflict_details
                WHERE id = :update_id
            """), {
                "update_id": update_id,
                "rolled_back_by": rolled_back_by,
                "has_conflict": conflict is not None,
                "conflict_details": json.dumps(conflict) if conflict else None
            })
            
            # Decrement rows_updated counter in import_history
            conn.execute(text("""
                UPDATE import_history
                SET rows_updated = GREATEST(0, COALESCE(rows_updated, 0) - 1)
                WHERE import_id = CAST(:import_id AS UUID)
            """), {"import_id": import_id})
        
        logger.info(f"Successfully rolled back update {update_id} for row {row_id}")
        
        # Update the update record with rollback info
        update["rolled_back_at"] = datetime.now(timezone.utc).isoformat()
        update["rolled_back_by"] = rolled_back_by
        update["has_conflict"] = conflict is not None
        
        return {
            "success": True,
            "message": "Update successfully rolled back" + (" (with conflict override)" if conflict and force else ""),
            "update": update,
            "conflict": conflict if force else None
        }
        
    except Exception as e:
        logger.error(f"Error rolling back update {update_id}: {str(e)}")
        raise


def list_all_row_updates(
    limit: int = 100,
    offset: int = 0,
    file_name: Optional[str] = None,
    table_name: Optional[str] = None,
    include_rolled_back: bool = False
) -> Tuple[List[Dict[str, Any]], int]:
    """
    List all row updates across all imports.
    
    Args:
        limit: Maximum number of updates to return
        offset: Number of updates to skip
        file_name: Optional filter by file name
        table_name: Optional filter by table name
        include_rolled_back: If True, include already rolled back updates
        
    Returns:
        Tuple of (updates_list, total_count)
    """
    engine = get_engine()
    
    where_clauses = []
    if not include_rolled_back:
        where_clauses.append("ru.rolled_back_at IS NULL")
    if file_name:
        where_clauses.append("ih.file_name ILIKE :file_name")
    if table_name:
        where_clauses.append("ru.table_name ILIKE :table_name")
    
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    count_sql = f"""
        SELECT COUNT(*) 
        FROM row_updates ru
        LEFT JOIN import_history ih ON ru.import_id = ih.import_id
        WHERE {where_clause}
    """
    
    query_sql = f"""
        SELECT 
            ru.id, ru.import_id, ru.table_name, ru.row_id,
            ru.previous_values, ru.new_values, ru.updated_columns,
            ru.current_values_hash, ru.updated_at,
            ru.rolled_back_at, ru.rolled_back_by,
            ru.rollback_conflict, ru.rollback_conflict_details,
            ih.file_name
        FROM row_updates ru
        LEFT JOIN import_history ih ON ru.import_id = ih.import_id
        WHERE {where_clause}
        ORDER BY ru.updated_at DESC
        LIMIT :limit OFFSET :offset
    """
    
    params = {
        "limit": limit,
        "offset": offset
    }
    
    if file_name:
        params["file_name"] = f"%{file_name}%"
    if table_name:
        params["table_name"] = f"%{table_name}%"
    
    updates = []
    total_count = 0
    
    try:
        with engine.connect() as conn:
            # Get total count
            total_count = conn.execute(text(count_sql), params).scalar() or 0
            
            # Get updates
            results = conn.execute(text(query_sql), params)
            
            for row in results:
                # Parse JSON fields
                previous_values = row[4]
                if isinstance(previous_values, str):
                    previous_values = json.loads(previous_values)
                previous_values = _make_json_safe(previous_values)
                
                new_values = row[5]
                if isinstance(new_values, str):
                    new_values = json.loads(new_values)
                new_values = _make_json_safe(new_values)
                
                rollback_conflict_details = row[12]
                if isinstance(rollback_conflict_details, str):
                    rollback_conflict_details = json.loads(rollback_conflict_details)
                if rollback_conflict_details:
                    rollback_conflict_details = _make_json_safe(rollback_conflict_details)
                
                updates.append({
                    "id": row[0],
                    "import_id": str(row[1]),
                    "table_name": row[2],
                    "row_id": row[3],
                    "previous_values": previous_values,
                    "new_values": new_values,
                    "updated_columns": row[6],
                    "current_values_hash": row[7],
                    "updated_at": row[8].isoformat() if row[8] else None,
                    "rolled_back_at": row[9].isoformat() if row[9] else None,
                    "rolled_back_by": row[10],
                    "has_conflict": row[11],
                    "rollback_conflict_details": rollback_conflict_details,
                    "file_name": row[13]
                })
        
        return updates, total_count
        
    except Exception as e:
        logger.error(f"Error listing all row updates: {str(e)}")
        raise


def rollback_import_updates(
    import_id: str,
    rolled_back_by: Optional[str] = None,
    skip_conflicts: bool = False
) -> Dict[str, Any]:
    """
    Rollback all updates from an import.
    
    Args:
        import_id: UUID of the import
        rolled_back_by: User performing the rollback
        skip_conflicts: If True, skip updates with conflicts; if False, stop on first conflict
        
    Returns:
        Dictionary with rollback results and any conflicts encountered
    """
    engine = get_engine()
    
    # Get all non-rolled-back updates
    updates, total_count = list_row_updates(import_id, limit=10000, include_rolled_back=False)
    
    if total_count == 0:
        return {
            "success": True,
            "message": "No updates to rollback",
            "updates_rolled_back": 0,
            "conflicts": []
        }
    
    rolled_back_count = 0
    conflicts = []
    
    logger.info(f"Starting rollback of {total_count} updates for import {import_id}")
    
    for update in updates:
        update_id = update["id"]
        
        try:
            result = rollback_single_update(
                import_id=import_id,
                update_id=update_id,
                rolled_back_by=rolled_back_by,
                force=skip_conflicts  # If skipping conflicts, force through them
            )
            
            if result["success"]:
                rolled_back_count += 1
                if result.get("conflict"):
                    conflicts.append(result["conflict"])
            else:
                # Conflict encountered and not forcing
                conflicts.append(result["conflict"])
                if not skip_conflicts:
                    # Stop on first conflict
                    logger.warning(f"Stopping rollback due to conflict on update {update_id}")
                    break
                    
        except Exception as e:
            logger.error(f"Error rolling back update {update_id}: {str(e)}")
            if not skip_conflicts:
                raise
    
    message = f"Successfully rolled back {rolled_back_count} of {total_count} updates"
    if conflicts:
        message += f" ({len(conflicts)} conflicts " + ("overridden" if skip_conflicts else "encountered") + ")"
    
    return {
        "success": True,
        "message": message,
        "updates_rolled_back": rolled_back_count,
        "conflicts": conflicts if conflicts else None
    }

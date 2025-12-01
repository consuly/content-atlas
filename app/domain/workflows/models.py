"""
Database models and operations for workflows.

This module handles all database interactions for workflow management,
including CRUD operations for workflows, steps, variables, and executions.
"""
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.db.session import get_engine

logger = logging.getLogger(__name__)


def create_workflow_tables():
    """Create all workflow-related tables if they don't exist."""
    engine = get_engine()
    
    with engine.begin() as conn:
        # Workflows table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS workflows (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(255) NOT NULL,
                description TEXT,
                created_by VARCHAR(255),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                is_active BOOLEAN DEFAULT TRUE
            )
        """))
        
        # Workflow steps table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS workflow_steps (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                workflow_id UUID REFERENCES workflows(id) ON DELETE CASCADE,
                step_order INTEGER NOT NULL,
                name VARCHAR(255),
                prompt_template TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(workflow_id, step_order)
            )
        """))
        
        # Workflow variables table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS workflow_variables (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                workflow_id UUID REFERENCES workflows(id) ON DELETE CASCADE,
                name VARCHAR(100) NOT NULL,
                display_name VARCHAR(255),
                variable_type VARCHAR(50) DEFAULT 'text',
                default_value TEXT,
                options JSONB,
                required BOOLEAN DEFAULT TRUE,
                UNIQUE(workflow_id, name)
            )
        """))
        
        # Workflow executions table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS workflow_executions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                workflow_id UUID REFERENCES workflows(id),
                executed_by VARCHAR(255),
                executed_at TIMESTAMP DEFAULT NOW(),
                status VARCHAR(50) DEFAULT 'running',
                variables_used JSONB,
                completed_at TIMESTAMP,
                error_message TEXT
            )
        """))
        
        # Workflow step results table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS workflow_step_results (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                execution_id UUID REFERENCES workflow_executions(id) ON DELETE CASCADE,
                step_id UUID REFERENCES workflow_steps(id),
                step_order INTEGER,
                executed_sql TEXT,
                result_csv TEXT,
                rows_returned INTEGER,
                execution_time_seconds FLOAT,
                status VARCHAR(50),
                error_message TEXT,
                executed_at TIMESTAMP DEFAULT NOW()
            )
        """))
    
    logger.info("Workflow tables created successfully")


def create_workflow(
    name: str,
    description: Optional[str],
    steps: List[Dict[str, Any]],
    variables: List[Dict[str, Any]],
    created_by: Optional[str] = None
) -> str:
    """
    Create a new workflow with steps and variables.
    
    Args:
        name: Workflow name
        description: Optional description
        steps: List of step definitions with step_order, name, prompt_template
        variables: List of variable definitions
        created_by: Optional user identifier
        
    Returns:
        Workflow ID (UUID as string)
    """
    engine = get_engine()
    workflow_id = str(uuid4())
    
    with engine.begin() as conn:
        # Insert workflow
        conn.execute(text("""
            INSERT INTO workflows (id, name, description, created_by)
            VALUES (:id, :name, :description, :created_by)
        """), {
            "id": workflow_id,
            "name": name,
            "description": description,
            "created_by": created_by
        })
        
        # Insert steps
        for step in steps:
            conn.execute(text("""
                INSERT INTO workflow_steps (workflow_id, step_order, name, prompt_template)
                VALUES (:workflow_id, :step_order, :name, :prompt_template)
            """), {
                "workflow_id": workflow_id,
                "step_order": step["step_order"],
                "name": step.get("name"),
                "prompt_template": step["prompt_template"]
            })
        
        # Insert variables
        for var in variables:
            conn.execute(text("""
                INSERT INTO workflow_variables 
                (workflow_id, name, display_name, variable_type, default_value, options, required)
                VALUES (:workflow_id, :name, :display_name, :variable_type, :default_value, :options, :required)
            """), {
                "workflow_id": workflow_id,
                "name": var["name"],
                "display_name": var.get("display_name"),
                "variable_type": var.get("variable_type", "text"),
                "default_value": var.get("default_value"),
                "options": json.dumps(var.get("options")) if var.get("options") else None,
                "required": var.get("required", True)
            })
    
    logger.info(f"Created workflow {workflow_id}: {name}")
    return workflow_id


def get_workflow(workflow_id: str) -> Optional[Dict[str, Any]]:
    """
    Get workflow details including steps and variables.
    
    Args:
        workflow_id: Workflow UUID
        
    Returns:
        Workflow dict with steps and variables, or None if not found
    """
    engine = get_engine()
    
    with engine.connect() as conn:
        # Get workflow
        result = conn.execute(text("""
            SELECT id, name, description, created_by, created_at, updated_at, is_active
            FROM workflows
            WHERE id = :id
        """), {"id": workflow_id})
        
        row = result.fetchone()
        if not row:
            return None
        
        workflow = {
            "id": str(row[0]),
            "name": row[1],
            "description": row[2],
            "created_by": row[3],
            "created_at": row[4],
            "updated_at": row[5],
            "is_active": row[6]
        }
        
        # Get steps
        steps_result = conn.execute(text("""
            SELECT id, step_order, name, prompt_template
            FROM workflow_steps
            WHERE workflow_id = :workflow_id
            ORDER BY step_order
        """), {"workflow_id": workflow_id})
        
        workflow["steps"] = [
            {
                "id": str(row[0]),
                "step_order": row[1],
                "name": row[2],
                "prompt_template": row[3]
            }
            for row in steps_result
        ]
        
        # Get variables
        vars_result = conn.execute(text("""
            SELECT id, name, display_name, variable_type, default_value, options, required
            FROM workflow_variables
            WHERE workflow_id = :workflow_id
            ORDER BY name
        """), {"workflow_id": workflow_id})
        
        workflow["variables"] = [
            {
                "id": str(row[0]),
                "name": row[1],
                "display_name": row[2],
                "variable_type": row[3],
                "default_value": row[4],
                "options": row[5] if isinstance(row[5], (list, dict)) else (json.loads(row[5]) if row[5] else None),
                "required": row[6]
            }
            for row in vars_result
        ]
    
    return workflow


def list_workflows(
    limit: int = 50,
    offset: int = 0,
    active_only: bool = True
) -> List[Dict[str, Any]]:
    """
    List all workflows.
    
    Args:
        limit: Maximum number of workflows to return
        offset: Number of workflows to skip
        active_only: If True, only return active workflows
        
    Returns:
        List of workflow summaries
    """
    engine = get_engine()
    
    with engine.connect() as conn:
        query = """
            SELECT w.id, w.name, w.description, w.created_by, w.created_at, w.updated_at, w.is_active,
                   COUNT(DISTINCT ws.id) as step_count,
                   COUNT(DISTINCT wv.id) as variable_count
            FROM workflows w
            LEFT JOIN workflow_steps ws ON w.id = ws.workflow_id
            LEFT JOIN workflow_variables wv ON w.id = wv.workflow_id
        """
        
        if active_only:
            query += " WHERE w.is_active = TRUE"
        
        query += """
            GROUP BY w.id, w.name, w.description, w.created_by, w.created_at, w.updated_at, w.is_active
            ORDER BY w.updated_at DESC
            LIMIT :limit OFFSET :offset
        """
        
        result = conn.execute(text(query), {"limit": limit, "offset": offset})
        
        workflows = [
            {
                "id": str(row[0]),
                "name": row[1],
                "description": row[2],
                "created_by": row[3],
                "created_at": row[4],
                "updated_at": row[5],
                "is_active": row[6],
                "step_count": row[7],
                "variable_count": row[8]
            }
            for row in result
        ]
    
    return workflows


def update_workflow(
    workflow_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    is_active: Optional[bool] = None
) -> bool:
    """
    Update workflow metadata.
    
    Args:
        workflow_id: Workflow UUID
        name: New name (optional)
        description: New description (optional)
        is_active: New active status (optional)
        
    Returns:
        True if updated, False if not found
    """
    engine = get_engine()
    
    updates = []
    params = {"id": workflow_id}
    
    if name is not None:
        updates.append("name = :name")
        params["name"] = name
    
    if description is not None:
        updates.append("description = :description")
        params["description"] = description
    
    if is_active is not None:
        updates.append("is_active = :is_active")
        params["is_active"] = is_active
    
    if not updates:
        return True  # Nothing to update
    
    updates.append("updated_at = NOW()")
    
    with engine.begin() as conn:
        result = conn.execute(
            text(f"UPDATE workflows SET {', '.join(updates)} WHERE id = :id"),
            params
        )
        
        return result.rowcount > 0


def delete_workflow(workflow_id: str) -> bool:
    """
    Delete a workflow (cascade deletes steps, variables, executions).
    
    Args:
        workflow_id: Workflow UUID
        
    Returns:
        True if deleted, False if not found
    """
    engine = get_engine()
    
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM workflows WHERE id = :id"),
            {"id": workflow_id}
        )
        
        return result.rowcount > 0


def create_execution(
    workflow_id: str,
    variables_used: Dict[str, Any],
    executed_by: Optional[str] = None
) -> str:
    """
    Create a new workflow execution record.
    
    Args:
        workflow_id: Workflow UUID
        variables_used: Variable values used for this execution
        executed_by: Optional user identifier
        
    Returns:
        Execution ID (UUID as string)
    """
    engine = get_engine()
    execution_id = str(uuid4())
    
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO workflow_executions (id, workflow_id, executed_by, variables_used)
            VALUES (:id, :workflow_id, :executed_by, :variables_used)
        """), {
            "id": execution_id,
            "workflow_id": workflow_id,
            "executed_by": executed_by,
            "variables_used": json.dumps(variables_used)
        })
    
    return execution_id


def update_execution_status(
    execution_id: str,
    status: str,
    error_message: Optional[str] = None
):
    """
    Update execution status.
    
    Args:
        execution_id: Execution UUID
        status: New status (running, completed, failed)
        error_message: Optional error message
    """
    engine = get_engine()
    
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE workflow_executions
            SET status = :status,
                error_message = :error_message,
                completed_at = CASE WHEN :status IN ('completed', 'failed') THEN NOW() ELSE completed_at END
            WHERE id = :id
        """), {
            "id": execution_id,
            "status": status,
            "error_message": error_message
        })


def save_step_result(
    execution_id: str,
    step_id: str,
    step_order: int,
    executed_sql: Optional[str],
    result_csv: Optional[str],
    rows_returned: Optional[int],
    execution_time_seconds: Optional[float],
    status: str,
    error_message: Optional[str] = None
):
    """
    Save the result of a workflow step execution.
    
    Args:
        execution_id: Execution UUID
        step_id: Step UUID
        step_order: Step order number
        executed_sql: SQL that was executed
        result_csv: CSV result data
        rows_returned: Number of rows returned
        execution_time_seconds: Execution time
        status: Step status (success, failed, skipped)
        error_message: Optional error message
    """
    engine = get_engine()
    
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO workflow_step_results 
            (execution_id, step_id, step_order, executed_sql, result_csv, 
             rows_returned, execution_time_seconds, status, error_message)
            VALUES (:execution_id, :step_id, :step_order, :executed_sql, :result_csv,
                    :rows_returned, :execution_time_seconds, :status, :error_message)
        """), {
            "execution_id": execution_id,
            "step_id": step_id,
            "step_order": step_order,
            "executed_sql": executed_sql,
            "result_csv": result_csv,
            "rows_returned": rows_returned,
            "execution_time_seconds": execution_time_seconds,
            "status": status,
            "error_message": error_message
        })


def get_execution(execution_id: str) -> Optional[Dict[str, Any]]:
    """
    Get execution details with all step results.
    
    Args:
        execution_id: Execution UUID
        
    Returns:
        Execution dict with step results, or None if not found
    """
    engine = get_engine()
    
    with engine.connect() as conn:
        # Get execution
        result = conn.execute(text("""
            SELECT e.id, e.workflow_id, e.executed_by, e.executed_at, e.status,
                   e.variables_used, e.completed_at, e.error_message,
                   w.name as workflow_name
            FROM workflow_executions e
            JOIN workflows w ON e.workflow_id = w.id
            WHERE e.id = :id
        """), {"id": execution_id})
        
        row = result.fetchone()
        if not row:
            return None
        
        execution = {
            "id": str(row[0]),
            "workflow_id": str(row[1]),
            "executed_by": row[2],
            "executed_at": row[3],
            "status": row[4],
            "variables_used": row[5] if isinstance(row[5], dict) else (json.loads(row[5]) if row[5] else {}),
            "completed_at": row[6],
            "error_message": row[7],
            "workflow_name": row[8]
        }
        
        # Get step results
        steps_result = conn.execute(text("""
            SELECT r.id, r.step_id, r.step_order, r.executed_sql, r.result_csv,
                   r.rows_returned, r.execution_time_seconds, r.status, r.error_message,
                   r.executed_at, s.name as step_name
            FROM workflow_step_results r
            LEFT JOIN workflow_steps s ON r.step_id = s.id
            WHERE r.execution_id = :execution_id
            ORDER BY r.step_order
        """), {"execution_id": execution_id})
        
        execution["step_results"] = [
            {
                "id": str(row[0]),
                "step_id": str(row[1]),
                "step_order": row[2],
                "executed_sql": row[3],
                "result_csv": row[4],
                "rows_returned": row[5],
                "execution_time_seconds": row[6],
                "status": row[7],
                "error_message": row[8],
                "executed_at": row[9],
                "step_name": row[10]
            }
            for row in steps_result
        ]
    
    return execution


def list_executions(
    workflow_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """
    List workflow executions.
    
    Args:
        workflow_id: Optional workflow UUID to filter by
        limit: Maximum number of executions to return
        offset: Number of executions to skip
        
    Returns:
        List of execution summaries
    """
    engine = get_engine()
    
    with engine.connect() as conn:
        query = """
            SELECT e.id, e.workflow_id, e.executed_by, e.executed_at, e.status,
                   e.variables_used, e.completed_at, w.name as workflow_name,
                   COUNT(r.id) as step_count
            FROM workflow_executions e
            JOIN workflows w ON e.workflow_id = w.id
            LEFT JOIN workflow_step_results r ON e.id = r.execution_id
        """
        
        params = {"limit": limit, "offset": offset}
        
        if workflow_id:
            query += " WHERE e.workflow_id = :workflow_id"
            params["workflow_id"] = workflow_id
        
        query += """
            GROUP BY e.id, e.workflow_id, e.executed_by, e.executed_at, e.status,
                     e.variables_used, e.completed_at, w.name
            ORDER BY e.executed_at DESC
            LIMIT :limit OFFSET :offset
        """
        
        result = conn.execute(text(query), params)
        
        executions = [
            {
                "id": str(row[0]),
                "workflow_id": str(row[1]),
                "executed_by": row[2],
                "executed_at": row[3],
                "status": row[4],
                "variables_used": row[5] if isinstance(row[5], dict) else (json.loads(row[5]) if row[5] else {}),
                "completed_at": row[6],
                "workflow_name": row[7],
                "step_count": row[8]
            }
            for row in result
        ]
    
    return executions

"""
Workflow execution engine.

This module handles the execution of workflow steps, variable substitution,
and result collection.
"""
import logging
import re
import time
from typing import Any, Dict, List, Optional

from app.domain.queries.agent import query_database_with_agent
from app.domain.workflows.models import (
    create_execution,
    get_workflow,
    save_step_result,
    update_execution_status,
)

logger = logging.getLogger(__name__)


def substitute_variables(template: str, variables: Dict[str, Any]) -> str:
    """
    Substitute variables in a template string.
    
    Variables are specified as {{variable_name}} in the template.
    
    Args:
        template: Template string with {{variable}} placeholders
        variables: Dictionary of variable values
        
    Returns:
        String with variables substituted
        
    Example:
        >>> substitute_variables("Get revenue for {{client}} in {{year}}", 
        ...                      {"client": "Acme", "year": "2025"})
        'Get revenue for Acme in 2025'
    """
    result = template
    
    # Find all {{variable}} patterns
    pattern = r'\{\{(\w+)\}\}'
    matches = re.findall(pattern, template)
    
    for var_name in matches:
        if var_name in variables:
            value = str(variables[var_name])
            result = result.replace(f"{{{{{var_name}}}}}", value)
        else:
            logger.warning(f"Variable '{var_name}' not found in provided variables")
    
    return result


def validate_variables(
    workflow: Dict[str, Any],
    provided_variables: Dict[str, Any]
) -> tuple[bool, Optional[str]]:
    """
    Validate that all required variables are provided.
    
    Args:
        workflow: Workflow definition with variables
        provided_variables: Variables provided by user
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_vars = [
        var["name"] for var in workflow["variables"]
        if var.get("required", True)
    ]
    
    missing = [var for var in required_vars if var not in provided_variables]
    
    if missing:
        return False, f"Missing required variables: {', '.join(missing)}"
    
    return True, None


def execute_workflow(
    workflow_id: str,
    variables: Dict[str, Any],
    executed_by: Optional[str] = None,
    include_context: bool = True
) -> Dict[str, Any]:
    """
    Execute a workflow with the provided variables.
    
    Args:
        workflow_id: Workflow UUID
        variables: Variable values to use
        executed_by: Optional user identifier
        include_context: If True, pass previous step results as context to later steps
        
    Returns:
        Execution result with all step results
    """
    start_time = time.time()
    
    # Get workflow definition
    workflow = get_workflow(workflow_id)
    if not workflow:
        return {
            "success": False,
            "error": f"Workflow {workflow_id} not found"
        }
    
    # Validate variables
    is_valid, error_msg = validate_variables(workflow, variables)
    if not is_valid:
        return {
            "success": False,
            "error": error_msg
        }
    
    # Create execution record
    execution_id = create_execution(workflow_id, variables, executed_by)
    
    logger.info(f"Starting workflow execution {execution_id} for workflow {workflow['name']}")
    
    step_results = []
    previous_results = []  # Store results from previous steps for context
    
    try:
        # Execute each step in order
        for step in workflow["steps"]:
            step_start_time = time.time()
            
            # Substitute variables in prompt template
            prompt = substitute_variables(step["prompt_template"], variables)
            
            # Add context from previous steps if enabled
            if include_context and previous_results:
                context_summary = "\n\nContext from previous steps:\n"
                for i, prev_result in enumerate(previous_results, 1):
                    if prev_result.get("executed_sql"):
                        context_summary += f"\nStep {i}: {prev_result['step_name'] or 'Unnamed'}\n"
                        context_summary += f"SQL: {prev_result['executed_sql']}\n"
                        if prev_result.get("rows_returned"):
                            context_summary += f"Returned {prev_result['rows_returned']} rows\n"
                
                prompt = prompt + context_summary
            
            logger.info(f"Executing step {step['step_order']}: {step.get('name', 'Unnamed')}")
            
            # Execute the query using the existing agent
            try:
                result = query_database_with_agent(prompt)
                
                step_execution_time = time.time() - step_start_time
                
                # Determine step status
                if result.get("success"):
                    step_status = "success"
                    step_error = None
                else:
                    step_status = "failed"
                    step_error = result.get("error", "Unknown error")
                
                # Save step result
                save_step_result(
                    execution_id=execution_id,
                    step_id=step["id"],
                    step_order=step["step_order"],
                    executed_sql=result.get("executed_sql"),
                    result_csv=result.get("data_csv"),
                    rows_returned=result.get("rows_returned"),
                    execution_time_seconds=step_execution_time,
                    status=step_status,
                    error_message=step_error
                )
                
                # Store result for context
                step_result = {
                    "step_order": step["step_order"],
                    "step_name": step.get("name"),
                    "executed_sql": result.get("executed_sql"),
                    "result_csv": result.get("data_csv"),
                    "rows_returned": result.get("rows_returned"),
                    "execution_time_seconds": step_execution_time,
                    "status": step_status,
                    "error_message": step_error,
                    "response": result.get("response")
                }
                
                step_results.append(step_result)
                previous_results.append(step_result)
                
                # If step failed, decide whether to continue
                if step_status == "failed":
                    logger.warning(f"Step {step['step_order']} failed: {step_error}")
                    # For now, continue with remaining steps
                    # Could add a "stop_on_error" flag to workflow config
                
            except Exception as e:
                logger.error(f"Error executing step {step['step_order']}: {e}", exc_info=True)
                
                step_execution_time = time.time() - step_start_time
                
                # Save failed step result
                save_step_result(
                    execution_id=execution_id,
                    step_id=step["id"],
                    step_order=step["step_order"],
                    executed_sql=None,
                    result_csv=None,
                    rows_returned=None,
                    execution_time_seconds=step_execution_time,
                    status="failed",
                    error_message=str(e)
                )
                
                step_results.append({
                    "step_order": step["step_order"],
                    "step_name": step.get("name"),
                    "status": "failed",
                    "error_message": str(e),
                    "execution_time_seconds": step_execution_time
                })
        
        # Mark execution as completed
        update_execution_status(execution_id, "completed")
        
        total_time = time.time() - start_time
        
        logger.info(f"Workflow execution {execution_id} completed in {total_time:.2f}s")
        
        return {
            "success": True,
            "execution_id": execution_id,
            "workflow_id": workflow_id,
            "workflow_name": workflow["name"],
            "status": "completed",
            "variables_used": variables,
            "step_results": step_results,
            "total_execution_time_seconds": total_time
        }
        
    except Exception as e:
        logger.error(f"Workflow execution {execution_id} failed: {e}", exc_info=True)
        
        # Mark execution as failed
        update_execution_status(execution_id, "failed", str(e))
        
        total_time = time.time() - start_time
        
        return {
            "success": False,
            "execution_id": execution_id,
            "workflow_id": workflow_id,
            "workflow_name": workflow["name"],
            "status": "failed",
            "error": str(e),
            "variables_used": variables,
            "step_results": step_results,
            "total_execution_time_seconds": total_time
        }

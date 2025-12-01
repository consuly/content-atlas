"""
Workflow management API endpoints.

This module provides REST API endpoints for creating, managing, and executing workflows.
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.api.schemas.shared import (
    CreateWorkflowRequest,
    ExecuteWorkflowRequest,
    GenerateWorkflowRequest,
    GenerateWorkflowResponse,
    UpdateWorkflowRequest,
    WorkflowCreateResponse,
    WorkflowDetail,
    WorkflowExecutionDetailResponse,
    WorkflowExecutionListResponse,
    WorkflowExecutionResponse,
    WorkflowExecutionSummary,
    WorkflowListResponse,
    WorkflowResponse,
    WorkflowSummary,
)
from app.domain.workflows.executor import execute_workflow
from app.domain.workflows.generator import generate_workflow_from_description
from app.domain.workflows.models import (
    create_workflow,
    delete_workflow,
    get_execution,
    get_workflow,
    list_executions,
    list_workflows,
    update_workflow,
)

router = APIRouter(prefix="/workflows", tags=["workflows"])
logger = logging.getLogger(__name__)


@router.post("", response_model=WorkflowCreateResponse)
async def create_workflow_endpoint(request: CreateWorkflowRequest):
    """
    Create a new workflow.
    
    A workflow consists of:
    - Ordered steps (LLM prompts that generate SQL queries)
    - Variables (configurable parameters like dates, filters)
    
    Example:
    ```json
    {
      "name": "Monthly Revenue Report",
      "description": "Generate revenue analysis for a time period",
      "steps": [
        {
          "step_order": 1,
          "name": "Total Revenue",
          "prompt_template": "Calculate total revenue between {{start_date}} and {{end_date}}"
        }
      ],
      "variables": [
        {
          "name": "start_date",
          "display_name": "Start Date",
          "variable_type": "date",
          "required": true
        }
      ]
    }
    ```
    """
    try:
        # Convert Pydantic models to dicts
        steps = [step.model_dump() for step in request.steps]
        variables = [var.model_dump() for var in request.variables]
        
        workflow_id = create_workflow(
            name=request.name,
            description=request.description,
            steps=steps,
            variables=variables,
            created_by=request.created_by
        )
        
        return WorkflowCreateResponse(
            success=True,
            workflow_id=workflow_id,
            message=f"Workflow '{request.name}' created successfully"
        )
        
    except Exception as e:
        logger.error(f"Error creating workflow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=WorkflowListResponse)
async def list_workflows_endpoint(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    active_only: bool = Query(default=True)
):
    """
    List all workflows.
    
    Parameters:
    - limit: Maximum number of workflows to return (1-100)
    - offset: Number of workflows to skip
    - active_only: If true, only return active workflows
    """
    try:
        workflows = list_workflows(limit=limit, offset=offset, active_only=active_only)
        
        # Convert to Pydantic models
        workflow_summaries = [
            WorkflowSummary(**wf) for wf in workflows
        ]
        
        return WorkflowListResponse(
            success=True,
            workflows=workflow_summaries,
            total_count=len(workflow_summaries)
        )
        
    except Exception as e:
        logger.error(f"Error listing workflows: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_workflow_endpoint(workflow_id: str):
    """
    Get detailed information about a specific workflow.
    
    Returns:
    - Workflow metadata
    - All steps with their prompts
    - All variables with their configurations
    """
    try:
        workflow = get_workflow(workflow_id)
        
        if not workflow:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
        
        workflow_detail = WorkflowDetail(**workflow)
        
        return WorkflowResponse(
            success=True,
            workflow=workflow_detail
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workflow {workflow_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{workflow_id}", response_model=WorkflowResponse)
async def update_workflow_endpoint(workflow_id: str, request: UpdateWorkflowRequest):
    """
    Update workflow metadata.
    
    Can update:
    - name
    - description
    - is_active (to deactivate/activate a workflow)
    
    Note: Cannot update steps or variables through this endpoint.
    To modify steps/variables, create a new workflow version.
    """
    try:
        success = update_workflow(
            workflow_id=workflow_id,
            name=request.name,
            description=request.description,
            is_active=request.is_active
        )
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
        
        # Get updated workflow
        workflow = get_workflow(workflow_id)
        workflow_detail = WorkflowDetail(**workflow)
        
        return WorkflowResponse(
            success=True,
            workflow=workflow_detail
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating workflow {workflow_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{workflow_id}")
async def delete_workflow_endpoint(workflow_id: str):
    """
    Delete a workflow.
    
    This will cascade delete:
    - All workflow steps
    - All workflow variables
    - All execution history
    
    Use with caution!
    """
    try:
        success = delete_workflow(workflow_id)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
        
        return {
            "success": True,
            "message": f"Workflow {workflow_id} deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting workflow {workflow_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{workflow_id}/execute", response_model=WorkflowExecutionResponse)
async def execute_workflow_endpoint(workflow_id: str, request: ExecuteWorkflowRequest):
    """
    Execute a workflow with the provided variable values.
    
    The workflow will:
    1. Validate that all required variables are provided
    2. Execute each step in order
    3. Substitute variables into prompt templates
    4. Pass context from previous steps to later steps (if include_context=true)
    5. Store all results in the database
    
    Example:
    ```json
    {
      "variables": {
        "start_date": "2025-01-01",
        "end_date": "2025-01-31",
        "client_name": "Acme Corp"
      },
      "executed_by": "user@example.com",
      "include_context": true
    }
    ```
    
    Returns execution results with all step outputs.
    """
    try:
        result = execute_workflow(
            workflow_id=workflow_id,
            variables=request.variables,
            executed_by=request.executed_by,
            include_context=request.include_context
        )
        
        if not result.get("success"):
            # Return error but with 200 status (execution was attempted)
            return WorkflowExecutionResponse(**result)
        
        return WorkflowExecutionResponse(**result)
        
    except Exception as e:
        logger.error(f"Error executing workflow {workflow_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{workflow_id}/executions", response_model=WorkflowExecutionListResponse)
async def list_workflow_executions_endpoint(
    workflow_id: str,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0)
):
    """
    List execution history for a specific workflow.
    
    Returns summaries of all executions including:
    - Execution status
    - Variables used
    - Execution time
    - Number of steps completed
    """
    try:
        executions = list_executions(
            workflow_id=workflow_id,
            limit=limit,
            offset=offset
        )
        
        execution_summaries = [
            WorkflowExecutionSummary(**ex) for ex in executions
        ]
        
        return WorkflowExecutionListResponse(
            success=True,
            executions=execution_summaries,
            total_count=len(execution_summaries)
        )
        
    except Exception as e:
        logger.error(f"Error listing executions for workflow {workflow_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executions/{execution_id}", response_model=WorkflowExecutionDetailResponse)
async def get_execution_endpoint(execution_id: str):
    """
    Get detailed results from a specific workflow execution.
    
    Returns:
    - Execution metadata
    - Variables used
    - Results from each step including:
      - SQL executed
      - CSV data returned
      - Execution time
      - Status and errors
    """
    try:
        execution = get_execution(execution_id)
        
        if not execution:
            raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
        
        return WorkflowExecutionDetailResponse(
            success=True,
            execution=execution
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting execution {execution_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate", response_model=GenerateWorkflowResponse)
async def generate_workflow_endpoint(request: GenerateWorkflowRequest):
    """
    Generate a workflow from a natural language description using LLM.
    
    The LLM will:
    1. Analyze your database schema
    2. Understand your requirements
    3. Design appropriate workflow steps
    4. Identify configurable variables
    5. Return a complete workflow definition
    
    Example:
    ```json
    {
      "description": "Create a workflow to analyze monthly revenue by client with date filters"
    }
    ```
    
    The generated workflow can then be created using the POST /workflows endpoint.
    """
    try:
        result = generate_workflow_from_description(request.description)
        
        return GenerateWorkflowResponse(**result)
        
    except Exception as e:
        logger.error(f"Error generating workflow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

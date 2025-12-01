"""
Tests for workflow management functionality.

This test suite covers:
1. Variable substitution and validation
2. Workflow CRUD operations (create, read, update, delete)
3. Workflow execution with real database queries
4. API endpoints
5. LLM workflow generation
6. Edge cases and error handling
"""
import pytest
import json
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from app.main import app
from app.domain.workflows.models import (
    create_workflow,
    get_workflow,
    list_workflows,
    update_workflow,
    delete_workflow,
    create_execution,
    get_execution,
    list_executions,
    update_execution_status,
    save_step_result,
)
from app.domain.workflows.executor import (
    substitute_variables,
    validate_variables,
    execute_workflow,
)
from app.domain.workflows.generator import generate_workflow_from_description

client = TestClient(app)


def test_substitute_variables():
    """Test variable substitution in templates."""
    template = "Get revenue for {{client}} between {{start_date}} and {{end_date}}"
    variables = {
        "client": "Acme Corp",
        "start_date": "2025-01-01",
        "end_date": "2025-01-31"
    }
    
    result = substitute_variables(template, variables)
    
    assert result == "Get revenue for Acme Corp between 2025-01-01 and 2025-01-31"


def test_substitute_variables_missing():
    """Test variable substitution with missing variables."""
    template = "Get revenue for {{client}} in {{year}}"
    variables = {"client": "Acme Corp"}
    
    # Should not raise error, just leave placeholder
    result = substitute_variables(template, variables)
    
    assert "Acme Corp" in result
    assert "{{year}}" in result


def test_create_and_get_workflow(initialize_test_database):
    """Test creating and retrieving a workflow."""
    steps = [
        {
            "step_order": 1,
            "name": "Total Revenue",
            "prompt_template": "Calculate total revenue between {{start_date}} and {{end_date}}"
        },
        {
            "step_order": 2,
            "name": "Revenue by Client",
            "prompt_template": "Show revenue breakdown by client for period {{start_date}} to {{end_date}}"
        }
    ]
    
    variables = [
        {
            "name": "start_date",
            "display_name": "Start Date",
            "variable_type": "date",
            "required": True
        },
        {
            "name": "end_date",
            "display_name": "End Date",
            "variable_type": "date",
            "required": True
        }
    ]
    
    workflow_id = create_workflow(
        name="Monthly Revenue Report",
        description="Generate revenue analysis for a time period",
        steps=steps,
        variables=variables,
        created_by="test@example.com"
    )
    
    assert workflow_id is not None
    
    # Retrieve workflow
    workflow = get_workflow(workflow_id)
    
    assert workflow is not None
    assert workflow["name"] == "Monthly Revenue Report"
    assert workflow["description"] == "Generate revenue analysis for a time period"
    assert workflow["created_by"] == "test@example.com"
    assert workflow["is_active"] is True
    assert len(workflow["steps"]) == 2
    assert len(workflow["variables"]) == 2
    
    # Check steps
    assert workflow["steps"][0]["step_order"] == 1
    assert workflow["steps"][0]["name"] == "Total Revenue"
    assert "{{start_date}}" in workflow["steps"][0]["prompt_template"]
    
    # Check variables
    var_names = [v["name"] for v in workflow["variables"]]
    assert "start_date" in var_names
    assert "end_date" in var_names


def test_list_workflows(initialize_test_database):
    """Test listing workflows."""
    # Create multiple workflows
    for i in range(3):
        create_workflow(
            name=f"Test Workflow {i}",
            description=f"Description {i}",
            steps=[{
                "step_order": 1,
                "prompt_template": f"Test query {i}"
            }],
            variables=[{
                "name": "test_var",
                "variable_type": "text"
            }]
        )
    
    workflows = list_workflows(limit=10, offset=0)
    
    assert len(workflows) >= 3
    assert all("name" in wf for wf in workflows)
    assert all("step_count" in wf for wf in workflows)
    assert all("variable_count" in wf for wf in workflows)


def test_update_workflow(initialize_test_database):
    """Test updating workflow metadata."""
    workflow_id = create_workflow(
        name="Original Name",
        description="Original Description",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    # Update workflow
    success = update_workflow(
        workflow_id=workflow_id,
        name="Updated Name",
        description="Updated Description",
        is_active=False
    )
    
    assert success is True
    
    # Verify updates
    workflow = get_workflow(workflow_id)
    assert workflow["name"] == "Updated Name"
    assert workflow["description"] == "Updated Description"
    assert workflow["is_active"] is False


def test_delete_workflow(initialize_test_database):
    """Test deleting a workflow."""
    workflow_id = create_workflow(
        name="To Delete",
        description="Will be deleted",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    # Delete workflow
    success = delete_workflow(workflow_id)
    assert success is True
    
    # Verify deletion
    workflow = get_workflow(workflow_id)
    assert workflow is None


def test_validate_variables():
    """Test variable validation."""
    workflow = {
        "variables": [
            {"name": "required_var", "required": True},
            {"name": "optional_var", "required": False}
        ]
    }
    
    # Valid - all required variables provided
    is_valid, error = validate_variables(workflow, {
        "required_var": "value",
        "optional_var": "value"
    })
    assert is_valid is True
    assert error is None
    
    # Valid - optional variable missing
    is_valid, error = validate_variables(workflow, {
        "required_var": "value"
    })
    assert is_valid is True
    
    # Invalid - required variable missing
    is_valid, error = validate_variables(workflow, {
        "optional_var": "value"
    })
    assert is_valid is False
    assert "required_var" in error


def test_create_and_get_execution(initialize_test_database):
    """Test creating and retrieving an execution."""
    # Create workflow
    workflow_id = create_workflow(
        name="Test Workflow",
        description="Test",
        steps=[{"step_order": 1, "prompt_template": "Test query"}],
        variables=[{"name": "test_var", "variable_type": "text"}]
    )
    
    # Create execution
    variables_used = {"test_var": "test_value"}
    execution_id = create_execution(
        workflow_id=workflow_id,
        variables_used=variables_used,
        executed_by="test@example.com"
    )
    
    assert execution_id is not None
    
    # Retrieve execution
    execution = get_execution(execution_id)
    
    assert execution is not None
    assert execution["workflow_id"] == workflow_id
    assert execution["executed_by"] == "test@example.com"
    assert execution["status"] == "running"
    assert execution["variables_used"] == variables_used


def test_list_executions(initialize_test_database):
    """Test listing executions."""
    # Create workflow
    workflow_id = create_workflow(
        name="Test Workflow",
        description="Test",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    # Create multiple executions
    for i in range(3):
        create_execution(
            workflow_id=workflow_id,
            variables_used={"var": f"value{i}"}
        )
    
    # List all executions
    executions = list_executions(limit=10, offset=0)
    assert len(executions) >= 3
    
    # List executions for specific workflow
    workflow_executions = list_executions(workflow_id=workflow_id, limit=10, offset=0)
    assert len(workflow_executions) == 3


def test_workflow_with_select_variable(initialize_test_database):
    """Test workflow with select-type variable."""
    variables = [
        {
            "name": "report_type",
            "display_name": "Report Type",
            "variable_type": "select",
            "options": ["daily", "weekly", "monthly"],
            "required": True
        }
    ]
    
    workflow_id = create_workflow(
        name="Flexible Report",
        description="Report with selectable type",
        steps=[{
            "step_order": 1,
            "prompt_template": "Generate {{report_type}} report"
        }],
        variables=variables
    )
    
    workflow = get_workflow(workflow_id)
    
    assert workflow is not None
    assert len(workflow["variables"]) == 1
    assert workflow["variables"][0]["variable_type"] == "select"
    assert workflow["variables"][0]["options"] == ["daily", "weekly", "monthly"]


def test_workflow_with_default_values(initialize_test_database):
    """Test workflow with default variable values."""
    variables = [
        {
            "name": "limit",
            "display_name": "Result Limit",
            "variable_type": "number",
            "default_value": "10",
            "required": False
        }
    ]
    
    workflow_id = create_workflow(
        name="Limited Results",
        description="Query with default limit",
        steps=[{
            "step_order": 1,
            "prompt_template": "Show top {{limit}} results"
        }],
        variables=variables
    )
    
    workflow = get_workflow(workflow_id)
    
    assert workflow["variables"][0]["default_value"] == "10"
    assert workflow["variables"][0]["required"] is False


def test_workflow_not_found():
    """Test retrieving non-existent workflow."""
    workflow = get_workflow("00000000-0000-0000-0000-000000000000")
    assert workflow is None


def test_update_nonexistent_workflow():
    """Test updating non-existent workflow."""
    success = update_workflow(
        workflow_id="00000000-0000-0000-0000-000000000000",
        name="New Name"
    )
    assert success is False


def test_delete_nonexistent_workflow():
    """Test deleting non-existent workflow."""
    success = delete_workflow("00000000-0000-0000-0000-000000000000")
    assert success is False


# ============================================================================
# API Endpoint Tests
# ============================================================================

def test_api_create_workflow():
    """Test creating workflow via API."""
    payload = {
        "name": "API Test Workflow",
        "description": "Created via API",
        "steps": [
            {
                "step_order": 1,
                "name": "Step 1",
                "prompt_template": "Query with {{var1}}"
            }
        ],
        "variables": [
            {
                "name": "var1",
                "display_name": "Variable 1",
                "variable_type": "text",
                "required": True
            }
        ],
        "created_by": "api_test@example.com"
    }
    
    response = client.post("/workflows", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "workflow_id" in data
    assert data["message"] == "Workflow 'API Test Workflow' created successfully"


def test_api_list_workflows():
    """Test listing workflows via API."""
    response = client.get("/workflows?limit=10&offset=0")
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "workflows" in data
    assert isinstance(data["workflows"], list)


def test_api_get_workflow(initialize_test_database):
    """Test getting workflow details via API."""
    # Create a workflow first
    workflow_id = create_workflow(
        name="API Get Test",
        description="Test",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    response = client.get(f"/workflows/{workflow_id}")
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["workflow"]["name"] == "API Get Test"


def test_api_get_nonexistent_workflow():
    """Test getting non-existent workflow via API."""
    response = client.get("/workflows/00000000-0000-0000-0000-000000000000")
    
    assert response.status_code == 404


def test_api_update_workflow(initialize_test_database):
    """Test updating workflow via API."""
    workflow_id = create_workflow(
        name="Original",
        description="Original",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    payload = {
        "name": "Updated via API",
        "is_active": False
    }
    
    response = client.put(f"/workflows/{workflow_id}", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["workflow"]["name"] == "Updated via API"
    assert data["workflow"]["is_active"] is False


def test_api_delete_workflow(initialize_test_database):
    """Test deleting workflow via API."""
    workflow_id = create_workflow(
        name="To Delete",
        description="Test",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    response = client.delete(f"/workflows/{workflow_id}")
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_api_execute_workflow_missing_variables(initialize_test_database):
    """Test executing workflow with missing required variables."""
    workflow_id = create_workflow(
        name="Test Execution",
        description="Test",
        steps=[{"step_order": 1, "prompt_template": "Query {{required_var}}"}],
        variables=[{"name": "required_var", "required": True}]
    )
    
    payload = {
        "variables": {},  # Missing required_var
        "executed_by": "test@example.com"
    }
    
    response = client.post(f"/workflows/{workflow_id}/execute", json=payload)
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert "required_var" in data["error"]


# ============================================================================
# Workflow Execution Tests
# ============================================================================

@patch('app.domain.workflows.executor.query_database_with_agent')
def test_execute_workflow_success(mock_query, initialize_test_database):
    """Test successful workflow execution."""
    # Mock the query agent to return success
    mock_query.return_value = {
        "success": True,
        "response": "Query executed successfully",
        "executed_sql": "SELECT * FROM test",
        "data_csv": "col1,col2\nval1,val2",
        "rows_returned": 1,
        "execution_time_seconds": 0.5
    }
    
    # Create workflow
    workflow_id = create_workflow(
        name="Test Execution",
        description="Test",
        steps=[
            {
                "step_order": 1,
                "name": "Step 1",
                "prompt_template": "Get data for {{client}}"
            }
        ],
        variables=[
            {
                "name": "client",
                "variable_type": "text",
                "required": True
            }
        ]
    )
    
    # Execute workflow
    result = execute_workflow(
        workflow_id=workflow_id,
        variables={"client": "Acme Corp"},
        executed_by="test@example.com"
    )
    
    assert result["success"] is True
    assert result["status"] == "completed"
    assert len(result["step_results"]) == 1
    assert result["step_results"][0]["status"] == "success"
    assert result["step_results"][0]["executed_sql"] == "SELECT * FROM test"


@patch('app.domain.workflows.executor.query_database_with_agent')
def test_execute_workflow_with_context(mock_query, initialize_test_database):
    """Test workflow execution with context passing between steps."""
    # Mock returns different results for each step
    mock_query.side_effect = [
        {
            "success": True,
            "response": "Step 1 result",
            "executed_sql": "SELECT COUNT(*) FROM orders",
            "data_csv": "count\n100",
            "rows_returned": 1,
            "execution_time_seconds": 0.3
        },
        {
            "success": True,
            "response": "Step 2 result",
            "executed_sql": "SELECT * FROM orders LIMIT 10",
            "data_csv": "id,amount\n1,50\n2,75",
            "rows_returned": 2,
            "execution_time_seconds": 0.4
        }
    ]
    
    # Create workflow with multiple steps
    workflow_id = create_workflow(
        name="Multi-Step Test",
        description="Test context passing",
        steps=[
            {
                "step_order": 1,
                "name": "Count Orders",
                "prompt_template": "Count total orders for {{client}}"
            },
            {
                "step_order": 2,
                "name": "Get Order Details",
                "prompt_template": "Show order details for {{client}}"
            }
        ],
        variables=[{"name": "client", "required": True}]
    )
    
    # Execute with context enabled
    result = execute_workflow(
        workflow_id=workflow_id,
        variables={"client": "Acme"},
        include_context=True
    )
    
    assert result["success"] is True
    assert len(result["step_results"]) == 2
    
    # Verify both steps executed
    assert result["step_results"][0]["step_name"] == "Count Orders"
    assert result["step_results"][1]["step_name"] == "Get Order Details"
    
    # Verify context was passed (check that second call included context)
    second_call_args = mock_query.call_args_list[1][0][0]
    assert "Context from previous steps" in second_call_args


@patch('app.domain.workflows.executor.query_database_with_agent')
def test_execute_workflow_step_failure(mock_query, initialize_test_database):
    """Test workflow execution when a step fails."""
    # First step succeeds, second fails
    mock_query.side_effect = [
        {
            "success": True,
            "response": "Step 1 success",
            "executed_sql": "SELECT 1",
            "data_csv": "result\n1",
            "rows_returned": 1,
            "execution_time_seconds": 0.2
        },
        {
            "success": False,
            "error": "Query failed",
            "response": "Error executing query"
        }
    ]
    
    workflow_id = create_workflow(
        name="Failure Test",
        description="Test step failure",
        steps=[
            {"step_order": 1, "prompt_template": "Step 1"},
            {"step_order": 2, "prompt_template": "Step 2"}
        ],
        variables=[]
    )
    
    result = execute_workflow(
        workflow_id=workflow_id,
        variables={},
        executed_by="test@example.com"
    )
    
    # Workflow completes even with step failure
    assert result["success"] is True
    assert result["status"] == "completed"
    assert len(result["step_results"]) == 2
    
    # First step succeeded
    assert result["step_results"][0]["status"] == "success"
    
    # Second step failed
    assert result["step_results"][1]["status"] == "failed"


def test_execute_nonexistent_workflow():
    """Test executing non-existent workflow."""
    result = execute_workflow(
        workflow_id="00000000-0000-0000-0000-000000000000",
        variables={}
    )
    
    assert result["success"] is False
    assert "not found" in result["error"]


# ============================================================================
# Execution Status and Results Tests
# ============================================================================

def test_update_execution_status(initialize_test_database):
    """Test updating execution status."""
    workflow_id = create_workflow(
        name="Status Test",
        description="Test",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    execution_id = create_execution(
        workflow_id=workflow_id,
        variables_used={}
    )
    
    # Update to completed
    update_execution_status(execution_id, "completed")
    
    execution = get_execution(execution_id)
    assert execution["status"] == "completed"
    assert execution["completed_at"] is not None


def test_save_step_result(initialize_test_database):
    """Test saving step execution results."""
    workflow_id = create_workflow(
        name="Result Test",
        description="Test",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    workflow = get_workflow(workflow_id)
    step_id = workflow["steps"][0]["id"]
    
    execution_id = create_execution(
        workflow_id=workflow_id,
        variables_used={}
    )
    
    # Save step result
    save_step_result(
        execution_id=execution_id,
        step_id=step_id,
        step_order=1,
        executed_sql="SELECT 1",
        result_csv="result\n1",
        rows_returned=1,
        execution_time_seconds=0.5,
        status="success"
    )
    
    # Retrieve execution with results
    execution = get_execution(execution_id)
    
    assert len(execution["step_results"]) == 1
    assert execution["step_results"][0]["executed_sql"] == "SELECT 1"
    assert execution["step_results"][0]["rows_returned"] == 1
    assert execution["step_results"][0]["status"] == "success"


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================

def test_workflow_with_no_steps():
    """Test that workflow requires at least one step."""
    # This should be validated at API level
    payload = {
        "name": "No Steps",
        "steps": [],  # Empty steps
        "variables": []
    }
    
    response = client.post("/workflows", json=payload)
    
    # Should fail validation
    assert response.status_code in [400, 422]


def test_workflow_with_duplicate_step_orders(initialize_test_database):
    """Test workflow with duplicate step orders."""
    # Database constraint should prevent this
    with pytest.raises(Exception):
        create_workflow(
            name="Duplicate Steps",
            description="Test",
            steps=[
                {"step_order": 1, "prompt_template": "Step 1"},
                {"step_order": 1, "prompt_template": "Step 1 again"}  # Duplicate
            ],
            variables=[]
        )


def test_workflow_with_special_characters_in_variables():
    """Test variable substitution with special characters."""
    template = "Search for {{query}} in database"
    variables = {
        "query": "O'Reilly & Associates"
    }
    
    result = substitute_variables(template, variables)
    
    assert "O'Reilly & Associates" in result


def test_workflow_with_numeric_variables():
    """Test variable substitution with numbers."""
    template = "Get top {{limit}} results with threshold {{threshold}}"
    variables = {
        "limit": 10,
        "threshold": 0.95
    }
    
    result = substitute_variables(template, variables)
    
    assert "10" in result
    assert "0.95" in result


def test_list_workflows_pagination(initialize_test_database):
    """Test workflow list pagination."""
    # Create 5 workflows
    for i in range(5):
        create_workflow(
            name=f"Pagination Test {i}",
            description="Test",
            steps=[{"step_order": 1, "prompt_template": "Test"}],
            variables=[]
        )
    
    # Get first page
    page1 = list_workflows(limit=2, offset=0)
    assert len(page1) == 2
    
    # Get second page
    page2 = list_workflows(limit=2, offset=2)
    assert len(page2) == 2
    
    # Verify different results
    page1_ids = {wf["id"] for wf in page1}
    page2_ids = {wf["id"] for wf in page2}
    assert page1_ids != page2_ids


def test_list_executions_pagination(initialize_test_database):
    """Test execution list pagination."""
    workflow_id = create_workflow(
        name="Pagination Test",
        description="Test",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    # Create 5 executions
    for i in range(5):
        create_execution(
            workflow_id=workflow_id,
            variables_used={"var": f"value{i}"}
        )
    
    # Get first page
    page1 = list_executions(workflow_id=workflow_id, limit=2, offset=0)
    assert len(page1) == 2
    
    # Get second page
    page2 = list_executions(workflow_id=workflow_id, limit=2, offset=2)
    assert len(page2) == 2


def test_inactive_workflow_in_list(initialize_test_database):
    """Test that inactive workflows can be filtered."""
    # Create active workflow
    create_workflow(
        name="Active",
        description="Active workflow",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    
    # Create inactive workflow
    inactive_id = create_workflow(
        name="Inactive",
        description="Inactive workflow",
        steps=[{"step_order": 1, "prompt_template": "Test"}],
        variables=[]
    )
    update_workflow(inactive_id, is_active=False)
    
    # List only active
    active_workflows = list_workflows(active_only=True)
    active_names = [wf["name"] for wf in active_workflows]
    assert "Active" in active_names
    assert "Inactive" not in active_names
    
    # List all
    all_workflows = list_workflows(active_only=False)
    all_names = [wf["name"] for wf in all_workflows]
    assert "Active" in all_names
    assert "Inactive" in all_names


# ============================================================================
# LLM Workflow Generation Tests (Mocked)
# ============================================================================

@patch('app.domain.workflows.generator.create_workflow_generator_agent')
def test_generate_workflow_from_description(mock_agent):
    """Test LLM workflow generation."""
    # Mock the agent to return a workflow definition
    mock_agent_instance = MagicMock()
    mock_agent.return_value = mock_agent_instance
    
    # Create the expected workflow that will be set on the context
    expected_workflow = {
        "name": "Generated Workflow",
        "description": "Auto-generated",
        "steps": [
            {
                "step_order": 1,
                "prompt_template": "Get revenue for {{client}}"
            }
        ],
        "variables": [
            {
                "name": "client",
                "variable_type": "text",
                "required": True
            }
        ]
    }
    
    # Mock the invoke method to return success and populate the context
    def mock_invoke(inputs, context=None, config=None):
        # Simulate the agent populating the context
        if context:
            context.generated_workflow = expected_workflow
        return {
            "messages": [
                MagicMock(content="Generated workflow successfully")
            ]
        }
    
    mock_agent_instance.invoke.side_effect = mock_invoke
    
    result = generate_workflow_from_description(
        "Create a workflow for revenue analysis by client"
    )
    
    assert result["success"] is True
    assert "workflow" in result
    assert result["workflow"]["name"] == "Generated Workflow"


def test_api_generate_workflow():
    """Test workflow generation via API (mocked)."""
    with patch('app.domain.workflows.generator.generate_workflow_from_description') as mock_gen:
        mock_gen.return_value = {
            "success": True,
            "workflow": {
                "name": "API Generated",
                "description": "Test",
                "steps": [],
                "variables": []
            },
            "llm_response": "Workflow generated"
        }
        
        payload = {
            "description": "Create a revenue report workflow"
        }
        
        response = client.post("/workflows/generate", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "workflow" in data

"""
LLM-powered workflow generator.

This module uses LangChain agents to automatically generate workflows
from natural language descriptions.
"""
import json
import logging
import re
from typing import Any, Dict, List, Optional, Annotated
from typing_extensions import NotRequired

from langchain.agents import create_agent, AgentState
from langchain.tools import tool, ToolRuntime
from langchain_anthropic import ChatAnthropic
from langchain_core.tools import InjectedToolArg
from langgraph.checkpoint.memory import InMemorySaver
from langchain_core.runnables import RunnableConfig

from app.core.config import settings
from app.db.context import get_database_schema, format_schema_for_prompt

logger = logging.getLogger(__name__)


# Custom AgentState for workflow generation
class WorkflowGeneratorState(AgentState):
    """State for workflow generator agent."""
    workflow_description: NotRequired[str]
    generated_workflow: NotRequired[Dict[str, Any]]


@tool
def get_database_schema_tool() -> str:
    """Get comprehensive information about all tables and their schemas in the database."""
    try:
        schema_info = get_database_schema()
        return format_schema_for_prompt(schema_info)
    except Exception as e:
        return f"Error retrieving database schema: {str(e)}"


@tool
def finalize_workflow(
    name: str,
    description: str,
    steps: List[Dict[str, Any]],
    variables: List[Dict[str, Any]],
    runtime: Annotated[ToolRuntime[WorkflowGeneratorState], InjectedToolArg()]
) -> Dict[str, Any]:
    """
    Finalize the workflow definition.
    
    This tool should be called when you've completed your analysis and are ready
    to create the workflow. It records your workflow definition for creation.
    
    Args:
        name: Workflow name (e.g., "Monthly Revenue Report")
        description: Detailed description of what the workflow does
        steps: List of workflow steps, each with:
            - step_order: Integer order (1, 2, 3, ...)
            - name: Optional step name
            - prompt_template: LLM prompt with {{variable}} placeholders
        variables: List of variables, each with:
            - name: Variable name (e.g., "start_date", "client_name")
            - display_name: Human-readable name (e.g., "Start Date")
            - variable_type: Type (text, date, number, select)
            - default_value: Optional default value
            - options: For select type, list of options
            - required: Boolean (default True)
        
    Returns:
        Confirmation of workflow definition
    """
    context = runtime.context
    
    # Validate steps
    if not steps:
        return {"error": "At least one step is required"}
    
    for i, step in enumerate(steps):
        if "step_order" not in step:
            return {"error": f"Step {i} missing step_order"}
        if "prompt_template" not in step:
            return {"error": f"Step {i} missing prompt_template"}
    
    # Validate variables
    for i, var in enumerate(variables):
        if "name" not in var:
            return {"error": f"Variable {i} missing name"}
        
        # Validate variable type
        valid_types = ["text", "date", "number", "select"]
        var_type = var.get("variable_type", "text")
        if var_type not in valid_types:
            return {"error": f"Variable '{var['name']}' has invalid type '{var_type}'. Must be one of: {', '.join(valid_types)}"}
        
        # For select type, options are required
        if var_type == "select" and not var.get("options"):
            return {"error": f"Variable '{var['name']}' is type 'select' but has no options"}
    
    # Store workflow definition in context
    context.generated_workflow = {
        "name": name,
        "description": description,
        "steps": steps,
        "variables": variables
    }
    
    return {
        "success": True,
        "message": f"Workflow '{name}' defined successfully",
        "name": name,
        "step_count": len(steps),
        "variable_count": len(variables)
    }


# Global checkpointer for conversation memory
_workflow_generator_checkpointer = InMemorySaver()


def create_workflow_generator_agent():
    """Create a LangChain agent for workflow generation."""
    
    api_key = (settings.anthropic_api_key or "").strip()
    if not api_key:
        raise RuntimeError(
            "Anthropic API key not configured. Set ANTHROPIC_API_KEY in your environment."
        )
    
    model = ChatAnthropic(
        model="claude-haiku-4-5-20251001",
        api_key=api_key,
        temperature=0,
        max_tokens=4096
    )
    
    tools = [
        get_database_schema_tool,
        finalize_workflow
    ]
    
    system_prompt = """You are a workflow design expert helping users create automated data analysis workflows.

Your task is to analyze a user's request and generate a workflow - a series of LLM/SQL query steps with configurable variables.

**Workflow Structure:**
- **Steps**: Ordered sequence of LLM prompts that will generate SQL queries
- **Variables**: Configurable parameters (like dates, client names, filters) that users can change when executing the workflow

**Analysis Process:**
1. Call get_database_schema_tool to understand available tables and columns
2. Analyze the user's request to identify:
   - What data they want to query
   - What tables are involved
   - What should be configurable (variables)
   - What sequence of queries makes sense
3. Design workflow steps that:
   - Build on each other logically
   - Use clear, specific prompts
   - Include {{variable}} placeholders where values should be configurable
4. Identify variables that should be configurable:
   - Date ranges (start_date, end_date)
   - Filters (client_name, product_category, etc.)
   - Thresholds (min_revenue, top_n, etc.)
5. Call finalize_workflow with your complete workflow definition

**Variable Types:**
- **text**: Free-form text input (names, descriptions)
- **date**: Date picker (YYYY-MM-DD format)
- **number**: Numeric input (integers or decimals)
- **select**: Dropdown with predefined options

**Step Design Guidelines:**
- Each step should have a clear, specific prompt
- Use {{variable_name}} syntax for variables (e.g., "Get revenue between {{start_date}} and {{end_date}}")
- Steps can reference previous results (the executor will provide context)
- Keep prompts focused - one query per step
- Order steps logically (summary first, then details, then analysis)

**Example Workflow:**
User request: "Create a workflow for monthly revenue reports by client"

Generated workflow:
{
  "name": "Monthly Revenue Report",
  "description": "Generate comprehensive revenue analysis for a specific time period",
  "steps": [
    {
      "step_order": 1,
      "name": "Total Revenue",
      "prompt_template": "Calculate total revenue from invoices table between {{start_date}} and {{end_date}}"
    },
    {
      "step_order": 2,
      "name": "Revenue by Client",
      "prompt_template": "Show revenue breakdown by client for invoices between {{start_date}} and {{end_date}}, ordered by revenue descending"
    },
    {
      "step_order": 3,
      "name": "Top Clients",
      "prompt_template": "List the top {{top_n}} clients by revenue for the period {{start_date}} to {{end_date}}"
    }
  ],
  "variables": [
    {
      "name": "start_date",
      "display_name": "Start Date",
      "variable_type": "date",
      "required": true
    },
    {
      "name": "end_date",
      "display_name": "End Date",
      "variable_type": "date",
      "required": true
    },
    {
      "name": "top_n",
      "display_name": "Number of Top Clients",
      "variable_type": "number",
      "default_value": "10",
      "required": false
    }
  ]
}

**CRITICAL:** You MUST call finalize_workflow before providing your final response. Do not end your analysis without calling this tool.
"""
    
    agent = create_agent(
        model=model,
        tools=tools,
        system_prompt=system_prompt,
        state_schema=WorkflowGeneratorState,
        checkpointer=_workflow_generator_checkpointer
    )
    
    return agent


def generate_workflow_from_description(
    description: str,
    thread_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Generate a workflow from a natural language description.
    
    Args:
        description: Natural language description of desired workflow
        thread_id: Optional thread ID for conversation continuity
        
    Returns:
        Generated workflow definition or error
    """
    try:
        if thread_id is None:
            from uuid import uuid4
            thread_id = f"workflow-gen-{uuid4()}"
        
        # Create context
        from dataclasses import dataclass
        
        @dataclass
        class GeneratorContext:
            workflow_description: str
            generated_workflow: Optional[Dict[str, Any]] = None
        
        context = GeneratorContext(workflow_description=description)
        
        # Create agent
        agent = create_workflow_generator_agent()
        
        # Create config with thread_id
        config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
        
        # Run agent
        prompt = f"""Generate a workflow based on this description:

{description}

Please analyze the database schema, design appropriate steps and variables, then call finalize_workflow with your complete workflow definition."""
        
        result = agent.invoke(
            {"messages": [{"role": "user", "content": prompt}]},
            context=context,
            config=config
        )
        
        # Extract response
        final_message = result["messages"][-1]
        response_text = final_message.content if hasattr(final_message, 'content') else str(final_message)
        
        # Check if workflow was generated
        if context.generated_workflow:
            return {
                "success": True,
                "workflow": context.generated_workflow,
                "llm_response": response_text
            }
        else:
            return {
                "success": False,
                "error": "LLM did not generate a workflow definition",
                "llm_response": response_text
            }
        
    except Exception as e:
        logger.error(f"Error generating workflow: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

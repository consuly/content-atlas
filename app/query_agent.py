import re
import time
from typing import Dict, List, Any, Optional
from typing_extensions import TypedDict, NotRequired
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage, RemoveMessage
from langchain_anthropic import ChatAnthropic
from langchain.agents import create_agent, AgentState
from langchain.agents.middleware import SummarizationMiddleware, HumanInTheLoopMiddleware, before_model
from langchain.agents.structured_output import ToolStrategy
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.runtime import Runtime
from langchain_core.runnables import RunnableConfig
from .database import get_engine
from .db_context import get_database_schema, format_schema_for_prompt, get_related_tables
from .config import settings


# Custom AgentState using TypedDict (v1.0 requirement)
class DatabaseQueryState(AgentState):
    """Custom state for database query agent."""
    query_intent: NotRequired[str]  # What the user wants to query
    approved_queries: NotRequired[List[str]]  # Track approved SQL queries


# Structured output schema for database query results
class DatabaseQueryResult(TypedDict):
    """Structured response for database query results."""
    explanation: str  # What the query does
    sql_query: str  # The executed SQL
    execution_time_seconds: float  # How long it took
    rows_returned: int  # Number of rows
    csv_data: str  # CSV formatted results


@tool
def get_database_schema_tool() -> str:
    """Get comprehensive information about all tables and their schemas in the database."""
    try:
        schema_info = get_database_schema()
        return format_schema_for_prompt(schema_info)
    except Exception as e:
        return f"Error retrieving database schema: {str(e)}"


@tool
def get_related_tables_tool(query: str) -> str:
    """Analyze a query and suggest which tables might be relevant for JOINs or related data."""
    try:
        schema_info = get_database_schema()
        related = get_related_tables(query, schema_info)
        if related:
            return f"Potentially related tables for this query: {', '.join(related)}"
        else:
            return "No specific table relationships detected. You may need to examine the schema more carefully."
    except Exception as e:
        return f"Error analyzing related tables: {str(e)}"


@before_model
def trim_messages(state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
    """Keep only the last few messages to fit context window and maintain performance."""
    messages = state["messages"]
    
    # Keep at least 3 messages (system + user + assistant)
    if len(messages) <= 6:
        return None  # No changes needed
    
    # Keep the first message (system prompt with schema context)
    first_msg = messages[0]
    
    # Keep the last 5-6 conversation turns (10-12 messages)
    # Ensure we keep an even number to maintain user/assistant pairs
    recent_messages = messages[-10:] if len(messages) % 2 == 0 else messages[-11:]
    
    new_messages = [first_msg] + recent_messages
    
    return {
        "messages": [
            RemoveMessage(id=REMOVE_ALL_MESSAGES),
            *new_messages
        ]
    }


@tool
def execute_sql_query(sql_query: str) -> str:
    """
    Execute a SELECT SQL query safely and return results as CSV.

    Args:
        sql_query: The SQL query to execute. Must be a SELECT statement only.
    """
    try:
        # Security validation
        if not sql_query.strip().upper().startswith('SELECT'):
            return "ERROR: Only SELECT queries are allowed for security reasons."

        # Remove any dangerous keywords
        dangerous_patterns = [
            r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b',
            r';\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)',
            r'--.*',
            r'/\*.*?\*/'
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, sql_query, re.IGNORECASE | re.MULTILINE):
                return "ERROR: Query contains forbidden operations."

        engine = get_engine()

        start_time = time.time()
        with engine.connect() as conn:
            # Set timeout and limit results
            conn.execute(text("SET statement_timeout = '30000'"))  # 30 second timeout

            result = conn.execute(text(sql_query))
            columns = result.keys()

            # Limit to 1000 rows for performance
            rows = result.fetchmany(1000)
            execution_time = time.time() - start_time

            if not rows:
                return f"Query executed successfully in {execution_time:.2f}s. No results returned."

            # Convert to DataFrame for CSV formatting
            df = pd.DataFrame(rows, columns=columns)

            # Format as CSV string
            csv_output = df.to_csv(index=False)

            return f"""Query executed successfully in {execution_time:.2f}s.
Rows returned: {len(df)}
Columns: {', '.join(columns)}

CSV Data:
{csv_output}"""

    except Exception as e:
        return f"ERROR executing query: {str(e)}"


# Global checkpointer instance for conversation memory
_checkpointer = InMemorySaver()


def create_query_agent(system_prompt: str):
    """Create a LangChain v1.0 agent for natural language database queries with memory."""

    # Initialize LLM (Anthropic Claude)
    llm = ChatAnthropic(
        model="claude-haiku-4-5-20251001",
        api_key=settings.anthropic_api_key,
        temperature=0,  # Keep deterministic for SQL generation
        max_tokens=4096
    )

    # Define tools
    tools = [
        get_database_schema_tool,
        get_related_tables_tool,
        execute_sql_query
    ]

    # Create the agent with v1.0 features including memory and message trimming
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prompt,
        state_schema=DatabaseQueryState,
        checkpointer=_checkpointer,  # Enable conversation memory
        middleware=[trim_messages]  # Trim old messages to manage context window
    )

    return agent


def query_database_with_agent(user_prompt: str, thread_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Process a natural language query using the LangChain v1.0 agent with conversation memory.

    Args:
        user_prompt: The natural language query from the user
        thread_id: Optional thread ID for conversation continuity. If not provided, uses "default".

    Returns:
        Dict containing response data with structured output
    """
    try:
        # Use default thread if none provided
        if thread_id is None:
            thread_id = "default"
        
        # System prompt with context
        system_prompt = f"""You are an expert SQL analyst helping users query their PostgreSQL database.

You can remember previous queries and results in this conversation, so users can refer back to them.

First, use the get_database_schema_tool to understand the available tables and their structure.

When generating SQL queries:
- Use proper PostgreSQL syntax
- Include appropriate JOINs when multiple tables are needed
- Use aggregate functions (SUM, AVG, COUNT, etc.) when requested
- Add WHERE clauses for filtering
- Use ORDER BY and LIMIT for sorting and pagination
- Always use double quotes for table and column names to handle special characters
- Generate efficient queries following database best practices

If the query is ambiguous, ask for clarification using the get_related_tables_tool to understand relationships.

Always execute the final query using execute_sql_query and return the results in structured format.

IMPORTANT: After executing a query with execute_sql_query, provide a final structured summary of what was done."""

        agent = create_query_agent(system_prompt)

        # Prepare messages (just the user message since system prompt is now in agent)
        messages = [HumanMessage(content=user_prompt)]

        # Create config with thread_id for conversation continuity
        config: RunnableConfig = {"configurable": {"thread_id": thread_id}}

        # Run the agent with the config to enable memory
        result = agent.invoke({"messages": messages}, config)

        # Extract data from v1.0 agent tool calls and responses
        executed_sql = None
        csv_data = None
        execution_time = None
        rows_returned = None

        # Parse through all messages to find tool calls and responses
        for message in result["messages"]:
            if hasattr(message, 'tool_calls') and message.tool_calls:
                # This is an AI message with tool calls
                for tool_call in message.tool_calls:
                    if tool_call.get('name') == 'execute_sql_query':
                        # Extract SQL from tool call arguments
                        sql_args = tool_call.get('args', {})
                        if 'sql_query' in sql_args:
                            executed_sql = sql_args['sql_query'].strip()
            elif hasattr(message, 'name') and message.name == 'execute_sql_query':
                # This is a tool response from execute_sql_query
                tool_content = message.content

                # Extract CSV data
                csv_match = re.search(r'CSV Data:\s*(.*?)(?=\n\n|$)', tool_content, re.DOTALL)
                if csv_match:
                    csv_data = csv_match.group(1).strip()

                # Extract execution time
                time_match = re.search(r'(\d+\.\d+)s', tool_content)
                if time_match:
                    execution_time = float(time_match.group(1))

                # Extract rows returned
                rows_match = re.search(r'Rows returned: (\d+)', tool_content)
                if rows_match:
                    rows_returned = int(rows_match.group(1))

        # Get the final human-readable response
        final_message = result["messages"][-1].content
        if isinstance(final_message, list):
            # Handle content blocks (v1.0 format)
            final_response = ""
            for block in final_message:
                if block.get('type') == 'text':
                    final_response += block.get('text', '')
        else:
            final_response = str(final_message)

        return {
            "success": True,
            "response": final_response,
            "executed_sql": executed_sql,
            "data_csv": csv_data,
            "execution_time_seconds": execution_time,
            "rows_returned": rows_returned
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "response": f"An error occurred while processing your query: {str(e)}"
        }

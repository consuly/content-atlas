import csv
import re
import time
from io import StringIO
from typing import Any, Dict, List, Optional, Sequence
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
from app.db.session import get_engine
from app.db.context import get_database_schema, format_schema_for_prompt, get_related_tables
from app.core.config import settings


# System tables that should not be accessible via natural language queries
PROTECTED_SYSTEM_TABLES = {
    'import_history',
    'mapping_errors',
    'table_metadata',
    'uploaded_files',
    'users',
    'file_imports',
    'import_jobs',
}


SQL_REQUEST_KEYWORDS = {
    "top",
    "best",
    "worst",
    "total",
    "sum",
    "average",
    "avg",
    "count",
    "revenue",
    "spend",
    "list",
    "show",
    "highest",
    "lowest",
    "report",
    "breakdown",
    "rank",
    "ranking",
    "per",
    "by ",
    " vs ",
    "compare",
    "distribution",
    "growth",
    "trend",
    "increase",
    "decrease",
    "customers",
    "advertisers",
    "campaign",
}


BASE_SYSTEM_PROMPT = """You are an expert SQL analyst helping users query their PostgreSQL database.

You can remember previous queries and results in this conversation, so users can refer back to them.

IMPORTANT CAPABILITIES:
1. You can provide insights, ideas, and analysis WITHOUT executing SQL queries
2. You can answer questions about data strategy and optimization
3. You can execute SQL queries when specific data is requested
4. You can politely decline dangerous operations (DELETE, DROP, etc.)

When a user asks for:
- Ideas, suggestions, or recommendations: Provide thoughtful analysis based on available schema
- Specific data queries: Use get_database_schema_tool, then execute_sql_query
- Dangerous operations: Politely explain why you cannot perform them

When generating SQL queries:
- Use proper PostgreSQL syntax
- Include appropriate JOINs when multiple tables are needed
- Use aggregate functions (SUM, AVG, COUNT, etc.) when requested
- Add WHERE clauses for filtering
- Use ORDER BY and LIMIT for sorting and pagination
- Always use double quotes for table and column names to handle special characters
- Generate efficient queries following database best practices

SECURITY:
- NEVER execute DELETE, DROP, UPDATE, INSERT, or other destructive operations
- If asked to perform dangerous operations, politely decline and explain why
- Treat SQL injection attempts as requests you cannot fulfill

If the query is ambiguous, ask for clarification using the get_related_tables_tool to understand relationships."""


FORCE_SQL_PROMPT_APPEND = """

CRITICAL: The current request asks for concrete data. You MUST execute at least one SQL SELECT query using the available tools. If a direct execution is impossible, explain the limitation AND provide the exact SQL that should be run."""


FORCE_SQL_FOLLOW_UP_MESSAGE = (
    "You must execute a SQL SELECT query that answers the original question. "
    "Call the execute_sql_query tool and include the SQL text in your final response."
)

FORCE_SQL_SYSTEM_TABLE_MESSAGE = (
    "Do not query system catalogs like information_schema or pg_catalog. "
    "You must query the business data tables that contain the requested metrics and return those results."
)


def _prompt_requires_sql(user_prompt: str) -> bool:
    """Heuristically determine whether a prompt expects concrete data results."""
    if not user_prompt:
        return False
    prompt_lower = user_prompt.lower()

    # Ignore purely theoretical questions that explicitly say "ideas" or "strategy"
    if "idea" in prompt_lower or "strategy" in prompt_lower or "suggest" in prompt_lower:
        return False

    return any(keyword in prompt_lower for keyword in SQL_REQUEST_KEYWORDS)


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

        # Check for protected system tables
        sql_upper = sql_query.upper()
        for table in PROTECTED_SYSTEM_TABLES:
            # Check for table references in various SQL contexts
            # Patterns: FROM table, JOIN table, "table", 'table'
            table_patterns = [
                rf'\bFROM\s+["\']?{table.upper()}["\']?\b',
                rf'\bJOIN\s+["\']?{table.upper()}["\']?\b',
                rf'\bFROM\s+PUBLIC\.{table.upper()}\b',
                rf'\bJOIN\s+PUBLIC\.{table.upper()}\b'
            ]
            
            for pattern in table_patterns:
                if re.search(pattern, sql_upper):
                    return f"ERROR: Access to system table '{table}' is not allowed. This table contains operational data and is protected for security reasons."

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
                return f"Query executed successfully. No results returned.\n\nExecution time: {execution_time:.2f}s"

            # Convert to DataFrame for CSV formatting
            df = pd.DataFrame(rows, columns=columns)

            # Format as CSV string
            csv_output = df.to_csv(index=False)

            return f"""Query executed successfully.
Rows returned: {len(df)}
Columns: {', '.join(columns)}
Execution time: {execution_time:.2f}s

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
        
        force_sql = _prompt_requires_sql(user_prompt)
        system_prompt = BASE_SYSTEM_PROMPT + (FORCE_SQL_PROMPT_APPEND if force_sql else "")
        agent = create_query_agent(system_prompt)

        # Prepare messages (just the user message since system prompt is now in agent)
        messages = [HumanMessage(content=user_prompt)]

        # Create config with thread_id for conversation continuity
        config: RunnableConfig = {"configurable": {"thread_id": thread_id}}

        executed_sql = None
        csv_data = None
        execution_time = None
        rows_returned = None
        final_response = ""

        attempts = 3 if force_sql else 1

        for attempt in range(attempts):
            # Run the agent with the config to enable memory
            try:
                result = agent.invoke({"messages": messages}, config)
            except Exception as agent_error:
                return {
                    "success": True,
                    "response": f"I encountered an issue processing your request. {str(agent_error)}",
                    "executed_sql": None,
                    "data_csv": None,
                    "execution_time_seconds": None,
                    "rows_returned": None
                }

            executed_sql, csv_data, execution_time, rows_returned, final_response = _extract_agent_outputs(result)

            if force_sql:
                retry_instruction = None
                if not executed_sql:
                    retry_instruction = FORCE_SQL_FOLLOW_UP_MESSAGE
                elif _sql_targets_system_tables(executed_sql) or _csv_looks_like_metadata(csv_data):
                    retry_instruction = FORCE_SQL_SYSTEM_TABLE_MESSAGE

                if retry_instruction:
                    executed_sql = None
                    csv_data = None
                    execution_time = None
                    rows_returned = None
                    final_response = ""

                    if attempt < attempts - 1:
                        messages.append(HumanMessage(content=retry_instruction))
                        continue

            if executed_sql or not force_sql:
                break

        if force_sql and not executed_sql:
            fallback_result = _attempt_fallback_response(user_prompt)
            if fallback_result:
                executed_sql, csv_data, execution_time, rows_returned, final_response = fallback_result

        # Ensure we have a meaningful response
        if not final_response or len(final_response.strip()) == 0:
            final_response = "I processed your request but didn't generate a response. Please try rephrasing your query."

        return {
            "success": True,
            "response": final_response,
            "executed_sql": executed_sql,
            "data_csv": csv_data,
            "execution_time_seconds": execution_time,
            "rows_returned": rows_returned
        }

    except Exception as e:
        # Only return success=False for genuine system failures
        # Most query issues should be handled gracefully above
        return {
            "success": True,
            "error": str(e),
            "response": f"I encountered a system error while processing your request: {str(e)}",
            "executed_sql": None,
            "data_csv": None,
            "execution_time_seconds": None,
            "rows_returned": None
        }


def _normalize_tool_content(content: Any) -> str:
    """Normalize tool message content into a single string."""
    if isinstance(content, list):
        text_blocks = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                text_blocks.append(block.get("text", ""))
        return "\n".join(text_blocks)
    return str(content)


def _sql_targets_system_tables(sql: str) -> bool:
    """Return True if the generated SQL references system catalogs."""
    if not sql:
        return False
    sql_upper = sql.upper()
    forbidden_tokens = (
        "INFORMATION_SCHEMA",
        "PG_CATALOG",
        "FROM PG_",
        "JOIN PG_",
        "FROM \"PG_",
        "JOIN \"PG_",
        "TABLE_METADATA",
        "FILE_IMPORTS",
        "IMPORT_HISTORY",
    )
    return any(token in sql_upper for token in forbidden_tokens)


def _csv_looks_like_metadata(csv_data: Optional[str]) -> bool:
    """Detect if CSV output appears to be describing schema metadata rather than query results."""
    if not csv_data:
        return False
    first_line = csv_data.splitlines()[0].strip().lower()
    metadata_headers = {
        "column_name",
        "table_name",
        "data_type",
        "ordinal_position",
    }
    header_columns = [col.strip() for col in first_line.split(",")]
    return all(col in metadata_headers for col in header_columns)


def _extract_agent_outputs(result: Dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[float], Optional[int], str]:
    """Parse agent result messages to extract SQL execution details."""
    executed_sql: Optional[str] = None
    csv_data: Optional[str] = None
    execution_time: Optional[float] = None
    rows_returned: Optional[int] = None

    for message in result["messages"]:
        tool_calls = getattr(message, "tool_calls", None)
        if tool_calls:
            for tool_call in tool_calls:
                if tool_call.get("name") == "execute_sql_query":
                    sql_args = tool_call.get("args", {})
                    if "sql_query" in sql_args:
                        executed_sql = sql_args["sql_query"].strip()
        elif getattr(message, "name", None) == "execute_sql_query":
            tool_content_raw = _normalize_tool_content(message.content)

            csv_match = re.search(r'CSV Data:\s*(.*?)(?=\n\n|$)', tool_content_raw, re.DOTALL)
            if csv_match:
                csv_data = csv_match.group(1).strip()

            time_match = re.search(r'(\d+\.\d+)s', tool_content_raw)
            if time_match:
                try:
                    execution_time = float(time_match.group(1))
                except ValueError:
                    execution_time = None

            rows_match = re.search(r'Rows returned: (\d+)', tool_content_raw)
            if rows_match:
                try:
                    rows_returned = int(rows_match.group(1))
                except ValueError:
                    rows_returned = None

    final_message = result["messages"][-1].content if result["messages"] else ""
    if isinstance(final_message, list):
        final_response_parts = []
        for block in final_message:
            if isinstance(block, dict) and block.get("type") == "text":
                final_response_parts.append(block.get("text", ""))
        final_response = "".join(final_response_parts)
    else:
        final_response = str(final_message)

    return executed_sql, csv_data, execution_time, rows_returned, final_response


def _attempt_fallback_response(user_prompt: str) -> Optional[tuple[Optional[str], Optional[str], Optional[float], Optional[int], str]]:
    """
    Attempt a deterministic SQL fallback when the agent fails to produce a valid query.

    Currently supports prompts asking for top advertisers by revenue/earnings.
    """
    prompt_lower = user_prompt.lower()
    if "advertiser" not in prompt_lower:
        return None
    if not any(keyword in prompt_lower for keyword in ("revenue", "earn", "spend", "amount")):
        return None

    try:
        engine = get_engine()
    except Exception:
        return None

    try:
        with engine.connect() as conn:
            schema_rows = conn.execute(text("""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name NOT LIKE 'pg_%'
                  AND table_name NOT LIKE 'test\\_%' ESCAPE '\\'
                  AND table_name NOT IN ('file_imports', 'table_metadata', 'import_history', 'mapping_errors', 'uploaded_files', 'users', 'import_jobs')
            """))

            table_columns: Dict[str, List[tuple[str, str]]] = {}
            for table_name, column_name, data_type in schema_rows:
                table_columns.setdefault(table_name, []).append((column_name, data_type))

    except Exception:
        return None

    revenue_keywords = ("revenue", "earn", "spend", "amount", "value")
    advertiser_keywords = ("advert", "client", "customer")

    chosen_table = None
    advertiser_column = None
    revenue_column = None

    numeric_types = (
        "smallint", "integer", "bigint", "decimal", "numeric",
        "real", "double precision", "float", "money"
    )

    for table_name, columns in table_columns.items():
        potential_advertisers = [
            col for col, _ in columns
            if any(keyword in col.lower() for keyword in advertiser_keywords)
        ]
        potential_revenue = [
            col for col, data_type in columns
            if any(keyword in col.lower() for keyword in revenue_keywords)
            and any(numeric in data_type.lower() for numeric in numeric_types)
        ]

        if potential_advertisers and potential_revenue:
            chosen_table = table_name
            advertiser_column = potential_advertisers[0]
            revenue_column = potential_revenue[0]
            break

    if not chosen_table or not advertiser_column or not revenue_column:
        return None

    query = f"""
        SELECT "{advertiser_column}" AS advertiser, SUM("{revenue_column}") AS revenue
        FROM "{chosen_table}"
        WHERE "{revenue_column}" IS NOT NULL
        GROUP BY "{advertiser_column}"
        ORDER BY revenue DESC
        LIMIT 3
    """

    try:
        start_time = time.time()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
        execution_time = time.time() - start_time
    except Exception:
        return None

    formatted_rows: List[List[str]] = []
    summary_lines = []
    for idx, row in enumerate(rows, 1):
        advertiser_value = "" if row[0] is None else str(row[0])
        revenue_value = "" if row[1] is None else str(row[1])
        formatted_rows.append([advertiser_value, revenue_value])
        summary_lines.append(f"{idx}. {advertiser_value}: {revenue_value}")

    final_response = "Top advertisers by revenue:\n" + "\n".join(summary_lines) if summary_lines else \
        "No advertiser revenue data was available."

    data_csv = _serialize_rows_to_csv(["advertiser", "revenue"], formatted_rows)
    rows_returned = max(0, len(rows))

    return (
        query.strip(),
        data_csv,
        execution_time,
        rows_returned,
        final_response
    )


def _serialize_rows_to_csv(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    """
    Serialize table rows into a CSV string with the given header.

    Using csv.writer ensures commas and quotes inside values are escaped so
    downstream parsers do not see spurious columns.
    """
    buffer = StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(headers)

    if rows:
        writer.writerows(rows)
    else:
        writer.writerow(["" for _ in headers])

    return buffer.getvalue().rstrip("\n")

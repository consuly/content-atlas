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
from app.db.context import (
    get_database_schema, 
    format_schema_for_prompt, 
    get_related_tables,
    get_table_names,
    format_table_list_for_prompt
)
from app.core.config import settings
from app.domain.queries.charting import build_chart_suggestion


# System tables that should not be accessible via natural language queries.
# Keep this list in sync with tests that assert protection of core system tables.
PROTECTED_SYSTEM_TABLES = {
    'import_history',
    'mapping_errors',
    'table_metadata',
    'uploaded_files',
    'users',
    'file_imports',
    'import_jobs',
    'llm_instructions',
    'table_fingerprints',
    'row_updates',
}

# Extended list used for strict SQL validation inside the agent; covers additional
# operational tables that should remain hidden from end users.
_EXTENDED_PROTECTED_TABLES = PROTECTED_SYSTEM_TABLES | {
    'api_keys',
    'import_duplicates',
    'query_messages',
    'query_threads',
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
    "merge",
    "combine",
    "union",
    "records from",
    "rows from",
}


BASE_SYSTEM_PROMPT = """You are an expert SQL analyst helping users query their PostgreSQL database.

You can remember previous queries and results in this conversation, so users can refer back to them.

IMPORTANT CAPABILITIES:
1. You can provide insights, ideas, and analysis WITHOUT executing SQL queries
2. You can answer questions about data strategy and optimization
3. You can execute SQL queries when specific data is requested
4. You can politely decline dangerous operations (DELETE, DROP, etc.)

QUERY STRATEGY (LAZY LOADING):
To optimize performance and accuracy, follow this 3-step process for every new query:
1. **Discovery**: Call `list_tables_tool` to see what tables are available.
2. **Schema Analysis**: Based on the user's request and the table list, identify the 1-3 most relevant tables and call `get_table_schema_tool(table_names=[...])` to get their detailed structure.
3. **Execution**: Once you have the schema, construct and run the SQL query using `execute_sql_query`.

When a user asks for:
- Ideas, suggestions, or recommendations: Provide thoughtful analysis based on available schema (fetch schema if needed)
- Specific data queries: Follow the Discovery -> Schema -> Execution flow.
- Dangerous operations: Politely explain why you cannot perform them

When generating SQL queries:
- Use proper PostgreSQL syntax
- Include appropriate JOINs when multiple tables are needed
- Use aggregate functions (SUM, AVG, COUNT, etc.) when requested
- Add WHERE clauses for filtering
- Use ORDER BY and LIMIT for sorting and pagination
- Always use double quotes for table and column names to handle special characters
- Generate efficient queries following database best practices
- NEVER select or reference system columns that start with underscore (_row_id, _import_id, _imported_at, _source_row_number, _corrections_applied)
- Only query user data columns - system metadata columns are for internal use only

COMBINING DATA FROM MULTIPLE TABLES:
**When to use JOIN vs UNION:**
- Use **JOIN** when tables are related via foreign keys or shared identifiers (e.g., orders → customers)
- Use **UNION/UNION ALL** when combining similar records from independent tables (e.g., merging contact lists, combining data from different sources)

**UNION Pattern for Merging Records:**
When a user requests "X records from table A AND Y records from table B" or wants to combine/merge data from multiple tables:
1. Identify common columns across tables (or use aliases to standardize column names)
2. Create separate SELECT statements for each table with appropriate WHERE/LIMIT clauses
3. Combine using UNION ALL (preserves all records including duplicates) or UNION (removes duplicates)
4. Wrap each SELECT in parentheses for clarity

**Example - Merging 100 records from each table:**
```sql
(SELECT "name", "email", "company", "phone" FROM "clients-list" 
 WHERE "email" IS NOT NULL LIMIT 100)
UNION ALL
(SELECT "name", "email", "company", "phone" FROM "competitors-list" 
 WHERE "email" IS NOT NULL LIMIT 100);
```

**Example - Combining with column aliasing when names differ:**
```sql
(SELECT "contact_name" AS "name", "email_address" AS "email", "org" AS "company" 
 FROM "table-a" LIMIT 500)
UNION ALL
(SELECT "full_name" AS "name", "email" AS "email", "company_name" AS "company" 
 FROM "table-b" LIMIT 500);
```

**Key Rules for UNION:**
- All SELECT statements must have the same number of columns
- Columns must be in the same order
- Column data types must be compatible
- Use UNION ALL unless you specifically need to remove duplicates (UNION is slower)
- If column names differ, use AS aliases to standardize the output
- **CRITICAL: When using LIMIT with UNION, you MUST wrap each SELECT in parentheses**
  - WRONG: `SELECT ... LIMIT 100 UNION ALL SELECT ... LIMIT 100` ❌ (syntax error!)
  - RIGHT: `(SELECT ... LIMIT 100) UNION ALL (SELECT ... LIMIT 100)` ✅
  - PostgreSQL requires parentheses when LIMIT appears before UNION/UNION ALL

CRITICAL PostgreSQL CONSTRAINTS:
1. **Numeric Type Selection**: Choose appropriate numeric types based on expected value ranges:
   - **INTEGER**: For numbers up to ~2.1 billion (2,147,483,647)
   - **BIGINT**: For large numbers like web traffic, social media metrics, financial amounts (up to 9 quintillion)
   - **NUMERIC/DECIMAL**: For precise decimal values or very large numbers
   - **When in doubt, use BIGINT** for any metric that could exceed 2 billion (visits, impressions, revenue, etc.)
   - Common columns that need BIGINT: traffic counts, website visits, social metrics, large monetary values

2. **Type Casting for COALESCE and Numeric Operations**: PostgreSQL does NOT automatically cast text to numeric types. You MUST explicitly cast when:
   - Using COALESCE with numeric defaults: `COALESCE(CAST("text_column" AS INTEGER), 0)` or `COALESCE("text_column"::INTEGER, 0)`
   - Performing math operations: `CAST("amount_text" AS DECIMAL) * 1.5`
   - Using numeric comparisons in WHERE/ORDER BY: `ORDER BY CAST("count_text" AS INTEGER) DESC`
   - **WRONG**: `COALESCE("assets_under_management", 0)`
   - **RIGHT**: `COALESCE(CAST("assets_under_management" AS DECIMAL), 0)`
   - **RIGHT**: `COALESCE("total_investments_count"::BIGINT, 0)` (use BIGINT for large counts)
   - Always check the schema - if a column is VARCHAR/TEXT but contains numbers, you MUST cast it

3. **SELECT DISTINCT + ORDER BY**: When using SELECT DISTINCT, ALL expressions in ORDER BY MUST appear in the SELECT clause
   - WRONG: SELECT DISTINCT "name" FROM table ORDER BY "age"
   - RIGHT: SELECT DISTINCT "name", "age" FROM table ORDER BY "age"
   - Alternative: Use subqueries or remove DISTINCT if you need to order by non-selected columns

4. **Column Name Verification**: ALWAYS verify column names exist in the schema before generating SQL
   - Use get_database_schema_tool to see exact column names
   - Table names may contain hyphens (e.g., "clients-list") - use double quotes
   - Column names are case-sensitive in PostgreSQL when quoted
   - If a column doesn't exist, check the schema for similar names

5. **Learning from Errors**: If a query fails with a PostgreSQL error:
   - **"integer out of range"**: The value exceeds INTEGER limit - use BIGINT instead
   - **"invalid input syntax for type numeric"**: Text has formatting (commas, $, etc.) - use REPLACE() to strip formatting before CAST
   - **"column does not exist"**: Verify exact column name from schema with get_table_schema_tool
   - Analyze the error message, identify the fix, and generate a corrected query

6. **Common Mistakes to Avoid**:
   - Don't assume column names - check the schema first
   - Don't mix DISTINCT with complex ORDER BY without including ORDER BY columns in SELECT
   - Don't forget to quote table/column names with special characters
   - Don't use text columns in numeric operations without CAST() - this causes "COALESCE types text and integer cannot be matched" errors
   - Don't use INTEGER for large numeric values (>2 billion) - use BIGINT

SECURITY:
- NEVER execute DELETE, DROP, UPDATE, INSERT, or other destructive operations
- NEVER access, list, or mention system tables (users, api_keys, file_imports, import_history, import_duplicates, import_jobs, mapping_errors, table_metadata, uploaded_files, query_messages, query_threads); restrict all queries to customer data tables only
- NEVER select system columns starting with underscore (_) - these are internal metadata columns
- If asked to perform dangerous operations or touch protected tables, politely decline and explain why
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
def list_tables_tool() -> str:
    """
    List all available tables in the database with brief descriptions.
    Use this FIRST to identify which tables might contain the data you need.
    
    Note: Temporary tables are hidden by default. If a user explicitly mentions
    a specific table name, use get_table_schema_tool with that name to access it.
    """
    try:
        tables = get_table_names(include_temporary=False)
        return format_table_list_for_prompt(tables)
    except Exception as e:
        return f"Error listing tables: {str(e)}"


@tool
def get_table_schema_tool(table_names: List[str]) -> str:
    """
    Get detailed schema (columns, types, samples) for specific tables.
    Use this AFTER identifying relevant tables with list_tables_tool.
    
    Args:
        table_names: List of table names to fetch schema for (e.g., ["orders", "customers"])
    
    Note: This tool will include temporary tables if they are explicitly named,
    even though they don't appear in list_tables_tool.
    """
    try:
        # When specific tables are requested, temporary tables are automatically included
        # if they're in the list (handled by get_database_schema logic)
        schema_info = get_database_schema(table_names=table_names)
        return format_schema_for_prompt(schema_info)
    except Exception as e:
        return f"Error retrieving schema: {str(e)}"


@tool
def get_related_tables_tool(query: str) -> str:
    """Analyze a query and suggest which tables might be relevant for JOINs or related data."""
    try:
        # For this tool, we still need the full schema logic internally to analyze relationships,
        # but we don't return the full schema to the LLM.
        schema_info = get_database_schema() 
        related = get_related_tables(query, schema_info)
        if related:
            return f"Potentially related tables for this query: {', '.join(related)}"
        else:
            return "No specific table relationships detected. You may need to examine the schema more carefully."
    except Exception as e:
        return f"Error analyzing related tables: {str(e)}"


def _message_has_tool_result(msg: Any) -> bool:
    """Check if a message contains a tool_result block."""
    content = getattr(msg, 'content', None)
    if isinstance(content, list):
        return any(
            isinstance(block, dict) and block.get('type') == 'tool_result'
            for block in content
        )
    return False


def _message_has_tool_use(msg: Any) -> bool:
    """Check if a message contains a tool_use block."""
    content = getattr(msg, 'content', None)
    if isinstance(content, list):
        return any(
            isinstance(block, dict) and block.get('type') == 'tool_use'
            for block in content
        )
    # Also check tool_calls attribute (LangChain format)
    tool_calls = getattr(msg, 'tool_calls', None)
    return bool(tool_calls)


def _trim_messages_impl(messages: List[Any]) -> tuple[int, Any] | None:
    """
    Core implementation of message trimming logic.
    Returns (cut_index, first_message) or None if no trimming needed.
    
    This is separated from the middleware decorator to allow for unit testing.
    """
    # Keep at least 6 messages to avoid trimming too aggressively if small
    if len(messages) <= 6:
        return None  # No changes needed
    
    # Always keep the first message (system prompt/context)
    first_msg = messages[0]
    
    # --- Token-Based Trimming Logic ---
    # Approximate token count (4 characters per token is a safe upper bound estimate)
    MAX_CONTEXT_TOKENS = 120000 
    
    def estimate_tokens(msg):
        content = getattr(msg, 'content', "") or ""
        if isinstance(content, list):
            text_content = ""
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text_content += block.get("text", "")
            content = text_content
        return len(str(content)) // 4

    current_tokens = 0
    # Calculate total tokens (roughly)
    for m in messages:
        current_tokens += estimate_tokens(m)
        
    # --- Logic ---
    
    # Target: keep last ~10 messages, but adjust to respect tool boundaries
    target_keep = 10
    cut_index = max(1, len(messages) - target_keep)
    
    # If we are over token limit, be more aggressive with cut_index
    if current_tokens > MAX_CONTEXT_TOKENS:
        # Walk backwards from end, accumulating tokens until we hit limit
        tokens_so_far = 0
        new_cut_index = len(messages)
        
        # Always reserve tokens for first message
        first_msg_tokens = estimate_tokens(first_msg)
        budget = MAX_CONTEXT_TOKENS - first_msg_tokens
        
        for i in range(len(messages) - 1, 0, -1):
            msg_tokens = estimate_tokens(messages[i])
            if tokens_so_far + msg_tokens > budget:
                new_cut_index = i + 1
                break
            tokens_so_far += msg_tokens
            new_cut_index = i
            
        # Use the more aggressive cut index
        cut_index = max(cut_index, new_cut_index)

    # CRITICAL FIX: Walk forward from cut_index to find a safe boundary
    # We must ensure we never orphan a tool_result without its tool_use
    max_iterations = len(messages)  # Safety: prevent infinite loops
    iteration_count = 0
    
    while cut_index < len(messages) and iteration_count < max_iterations:
        iteration_count += 1
        msg = messages[cut_index]
        
        # If the message at cut_index is a tool_result, we need to find its tool_use
        if _message_has_tool_result(msg):
            # Walk backwards to find the corresponding tool_use message
            found_tool_use = False
            for search_index in range(cut_index - 1, 0, -1):
                if _message_has_tool_use(messages[search_index]):
                    # Found the tool_use - we must cut BEFORE it to keep the pair together
                    cut_index = search_index
                    found_tool_use = True
                    break
            
            # If we couldn't find a tool_use (shouldn't happen), skip this message
            if not found_tool_use:
                cut_index += 1
                continue
            # After adjusting cut_index backward, continue the loop to re-check the new position
            continue
        
        # If the message at cut_index is a tool_use, check if there's a tool_result after it
        elif _message_has_tool_use(msg):
            # Look forward to see if there's a corresponding tool_result
            has_tool_result_after = False
            for search_index in range(cut_index + 1, len(messages)):
                if _message_has_tool_result(messages[search_index]):
                    has_tool_result_after = True
                    break
                # Stop searching if we hit another tool_use or the end
                if _message_has_tool_use(messages[search_index]):
                    break
            
            # If there's a tool_result after this tool_use, both will be kept together
            # (since we keep from cut_index onwards), so this is a safe cut point
            if has_tool_result_after:
                break
            # If no tool_result follows, we can also cut here safely  
            break
        else:
            # This message is neither tool_use nor tool_result, so it's a safe cut point
            break
        
        # Move to the next potential cut point
        cut_index += 1
        
        # Safety check: don't go past the messages
        if cut_index >= len(messages) - 1:
            break
    
    # Ensure we don't cut too much (keep at least 3 messages after first)
    if len(messages) - cut_index < 3:
        cut_index = max(1, len(messages) - 3)
        
        # ADDITIONAL SAFETY: After adjusting to keep at least 3 messages,
        # verify we didn't create an orphaned tool_result
        safety_iterations = 0
        max_safety_iterations = 10  # Prevent infinite loop
        
        while cut_index < len(messages) - 3 and safety_iterations < max_safety_iterations:
            safety_iterations += 1
            
            if _message_has_tool_result(messages[cut_index]):
                # Find its tool_use and include it
                original_cut_index = cut_index
                for search_index in range(cut_index - 1, 0, -1):
                    if _message_has_tool_use(messages[search_index]):
                        cut_index = search_index
                        break
                
                # If we adjusted backward, break to avoid infinite loop
                if cut_index < original_cut_index:
                    break
                # If we didn't find a tool_use, move forward
                cut_index += 1
            else:
                break
    
    return (cut_index, first_msg)


@before_model
def trim_messages(state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
    """
    Keep only recent messages while preserving tool_use/tool_result pairs.
    Also implements token-based trimming to prevent context overflow.
    """
    messages = state["messages"]
    
    result = _trim_messages_impl(messages)
    if result is None:
        return None
    
    cut_index, first_msg = result
    recent_messages = messages[cut_index:]
    new_messages = [first_msg] + recent_messages
    
    return {
        "messages": [
            RemoveMessage(id=REMOVE_ALL_MESSAGES),
            *new_messages
        ]
    }


def validate_sql_against_schema(sql_query: str) -> tuple[bool, Optional[str]]:
    """
    Validate SQL query against the database schema.
    
    Returns:
        tuple[bool, Optional[str]]: (is_valid, error_message)
        - If valid: (True, None)
        - If invalid: (False, "descriptive error message")
    """
    try:
        # Get current database schema
        schema_info = get_database_schema()
        table_columns: Dict[str, List[str]] = {}
        table_column_types: Dict[str, Dict[str, str]] = {}
        
        for table_name, table_info in schema_info["tables"].items():
            table_columns[table_name] = [col["name"] for col in table_info["columns"]]
            # Store column types for validation
            table_column_types[table_name] = {
                col["name"]: col["type"].upper() for col in table_info["columns"]
            }
        
        # Parse SQL to extract table and column references
        sql_upper = sql_query.upper()
        sql_normalized = sql_query.replace('"', '').replace("'", "")
        
        # Check 0: Detect COALESCE with type mismatches (TEXT with numeric defaults)
        coalesce_pattern = r'COALESCE\s*\(\s*"([^"]+)"\s*,\s*(\d+)\s*\)'
        coalesce_matches = re.findall(coalesce_pattern, sql_query, re.IGNORECASE)
        
        if coalesce_matches:
            # Extract table references to check column types
            table_refs = re.findall(r'(?:FROM|JOIN)\s+"([^"]+)"', sql_query, re.IGNORECASE)
            
            for col_name, default_value in coalesce_matches:
                # Check if this column exists in any of the referenced tables
                for table_name in table_refs:
                    if table_name in table_column_types and col_name in table_column_types[table_name]:
                        col_type = table_column_types[table_name][col_name]
                        
                        # Check if it's a text type being coalesced with a number
                        text_types = ['VARCHAR', 'TEXT', 'CHAR', 'CHARACTER']
                        if any(text_type in col_type for text_type in text_types):
                            return (False,
                                f"VALIDATION ERROR: Type mismatch in COALESCE expression.\n"
                                f"Column '{col_name}' is type {col_type} but you're using numeric default {default_value}.\n"
                                f"PostgreSQL error: 'COALESCE types text and integer cannot be matched'\n"
                                f"Fix: Use explicit casting: COALESCE(CAST(\"{col_name}\" AS INTEGER), {default_value}) "
                                f"or COALESCE(\"{col_name}\"::INTEGER, {default_value})")
        
        # Check 1: Detect LIMIT before UNION without parentheses
        if 'UNION' in sql_upper:
            # Pattern: LIMIT <number> followed by UNION without closing parenthesis
            limit_union_pattern = r'LIMIT\s+\d+\s+UNION'
            if re.search(limit_union_pattern, sql_upper):
                # Check if this LIMIT is inside parentheses (which would be correct)
                # Look for patterns like: ) UNION or proper wrapping
                if not re.search(r'\)\s*UNION', sql_query, re.IGNORECASE):
                    return (False,
                        "VALIDATION ERROR: LIMIT clause before UNION requires parentheses around each SELECT statement.\n"
                        "PostgreSQL syntax error: When using LIMIT with UNION, each SELECT must be wrapped in parentheses.\n"
                        "Fix: Change from 'SELECT ... LIMIT 100 UNION ALL SELECT ... LIMIT 100'\n"
                        "     to '(SELECT ... LIMIT 100) UNION ALL (SELECT ... LIMIT 100)'\n"
                        "Each SELECT statement should be enclosed in parentheses when using LIMIT with UNION.")
        
        # Check 2: Detect SELECT DISTINCT + ORDER BY mismatch
        if "SELECT DISTINCT" in sql_upper and "ORDER BY" in sql_upper:
            # Extract SELECT columns
            select_match = re.search(r'SELECT\s+DISTINCT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
            if select_match:
                select_clause = select_match.group(1)
                # Remove quoted identifiers and aliases for comparison
                select_cols = re.findall(r'"([^"]+)"', select_clause)
                
                # Extract ORDER BY expressions
                order_match = re.search(r'ORDER\s+BY\s+(.*?)(?:LIMIT|$)', sql_query, re.IGNORECASE | re.DOTALL)
                if order_match:
                    order_clause = order_match.group(1).strip()
                    
                    # Check for CASE expressions in ORDER BY
                    if "CASE" in order_clause.upper():
                        # Extract column references from CASE expression
                        case_cols = re.findall(r'"([^"]+)"', order_clause)
                        for case_col in case_cols:
                            if case_col not in select_cols:
                                return (False, 
                                    f"VALIDATION ERROR: When using SELECT DISTINCT, all columns in ORDER BY must appear in SELECT list.\n"
                                    f"Column '{case_col}' is referenced in ORDER BY but not in SELECT.\n"
                                    f"Fix: Either add '{case_col}' to your SELECT clause, or remove DISTINCT.")
                    
                    # Check for direct column references in ORDER BY
                    order_cols = re.findall(r'"([^"]+)"', order_clause)
                    for order_col in order_cols:
                        if order_col not in select_cols and not any(order_col in col for col in select_cols):
                            return (False,
                                f"VALIDATION ERROR: When using SELECT DISTINCT, all columns in ORDER BY must appear in SELECT list.\n"
                                f"Column '{order_col}' is in ORDER BY but not in SELECT.\n"
                                f"Fix: Add '{order_col}' to your SELECT clause or remove DISTINCT.")
        
        # Check 3: Detect numeric casts on formatted text columns
        cast_pattern = r'CAST\s*\(\s*(?:REPLACE\s*\([^)]+\)\s*,\s*)?["\']?([a-zA-Z_][a-zA-Z0-9_]*)["\']?\s+AS\s+(DECIMAL|NUMERIC|INTEGER|INT|BIGINT|FLOAT|REAL)\s*\)'
        cast_matches = re.findall(cast_pattern, sql_query, re.IGNORECASE)
        
        # Also check for :: casting syntax
        double_colon_pattern = r'["\']?([a-zA-Z_][a-zA-Z0-9_]*)["\']?\s*::\s*(DECIMAL|NUMERIC|INTEGER|INT|BIGINT|FLOAT|REAL)'
        cast_matches.extend(re.findall(double_colon_pattern, sql_query, re.IGNORECASE))
        
        if cast_matches:
            # Get table references to check column types
            table_refs_for_cast = re.findall(r'(?:FROM|JOIN)\s+"([^"]+)"', sql_query, re.IGNORECASE)
            
            for col_name, target_type in cast_matches:
                # Skip if already wrapped in REPLACE (user already fixed it)
                if f'REPLACE("{col_name}"' in sql_query or f"REPLACE('{col_name}'" in sql_query:
                    continue
                
                # Find which table this column belongs to
                for table_name in table_refs_for_cast:
                    if table_name not in table_column_types:
                        continue
                    
                    if col_name in table_column_types[table_name]:
                        col_type = table_column_types[table_name][col_name]
                        
                        # Only check TEXT/VARCHAR columns
                        text_types = ['VARCHAR', 'TEXT', 'CHAR', 'CHARACTER']
                        if any(text_type in col_type for text_type in text_types):
                            # Check sample values for formatting
                            table_info = schema_info["tables"].get(table_name, {})
                            sample_data = table_info.get("sample_values", {})
                            sample_values = sample_data.get(col_name, [])
                            
                            has_formatting = any(
                                isinstance(val, str) and (
                                    ',' in val or '$' in val or '(' in val or '%' in val
                                )
                                for val in sample_values if val is not None
                            )
                            
                            if has_formatting:
                                sample_example = next(
                                    (str(val) for val in sample_values 
                                     if val and isinstance(val, str) and (',' in str(val) or '$' in str(val))),
                                    None
                                )
                                
                                return (False,
                                    f"VALIDATION ERROR: Attempting to cast formatted number to {target_type.upper()}.\n"
                                    f"Column '{col_name}' in table '{table_name}' contains formatted values like \"{sample_example}\".\n"
                                    f"PostgreSQL error: 'invalid input syntax for type numeric'\n"
                                    f"Fix: Strip formatting characters first:\n"
                                    f"  CAST(REPLACE(\"{col_name}\", ',', '') AS {target_type.upper()})\n"
                                    f"Or for currency: CAST(REPLACE(REPLACE(\"{col_name}\", '$', ''), ',', '') AS {target_type.upper()})")
        
        # Check 4: Verify all referenced columns exist in their tables
        # Extract table references from FROM and JOIN clauses
        table_refs = re.findall(r'(?:FROM|JOIN)\s+"([^"]+)"', sql_query, re.IGNORECASE)
        
        for table_ref in table_refs:
            if table_ref not in table_columns:
                available_tables = ", ".join(list(table_columns.keys())[:5])
                return (False,
                    f"VALIDATION ERROR: Table '{table_ref}' does not exist in the database.\n"
                    f"Available tables include: {available_tables}\n"
                    f"Fix: Use get_database_schema_tool to see all available tables.")
            
            # Extract column references for this table
            # Look for patterns like "table"."column" or just "column" in SELECT/WHERE
            table_col_pattern = rf'"{table_ref}"\."([^"]+)"'
            table_specific_cols = re.findall(table_col_pattern, sql_query, re.IGNORECASE)
            
            for col in table_specific_cols:
                if col not in table_columns[table_ref]:
                    available_cols = ", ".join(table_columns[table_ref][:10])
                    return (False,
                        f"VALIDATION ERROR: Column '{col}' does not exist in table '{table_ref}'.\n"
                        f"Available columns: {available_cols}\n"
                        f"Fix: Check the schema with get_database_schema_tool and use the correct column name.")
        
        # Check 5: For single-table queries, validate all column references
        if len(table_refs) == 1:
            table_name = table_refs[0]
            # Extract all quoted column references
            all_cols = re.findall(r'"([^"]+)"', sql_query, re.IGNORECASE)
            # Remove the table name itself
            all_cols = [col for col in all_cols if col != table_name]
            
            for col in all_cols:
                # Skip if it's a known keyword or function result
                if col.upper() in ('ASC', 'DESC', 'NULLS', 'LAST', 'FIRST'):
                    continue
                
                if col not in table_columns[table_name]:
                    # Check for close matches
                    similar_cols = [c for c in table_columns[table_name] 
                                   if c.lower().startswith(col.lower()[:3])]
                    suggestion = f" Did you mean: {', '.join(similar_cols[:3])}?" if similar_cols else ""
                    available_cols = ", ".join(table_columns[table_name][:10])
                    
                    return (False,
                        f"VALIDATION ERROR: Column '{col}' does not exist in table '{table_name}'.{suggestion}\n"
                        f"Available columns: {available_cols}\n"
                        f"Fix: Use get_database_schema_tool to verify the exact column names.")
        
        return (True, None)
        
    except Exception as e:
        # If validation fails due to an error, allow the query to proceed
        # (fail open rather than fail closed for validation)
        return (True, None)


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
        
        # Validate SQL against schema (Phase 2: Pre-execution validation)
        is_valid, validation_error = validate_sql_against_schema(sql_query)
        if not is_valid:
            return validation_error

        # Check for protected system tables
        sql_upper = sql_query.upper()
        for table in _EXTENDED_PROTECTED_TABLES:
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
                    # Surface the table name for core system tables covered by tests,
                    # but keep extended operational tables opaque.
                    if table in PROTECTED_SYSTEM_TABLES:
                        return f"ERROR: Access to system table '{table}' is not allowed. This table contains operational data and is protected for security reasons."
                    return "ERROR: Access to protected system tables is not allowed."

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
            timeout_ms = settings.query_timeout_seconds * 1000
            conn.execute(text(f"SET statement_timeout = '{timeout_ms}'"))

            result = conn.execute(text(sql_query))
            columns = result.keys()

            # Limit rows based on configuration
            rows = result.fetchmany(settings.query_row_limit)
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
        model="claude-sonnet-4-5-20250929",
        api_key=settings.anthropic_api_key,
        temperature=0,  # Keep deterministic for SQL generation
        max_tokens=4096,
        timeout=90.0,  # 90 second timeout for API calls
        max_retries=2  # Retry on transient failures
    )

    # Define tools
    tools = [
        list_tables_tool,          # New tool: lightweight discovery
        get_table_schema_tool,     # New tool: detailed drill-down
        get_related_tables_tool,   # Existing
        execute_sql_query          # Existing
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

        # Allow up to 3 attempts total for error recovery
        max_attempts = 3 if force_sql else 2  # Even for non-SQL queries, allow one retry for errors
        
        for attempt in range(max_attempts):
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

            # Check if the query resulted in a PostgreSQL error
            retry_instruction = None
            
            # First priority: Check for database execution errors and provide recovery guidance
            if final_response.startswith("ERROR"):
                error_recovery = _detect_postgresql_error(final_response)
                if error_recovery and attempt < max_attempts - 1:
                    retry_instruction = error_recovery
            
            # Second priority: For SQL-required prompts, ensure SQL was executed
            elif force_sql:
                if not executed_sql:
                    retry_instruction = FORCE_SQL_FOLLOW_UP_MESSAGE
                elif _sql_targets_system_tables(executed_sql) or _csv_looks_like_metadata(csv_data):
                    retry_instruction = FORCE_SQL_SYSTEM_TABLE_MESSAGE

            # If we need to retry, reset state and add retry instruction
            if retry_instruction and attempt < max_attempts - 1:
                executed_sql = None
                csv_data = None
                execution_time = None
                rows_returned = None
                final_response = ""
                messages.append(HumanMessage(content=retry_instruction))
                continue

            # Success - break out of retry loop
            if executed_sql or not force_sql or attempt == max_attempts - 1:
                break

        if force_sql and not executed_sql:
            fallback_result = _attempt_fallback_response(user_prompt)
            if fallback_result:
                executed_sql, csv_data, execution_time, rows_returned, final_response = fallback_result

        # Ensure we have a meaningful response
        if not final_response or len(final_response.strip()) == 0:
            final_response = "I processed your request but didn't generate a response. Please try rephrasing your query."

        chart_suggestion = build_chart_suggestion(user_prompt, csv_data)

        return {
            "success": True,
            "response": final_response,
            "executed_sql": executed_sql,
            "data_csv": csv_data,
            "execution_time_seconds": execution_time,
            "rows_returned": rows_returned,
            "chart_suggestion": chart_suggestion,
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
            "rows_returned": None,
            "chart_suggestion": {
                "should_display": False,
                "reason": f"Unable to suggest a chart due to error: {str(e)}",
                "spec": None,
            },
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


def _detect_postgresql_error(error_message: str) -> Optional[str]:
    """
    Detect common PostgreSQL errors and provide actionable recovery instructions.
    
    Returns:
        Optional[str]: Recovery instruction for the LLM, or None if not a known error pattern
    """
    if not error_message or not error_message.startswith("ERROR"):
        return None
    
    error_lower = error_message.lower()
    
    # Pattern 1: Integer out of range
    if "integer out of range" in error_lower:
        return (
            "The query failed because a value exceeds the INTEGER type limit (2,147,483,647).\n"
            "Fix: Replace CAST(column AS INTEGER) with CAST(column AS BIGINT) in your query.\n"
            "BIGINT can handle much larger numbers (up to 9 quintillion).\n"
            "Please regenerate the query using BIGINT instead of INTEGER for numeric casts."
        )
    
    # Pattern 2: Invalid input syntax for numeric (formatted numbers)
    if "invalid input syntax for type" in error_lower and ("numeric" in error_lower or "integer" in error_lower):
        return (
            "The query failed because the column contains formatted text (e.g., '1,234' or '$5,000').\n"
            "Fix: Strip formatting characters before casting:\n"
            "  CAST(REPLACE(REPLACE(column, ',', ''), '$', '') AS NUMERIC)\n"
            "Please regenerate the query with proper text formatting removal."
        )
    
    # Pattern 3: Column does not exist
    if "column" in error_lower and "does not exist" in error_lower:
        # Extract column name if possible
        col_match = re.search(r'column "([^"]+)" does not exist', error_message, re.IGNORECASE)
        col_name = col_match.group(1) if col_match else "unknown"
        return (
            f"The query failed because column '{col_name}' does not exist in the table.\n"
            "Fix: Use get_table_schema_tool to verify the exact column names, then regenerate the query.\n"
            "Column names are case-sensitive and may have different spellings than expected."
        )
    
    # Pattern 4: Type mismatch in COALESCE
    if "coalesce types" in error_lower and "cannot be matched" in error_lower:
        return (
            "The query failed due to type mismatch in COALESCE.\n"
            "Fix: Add explicit casting: COALESCE(CAST(column AS INTEGER), 0)\n"
            "Please regenerate the query with proper type casting."
        )
    
    # Pattern 5: Division by zero
    if "division by zero" in error_lower:
        return (
            "The query failed due to division by zero.\n"
            "Fix: Use NULLIF to avoid division by zero: column1 / NULLIF(column2, 0)\n"
            "Please regenerate the query with zero-division protection."
        )
    
    # Pattern 6: Syntax errors
    if "syntax error" in error_lower:
        return (
            "The query has a SQL syntax error.\n"
            "Fix: Review the SQL syntax, check for missing commas, parentheses, or keywords.\n"
            "Please regenerate the query with correct SQL syntax."
        )
    
    return None


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
        "IMPORT_DUPLICATES",
        "QUERY_MESSAGES",
        "QUERY_THREADS",
        "API_KEYS",
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
                  AND table_name NOT IN ('file_imports', 'table_metadata', 'import_history', 'mapping_errors', 'mapping_chunk_status', 'uploaded_files', 'users', 'import_jobs', 'import_duplicates', 'api_keys', 'query_messages', 'query_threads')
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

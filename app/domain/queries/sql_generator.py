"""
Lightweight SQL generation from natural language prompts.

This module provides a fast, single-LLM-call SQL generator that bypasses
the full agent workflow. Designed for the probe phase of large export requests.
"""
import re
from typing import Dict, List, Optional, Any
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage

from app.core.config import settings
from app.db.context import get_database_schema, format_schema_for_prompt, get_table_names


# Protected system tables (same as agent.py)
PROTECTED_SYSTEM_TABLES = {
    'import_history', 'mapping_errors', 'table_metadata', 'uploaded_files',
    'users', 'file_imports', 'import_jobs', 'llm_instructions', 'workflows',
    'workflow_steps', 'workflow_variables', 'workflow_executions',
    'workflow_step_results', 'api_keys', 'import_duplicates',
    'query_messages', 'query_threads',
}


SQL_GENERATION_SYSTEM_PROMPT = """You are an expert SQL generator. Your job is to convert natural language requests into PostgreSQL SELECT queries.

**Your Task:**
1. Analyze the user's natural language prompt
2. Review the provided database schema
3. Generate a single, accurate SQL SELECT query
4. Return ONLY the SQL query and a brief explanation

**Rules:**
- Generate ONLY SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
- Use proper PostgreSQL syntax with double quotes for table/column names
- NEVER access system tables (users, api_keys, import_history, etc.)
- NEVER select system columns starting with underscore (_row_id, _import_id, etc.)
- Include appropriate WHERE, ORDER BY, and LIMIT clauses based on the request
- Use proper JOINs when multiple tables are needed
- Use UNION/UNION ALL when combining records from multiple tables
- Always wrap SELECT statements in parentheses when using LIMIT with UNION

**Type Casting:**
- Use explicit CAST() for text-to-numeric conversions: CAST(column AS INTEGER)
- Use BIGINT for large numbers (>2 billion)
- Strip formatting before casting: CAST(REPLACE(column, ',', '') AS NUMERIC)

**Output Format:**
Return your response in this exact format:

SQL:
```sql
<your SQL query here>
```

EXPLANATION:
<brief explanation of what the query does>

TABLES:
<comma-separated list of tables referenced>"""


def generate_sql_from_prompt(
    prompt: str,
    table_hints: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Generate SQL query from natural language prompt using a single LLM call.
    
    Args:
        prompt: Natural language description of the desired query
        table_hints: Optional list of table names to focus on (narrows schema context)
    
    Returns:
        Dict containing:
        - success: bool
        - sql_query: str (if successful)
        - tables_referenced: List[str] (if successful)
        - explanation: str (if successful)
        - error: str (if failed)
    """
    try:
        # Get database schema (with optional table filtering)
        if table_hints:
            # Filter to only specified tables
            available_tables = [t["name"] for t in get_table_names()]
            filtered_tables = [t for t in table_hints if t in available_tables]
            if not filtered_tables:
                return {
                    "success": False,
                    "error": f"None of the specified tables found: {', '.join(table_hints)}"
                }
            schema_info = get_database_schema(table_names=filtered_tables)
        else:
            # Get all tables
            schema_info = get_database_schema()
        
        # Format schema for prompt
        schema_text = format_schema_for_prompt(schema_info)
        
        # Build the prompt
        full_prompt = f"""{SQL_GENERATION_SYSTEM_PROMPT}

**Database Schema:**
{schema_text}

**User Request:**
{prompt}

Generate the SQL query now."""
        
        # Initialize LLM
        llm = ChatAnthropic(
            model="claude-haiku-4-5-20251001",
            api_key=settings.anthropic_api_key,
            temperature=0,
            max_tokens=2048
        )
        
        # Single LLM call
        message = HumanMessage(content=full_prompt)
        response = llm.invoke([message])
        
        # Parse response
        result = _parse_llm_response(response.content)
        
        if not result["success"]:
            return result
        
        # Validate the generated SQL
        validation_result = _validate_generated_sql(result["sql_query"])
        if not validation_result["valid"]:
            return {
                "success": False,
                "error": validation_result["error"]
            }
        
        return result
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to generate SQL: {str(e)}"
        }


def _parse_llm_response(content: str) -> Dict[str, Any]:
    """
    Parse the LLM response to extract SQL, explanation, and tables.
    
    Args:
        content: Raw LLM response text
    
    Returns:
        Dict with success, sql_query, explanation, tables_referenced, or error
    """
    try:
        # Extract SQL from code block
        sql_match = re.search(r'```sql\s*(.*?)\s*```', content, re.DOTALL | re.IGNORECASE)
        if not sql_match:
            # Try without language tag
            sql_match = re.search(r'```\s*(SELECT.*?)\s*```', content, re.DOTALL | re.IGNORECASE)
        
        if not sql_match:
            return {
                "success": False,
                "error": "Could not extract SQL from LLM response"
            }
        
        sql_query = sql_match.group(1).strip()
        
        # Extract explanation
        explanation_match = re.search(r'EXPLANATION:\s*(.+?)(?=TABLES:|$)', content, re.DOTALL | re.IGNORECASE)
        explanation = explanation_match.group(1).strip() if explanation_match else "SQL query generated"
        
        # Extract tables
        tables_match = re.search(r'TABLES:\s*(.+?)(?=\n\n|$)', content, re.DOTALL | re.IGNORECASE)
        tables_text = tables_match.group(1).strip() if tables_match else ""
        tables_referenced = [t.strip() for t in tables_text.split(',') if t.strip()]
        
        # If tables not explicitly listed, extract from SQL
        if not tables_referenced:
            tables_referenced = _extract_tables_from_sql(sql_query)
        
        return {
            "success": True,
            "sql_query": sql_query,
            "explanation": explanation,
            "tables_referenced": tables_referenced
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to parse LLM response: {str(e)}"
        }


def _extract_tables_from_sql(sql: str) -> List[str]:
    """Extract table names from SQL query."""
    # Find table names after FROM and JOIN keywords
    pattern = r'(?:FROM|JOIN)\s+"([^"]+)"'
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return list(set(matches))  # Remove duplicates


def _validate_generated_sql(sql_query: str) -> Dict[str, Any]:
    """
    Validate generated SQL for security and correctness.
    
    Returns:
        Dict with 'valid' (bool) and optional 'error' (str)
    """
    # Must be a SELECT query
    sql_stripped = sql_query.strip().upper()
    if not (sql_stripped.startswith('SELECT') or sql_stripped.startswith('(SELECT')):
        return {
            "valid": False,
            "error": "Generated query is not a SELECT statement"
        }
    
    # Check for protected system tables
    sql_upper = sql_query.upper()
    for table in PROTECTED_SYSTEM_TABLES:
        table_patterns = [
            rf'\bFROM\s+["\']?{table.upper()}["\']?\b',
            rf'\bJOIN\s+["\']?{table.upper()}["\']?\b',
        ]
        for pattern in table_patterns:
            if re.search(pattern, sql_upper):
                return {
                    "valid": False,
                    "error": f"Cannot access protected system table: {table}"
                }
    
    # Check for dangerous operations
    dangerous_patterns = [
        r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b',
        r';\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)',
    ]
    for pattern in dangerous_patterns:
        if re.search(pattern, sql_query, re.IGNORECASE):
            return {
                "valid": False,
                "error": "Generated query contains forbidden operations"
            }
    
    return {"valid": True}

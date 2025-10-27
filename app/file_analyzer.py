"""
AI-powered file analysis for intelligent database consolidation.

This module uses LangChain agents with Claude Sonnet to analyze uploaded files
and determine the optimal import strategy by comparing with existing database tables.
"""

from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
import numpy as np
from dataclasses import dataclass
from langchain.tools import tool, ToolRuntime
from langchain.agents import create_agent
from langchain_anthropic import ChatAnthropic
from .config import settings
from .db_context import get_database_schema, format_schema_for_prompt
import logging

logger = logging.getLogger(__name__)


class ImportStrategy(str, Enum):
    """Strategies for importing data into the database"""
    NEW_TABLE = "new_table"           # Create fresh table
    MERGE_EXACT = "merge_exact"       # Exact schema match
    EXTEND_TABLE = "extend_table"     # Add columns to existing
    ADAPT_DATA = "adapt_data"         # Transform to fit existing


class AnalysisMode(str, Enum):
    """Controls auto-execution behavior"""
    MANUAL = "manual"                      # User reviews and approves
    AUTO_HIGH_CONFIDENCE = "auto_high"     # Auto-execute if confidence > threshold
    AUTO_ALWAYS = "auto_always"            # Always auto-execute


class ConflictResolutionMode(str, Enum):
    """How to handle schema conflicts"""
    ASK_USER = "ask_user"              # Stop and ask for clarification
    LLM_DECIDE = "llm_decide"          # Let LLM resolve conflicts
    PREFER_FLEXIBLE = "prefer_flexible" # Use most flexible data type


@dataclass
class AnalysisContext:
    """Context passed through the analysis pipeline"""
    file_sample: List[Dict[str, Any]]
    file_metadata: Dict[str, Any]
    existing_schema: Dict[str, Any]
    analysis_mode: AnalysisMode
    conflict_mode: ConflictResolutionMode
    user_id: Optional[str] = None


@dataclass
class TableMatch:
    """Represents a potential table match"""
    table_name: str
    similarity_score: float  # 0.0 to 1.0
    matching_columns: List[str]
    missing_columns: List[str]
    extra_columns: List[str]
    reasoning: str


@dataclass
class SchemaConflict:
    """Represents a conflict that needs resolution"""
    conflict_type: str  # "data_type_mismatch", "column_name_variation", etc.
    description: str
    options: List[str]
    recommended_option: str
    reasoning: str


def calculate_sample_size(total_rows: int) -> int:
    """
    Calculate optimal sample size based on total rows.
    
    Strategy:
    - Small files (<= 100): Use all data
    - Medium files (100-1000): 100 rows
    - Large files (1000-10000): 200 rows
    - Very large files (>10000): 500 rows
    
    Args:
        total_rows: Total number of rows in the file
        
    Returns:
        Optimal sample size
    """
    if total_rows <= 100:
        return total_rows
    elif total_rows <= 1000:
        return 100
    elif total_rows <= 10000:
        return 200
    else:
        return 500


def sample_file_data(
    records: List[Dict[str, Any]], 
    target_sample_size: Optional[int] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Intelligent sampling strategy for file analysis.
    
    Strategy:
    - Always include beginning rows (captures header patterns)
    - Stratified random sampling across the rest of the file
    - Ensures good population distribution
    
    Args:
        records: All records from the file
        target_sample_size: Desired sample size (auto-calculated if None)
        
    Returns:
        Tuple of (sampled_records, total_row_count)
    """
    total_rows = len(records)
    
    # Calculate sample size if not provided
    if target_sample_size is None:
        target_sample_size = calculate_sample_size(total_rows)
    
    # If file is smaller than target, use all data
    if total_rows <= target_sample_size:
        logger.info(f"File has {total_rows} rows, using all data")
        return records, total_rows
    
    # Split sample budget: 50% from beginning, 50% random
    head_size = min(50, target_sample_size // 2)
    random_size = target_sample_size - head_size
    
    logger.info(f"Sampling {target_sample_size} rows from {total_rows} total "
                f"({head_size} from start, {random_size} random)")
    
    # Get beginning rows
    head_sample = records[:head_size]
    
    # Stratified random sampling from the rest
    remaining = records[head_size:]
    if random_size >= len(remaining):
        random_sample = remaining
    else:
        # Sample evenly across the file for good distribution
        indices = np.linspace(0, len(remaining) - 1, random_size, dtype=int)
        random_sample = [remaining[i] for i in indices]
    
    return head_sample + random_sample, total_rows


# Tools for the agent

@tool
def analyze_file_structure(
    runtime: ToolRuntime[AnalysisContext]
) -> Dict[str, Any]:
    """
    Analyze the structure of the uploaded file sample.
    
    Returns information about columns, data types, and data quality issues.
    """
    context = runtime.context
    sample_data = context.file_sample
    
    if not sample_data:
        return {"error": "No sample data provided"}
    
    # Extract column information
    columns = list(sample_data[0].keys())
    
    # Analyze data types for each column
    column_analysis = {}
    for col in columns:
        values = [row.get(col) for row in sample_data if row.get(col) is not None]
        
        # Determine predominant type
        types = {}
        for val in values:
            val_type = type(val).__name__
            types[val_type] = types.get(val_type, 0) + 1
        
        predominant_type = max(types.items(), key=lambda x: x[1])[0] if types else "unknown"
        
        # Check for nulls
        null_count = sum(1 for row in sample_data if row.get(col) is None)
        null_percentage = (null_count / len(sample_data)) * 100
        
        column_analysis[col] = {
            "predominant_type": predominant_type,
            "null_percentage": null_percentage,
            "sample_values": values[:5]  # First 5 non-null values
        }
    
    # Identify data quality issues
    issues = []
    for col, analysis in column_analysis.items():
        if analysis["null_percentage"] > 50:
            issues.append(f"Column '{col}' has {analysis['null_percentage']:.1f}% null values")
    
    return {
        "columns": columns,
        "column_count": len(columns),
        "row_count_sampled": len(sample_data),
        "total_rows": context.file_metadata.get("total_rows", len(sample_data)),
        "column_analysis": column_analysis,
        "data_quality_issues": issues
    }


@tool
def get_existing_database_schema(
    runtime: ToolRuntime[AnalysisContext]
) -> str:
    """
    Get the current database schema with all tables and their structures.
    
    Returns formatted schema information for analysis.
    """
    context = runtime.context
    schema_info = context.existing_schema
    
    # Format schema for LLM consumption
    formatted = format_schema_for_prompt(schema_info)
    
    return formatted


@tool
def compare_file_with_tables(
    file_columns: List[str],
    runtime: ToolRuntime[AnalysisContext]
) -> List[Dict[str, Any]]:
    """
    Compare file columns with existing database tables to find potential matches.
    
    Args:
        file_columns: List of column names from the file
        
    Returns:
        List of potential table matches with similarity analysis
    """
    context = runtime.context
    schema_info = context.existing_schema
    
    matches = []
    
    for table_name, table_info in schema_info.get("tables", {}).items():
        table_columns = [col["name"] for col in table_info["columns"]]
        
        # Find matching columns (case-insensitive)
        file_cols_lower = [c.lower() for c in file_columns]
        table_cols_lower = [c.lower() for c in table_columns]
        
        matching = [c for c in file_cols_lower if c in table_cols_lower]
        missing_in_file = [c for c in table_cols_lower if c not in file_cols_lower]
        extra_in_file = [c for c in file_cols_lower if c not in table_cols_lower]
        
        # Calculate basic similarity score
        if len(table_columns) > 0:
            similarity = len(matching) / max(len(file_columns), len(table_columns))
        else:
            similarity = 0.0
        
        matches.append({
            "table_name": table_name,
            "similarity_score": similarity,
            "matching_columns": matching,
            "missing_in_file": missing_in_file,
            "extra_in_file": extra_in_file,
            "table_row_count": table_info.get("row_count", 0)
        })
    
    # Sort by similarity score
    matches.sort(key=lambda x: x["similarity_score"], reverse=True)
    
    return matches


@tool
def resolve_conflict(
    conflict_description: str,
    options: List[str],
    runtime: ToolRuntime[AnalysisContext]
) -> str:
    """
    Resolve a schema or data type conflict.
    
    Args:
        conflict_description: Description of the conflict
        options: Available resolution options
        
    Returns:
        Chosen resolution option
    """
    context = runtime.context
    
    if context.conflict_mode == ConflictResolutionMode.ASK_USER:
        # Signal that user input is required
        return "USER_INPUT_REQUIRED"
    
    elif context.conflict_mode == ConflictResolutionMode.PREFER_FLEXIBLE:
        # Choose the most flexible option (usually TEXT for data types)
        if "TEXT" in options:
            return "TEXT"
        elif "VARCHAR" in options:
            return "VARCHAR"
        else:
            return options[0] if options else "TEXT"
    
    else:  # LLM_DECIDE
        # Let the agent decide based on context
        return "LLM_WILL_DECIDE"


def create_file_analyzer_agent(max_iterations: int = 5):
    """
    Create an LLM-powered agent for file analysis.
    
    The agent has access to tools for:
    - Analyzing file structure
    - Comparing with existing tables
    - Resolving conflicts
    
    Args:
        max_iterations: Maximum number of tool calls allowed (prevents runaway costs)
        
    Returns:
        Configured LangChain agent
    """
    model = ChatAnthropic(
        model="claude-sonnet-4-5",
        api_key=settings.anthropic_api_key,
        temperature=0,  # Deterministic for consistent decisions
        max_tokens=4096
    )
    
    tools = [
        analyze_file_structure,
        get_existing_database_schema,
        compare_file_with_tables,
        resolve_conflict
    ]
    
    system_prompt = """You are a database consolidation expert helping users organize data from multiple sources.

Your task is to analyze an uploaded file and determine the best way to import it into an existing database.

Available Import Strategies:
1. NEW_TABLE - Data is unique enough to warrant a new table
2. MERGE_EXACT - File matches an existing table's schema exactly
3. EXTEND_TABLE - File is similar to an existing table but has additional columns
4. ADAPT_DATA - File data can be transformed to fit an existing table structure

Analysis Process:
1. Use analyze_file_structure to understand the file's columns and data types
2. Use get_existing_database_schema to see what tables already exist
3. Use compare_file_with_tables to find potential matches
4. Consider business semantics - do columns represent the same concepts?
5. Use resolve_conflict if there are data type or naming conflicts

Important Considerations:
- Column name variations (e.g., "customer_id" vs "client_id") may represent the same data
- Data consolidation benefits should be weighed against data integrity
- Consider existing table usage patterns and row counts
- Provide clear reasoning for your recommendations
- Include confidence scores (0.0 to 1.0) based on match quality

CRITICAL: You have a maximum of {max_iterations} tool calls to complete your analysis.
Be efficient and strategic with your tool usage.

Output Format:
Provide a structured recommendation including:
- Recommended strategy (NEW_TABLE, MERGE_EXACT, EXTEND_TABLE, or ADAPT_DATA)
- Confidence score (0.0 to 1.0)
- Clear reasoning for the recommendation
- If merging/extending: which table to use
- Suggested column mappings
- Any data quality issues or conflicts found
"""
    
    agent = create_agent(
        model=model,
        tools=tools,
        system_prompt=system_prompt.format(max_iterations=max_iterations)
    )
    
    return agent


def analyze_file_for_import(
    file_sample: List[Dict[str, Any]],
    file_metadata: Dict[str, Any],
    analysis_mode: AnalysisMode = AnalysisMode.MANUAL,
    conflict_mode: ConflictResolutionMode = ConflictResolutionMode.ASK_USER,
    user_id: Optional[str] = None,
    max_iterations: int = 5
) -> Dict[str, Any]:
    """
    Analyze a file and determine the optimal import strategy.
    
    Args:
        file_sample: Sample of records from the file
        file_metadata: Metadata about the file (name, total rows, etc.)
        analysis_mode: Whether to require user approval
        conflict_mode: How to handle conflicts
        user_id: Optional user identifier
        max_iterations: Maximum LLM iterations
        
    Returns:
        Analysis results with recommendations
    """
    try:
        # Get existing database schema
        schema_info = get_database_schema()
        
        # Create analysis context
        context = AnalysisContext(
            file_sample=file_sample,
            file_metadata=file_metadata,
            existing_schema=schema_info,
            analysis_mode=analysis_mode,
            conflict_mode=conflict_mode,
            user_id=user_id
        )
        
        # Create and run agent
        agent = create_file_analyzer_agent(max_iterations=max_iterations)
        
        prompt = f"""Analyze this file for database import:

File: {file_metadata.get('name', 'unknown')}
Total Rows: {file_metadata.get('total_rows', 'unknown')}
Sample Size: {len(file_sample)}

Please analyze the file structure, compare it with existing tables, and recommend the best import strategy."""
        
        result = agent.invoke(
            {"messages": [{"role": "user", "content": prompt}]},
            context=context
        )
        
        # Extract the agent's response
        final_message = result["messages"][-1]
        response_text = final_message.content if hasattr(final_message, 'content') else str(final_message)
        
        # Count iterations used
        iterations_used = len([m for m in result["messages"] if hasattr(m, 'tool_calls') and m.tool_calls])
        
        return {
            "success": True,
            "response": response_text,
            "iterations_used": iterations_used,
            "max_iterations": max_iterations
        }
        
    except Exception as e:
        logger.error(f"Error analyzing file: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "response": f"Analysis failed: {str(e)}"
        }

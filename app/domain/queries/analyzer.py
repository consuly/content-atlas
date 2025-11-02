"""
AI-powered file analysis for intelligent database consolidation.

This module uses LangChain agents with Claude Haiku to analyze uploaded files
and determine the optimal import strategy by comparing with existing database tables.
"""

from typing import List, Dict, Any, Optional, Tuple, Annotated
from typing_extensions import NotRequired
from enum import Enum
import numpy as np
from dataclasses import dataclass
from langchain.tools import tool, ToolRuntime
from langchain_core.tools import InjectedToolArg
from langchain.agents import create_agent, AgentState
from langchain.agents.middleware import before_model
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import RemoveMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.runtime import Runtime
from app.core.config import settings
from app.db.context import get_database_schema, format_schema_for_prompt
from app.utils.date import detect_date_column, infer_date_format
import logging
import time

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


# Custom AgentState for file analysis with retry tracking
class FileAnalysisState(AgentState):
    """State for file analysis agent with conversation memory and retry tracking."""
    attempt_count: NotRequired[int]  # Number of analysis attempts
    retry_count: NotRequired[int]  # Number of retries due to errors
    error_history: NotRequired[List[str]]  # History of errors encountered
    resolution_attempts: NotRequired[List[str]]  # History of resolution attempts
    start_time: NotRequired[float]  # Analysis start time for timeout tracking


@dataclass
class AnalysisContext:
    """Context passed through the analysis pipeline"""
    file_sample: List[Dict[str, Any]]
    file_metadata: Dict[str, Any]
    existing_schema: Dict[str, Any]
    analysis_mode: AnalysisMode
    conflict_mode: ConflictResolutionMode
    user_id: Optional[str] = None
    attempt_count: int = 0
    retry_count: int = 0
    error_history: List[str] = None
    resolution_attempts: List[str] = None
    
    def __post_init__(self):
        if self.error_history is None:
            self.error_history = []
        if self.resolution_attempts is None:
            self.resolution_attempts = []


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
    target_sample_size: Optional[int] = None,
    max_sample_size: Optional[int] = None
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
        max_sample_size: Optional hard cap for the computed sample size
        
    Returns:
        Tuple of (sampled_records, total_row_count)
    """
    total_rows = len(records)
    
    # Calculate sample size if not provided
    if target_sample_size is None:
        target_sample_size = calculate_sample_size(total_rows)
    
    if max_sample_size is not None:
        target_sample_size = min(target_sample_size, max_sample_size)
    
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
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()]
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
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()]
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
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()]
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
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()]
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


@tool
def analyze_raw_csv_structure(
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()]
) -> Dict[str, Any]:
    """
    Analyze raw CSV structure to determine if it has headers and infer column meanings.
    
    This is the PRIMARY tool for CSV analysis. It examines the raw CSV rows to:
    1. Determine if row 1 is a header or data
    2. Infer semantic column names (for headerless files)
    3. Identify data types and patterns
    4. Detect needed transformations (date formats, string cleaning, etc.)
    
    Use this tool FIRST when analyzing CSV files, before any other structural analysis.
    
    Returns:
        Comprehensive analysis of CSV structure with recommendations
    """
    import re
    
    context = runtime.context
    
    # Check if we have raw CSV rows in metadata
    raw_rows = context.file_metadata.get('raw_csv_rows')
    
    if not raw_rows:
        # Fallback to analyzing processed sample data
        return infer_schema_from_headerless_data(runtime)
    
    if len(raw_rows) < 2:
        return {
            "error": "Need at least 2 rows to analyze CSV structure",
            "rows_available": len(raw_rows)
        }
    
    first_row = raw_rows[0]
    second_row = raw_rows[1] if len(raw_rows) > 1 else []
    
    # Analyze first row to determine if it's a header
    has_header = _analyze_if_header_row(first_row, second_row)
    
    result = {
        "has_header": has_header,
        "num_columns": len(first_row),
        "sample_rows": raw_rows[:5]  # First 5 rows for context
    }
    
    if has_header:
        # First row is header - use those names
        result["column_names"] = first_row
        result["reasoning"] = "First row contains header-like strings (column names)"
        result["data_starts_at_row"] = 1  # Data starts at row index 1 (0-indexed)
        
        # Analyze data types from subsequent rows
        data_rows = raw_rows[1:]
        result["inferred_types"] = _infer_column_types_from_rows(first_row, data_rows)
        
    else:
        # First row is data - need to infer column meanings
        result["reasoning"] = "First row contains data values, not headers"
        result["data_starts_at_row"] = 0  # Data starts at row index 0
        
        # Infer semantic column names from data patterns
        inferred_schema = _infer_schema_from_data_rows(raw_rows)
        result["inferred_columns"] = inferred_schema["columns"]
        result["overall_confidence"] = inferred_schema["confidence"]
        result["transformations_needed"] = inferred_schema["transformations"]
    
    return result


def _analyze_if_header_row(first_row: List[str], second_row: List[str]) -> bool:
    """
    Determine if the first row is a header or data.
    
    Heuristics:
    - Headers contain descriptive words (name, email, date, id, etc.)
    - Headers don't contain timestamps, emails, or typical data patterns
    - Headers are usually shorter and more uniform
    """
    import re
    
    # Common header keywords
    header_keywords = [
        'id', 'name', 'email', 'date', 'time', 'first', 'last', 'phone',
        'address', 'city', 'state', 'zip', 'country', 'age', 'gender',
        'status', 'type', 'category', 'description', 'notes', 'created',
        'updated', 'modified', 'user', 'customer', 'client', 'product'
    ]
    
    # Check for data patterns that indicate NOT a header
    for value in first_row:
        value_lower = value.lower().strip()
        
        # ISO timestamp pattern
        if re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
            return False
        
        # Email pattern
        if '@' in value and '.' in value.split('@')[-1]:
            return False
        
        # Pure numbers (IDs in data, not headers)
        if value.replace('.', '').replace('-', '').isdigit() and len(value) > 4:
            return False
    
    # Check if first row contains header-like keywords
    header_score = 0
    for value in first_row:
        value_lower = value.lower().strip()
        if any(keyword in value_lower for keyword in header_keywords):
            header_score += 1
        
        # Short, descriptive words are header-like
        if len(value) < 20 and value.replace('_', '').replace('-', '').isalpha():
            header_score += 0.5
    
    # If more than 50% of columns look like headers, it's probably a header
    return header_score / len(first_row) > 0.5


def _infer_column_types_from_rows(
    column_names: List[str],
    data_rows: List[List[str]]
) -> Dict[str, Dict[str, Any]]:
    """Infer data types for each column from data rows."""
    import re
    
    column_types = {}
    
    for col_idx, col_name in enumerate(column_names):
        # Extract values for this column
        values = [row[col_idx] if col_idx < len(row) else None for row in data_rows]
        values = [v for v in values if v]  # Remove empty
        
        if not values:
            column_types[col_name] = {
                "data_type": "TEXT",
                "confidence": 0.0,
                "reasoning": "No data values to analyze"
            }
            continue
        
        # Check for phone number patterns FIRST (before numeric check)
        # Phone patterns: xxx.xxx.xxxx, xxx-xxx-xxxx, (xxx) xxx-xxxx, xxxxxxxxxx
        phone_pattern = re.compile(r'^[\d\s\(\)\.\-]{10,}$')
        phone_keywords = ['phone', 'tel', 'mobile', 'fax', 'contact']
        
        # Check column name for phone keywords
        col_name_lower = col_name.lower()
        has_phone_keyword = any(keyword in col_name_lower for keyword in phone_keywords)
        
        # Check if values match phone patterns
        sample_values = [str(v) for v in values[:20]]
        phone_matches = sum(1 for v in sample_values if phone_pattern.match(v))
        phone_match_ratio = phone_matches / len(sample_values) if sample_values else 0
        
        if has_phone_keyword or phone_match_ratio > 0.7:
            column_types[col_name] = {
                "data_type": "TEXT",
                "confidence": 0.98 if has_phone_keyword else 0.90,
                "reasoning": "Contains phone number values" if has_phone_keyword else "Values match phone number patterns"
            }
        # Check for date patterns
        elif detect_date_column(values[:20]):
            column_types[col_name] = {
                "data_type": "TIMESTAMP",
                "confidence": 0.95,
                "reasoning": "Contains date/timestamp values"
            }
        # Check for email
        elif any('@' in str(v) for v in values[:10]):
            column_types[col_name] = {
                "data_type": "TEXT",
                "confidence": 0.90,
                "reasoning": "Contains email addresses"
            }
        # Check for numeric (only if not phone)
        elif all(str(v).replace('.', '').replace('-', '').isdigit() for v in values[:20]):
            column_types[col_name] = {
                "data_type": "DECIMAL",
                "confidence": 0.85,
                "reasoning": "Contains numeric values"
            }
        else:
            column_types[col_name] = {
                "data_type": "TEXT",
                "confidence": 0.70,
                "reasoning": "Text/string values"
            }
    
    return column_types


def _infer_schema_from_data_rows(data_rows: List[List[str]]) -> Dict[str, Any]:
    """Infer semantic column names and types from data rows (headerless file)."""
    import re
    
    if not data_rows or not data_rows[0]:
        return {
            "columns": {},
            "confidence": 0.0,
            "transformations": []
        }
    
    num_columns = len(data_rows[0])
    inferred_columns = {}
    transformations = []
    
    for col_idx in range(num_columns):
        # Extract values for this column
        values = [row[col_idx] if col_idx < len(row) else None for row in data_rows]
        values = [v for v in values if v]  # Remove empty
        
        if not values:
            inferred_columns[f"col_{col_idx}"] = {
                "semantic_name": "unknown",
                "data_type": "TEXT",
                "confidence": 0.0,
                "reasoning": "No values to analyze"
            }
            continue
        
        # Analyze patterns
        sample_values = values[:20]
        
        # Check for phone number patterns FIRST (before numeric check)
        phone_pattern = re.compile(r'^[\d\s\(\)\.\-]{10,}$')
        phone_matches = sum(1 for v in sample_values if phone_pattern.match(str(v)))
        phone_match_ratio = phone_matches / len(sample_values) if sample_values else 0
        
        if phone_match_ratio > 0.7:
            inferred_columns[f"col_{col_idx}"] = {
                "semantic_name": "phone",
                "data_type": "TEXT",
                "confidence": 0.95,
                "reasoning": f"{phone_matches}/{len(sample_values)} values match phone number patterns"
            }
        
        # Check for date patterns
            date_format = infer_date_format(sample_values)
            inferred_columns[f"col_{col_idx}"] = {
                "semantic_name": "date",
                "data_type": "TIMESTAMP",
                "confidence": 0.95,
                "reasoning": f"Contains date values in {date_format} format"
            }
            transformations.append({
                "column": f"col_{col_idx}",
                "type": "date_standardization",
                "from_format": date_format,
                "to_format": "ISO 8601"
            })
        
        # Check for email
        elif any('@' in str(v) and '.' in str(v) for v in sample_values[:10]):
            email_count = sum(1 for v in sample_values if '@' in str(v))
            inferred_columns[f"col_{col_idx}"] = {
                "semantic_name": "email",
                "data_type": "TEXT",
                "confidence": 0.98,
                "reasoning": f"{email_count}/{len(sample_values)} values contain @ symbol"
            }
        
        # Check for numeric (only if not phone)
        elif all(str(v).replace('.', '').replace('-', '').isdigit() for v in sample_values):
            if all(str(v).isdigit() for v in sample_values):
                inferred_columns[f"col_{col_idx}"] = {
                    "semantic_name": "id" if col_idx == 0 else "number",
                    "data_type": "INTEGER",
                    "confidence": 0.85,
                    "reasoning": "All values are integers"
                }
            else:
                inferred_columns[f"col_{col_idx}"] = {
                    "semantic_name": "decimal_value",
                    "data_type": "DECIMAL(10,2)",
                    "confidence": 0.85,
                    "reasoning": "Values contain decimal numbers"
                }
        
        # Check for proper names
        elif all(isinstance(v, str) for v in sample_values):
            proper_case_count = sum(1 for v in sample_values if v and v[0].isupper())
            
            if proper_case_count / len(sample_values) > 0.7:
                avg_length = sum(len(str(v)) for v in sample_values) / len(sample_values)
                
                if avg_length < 15:
                    if col_idx == 1:
                        semantic_name = "first_name"
                    elif col_idx == 2:
                        semantic_name = "last_name"
                    else:
                        semantic_name = "name"
                    
                    inferred_columns[f"col_{col_idx}"] = {
                        "semantic_name": semantic_name,
                        "data_type": "TEXT",
                        "confidence": 0.75,
                        "reasoning": "Proper case strings, short length"
                    }
                else:
                    inferred_columns[f"col_{col_idx}"] = {
                        "semantic_name": "text_field",
                        "data_type": "TEXT",
                        "confidence": 0.60,
                        "reasoning": "Text values, longer strings"
                    }
            else:
                inferred_columns[f"col_{col_idx}"] = {
                    "semantic_name": "text_field",
                    "data_type": "TEXT",
                    "confidence": 0.50,
                    "reasoning": "String values without clear pattern"
                }
        
        else:
            inferred_columns[f"col_{col_idx}"] = {
                "semantic_name": "mixed_field",
                "data_type": "TEXT",
                "confidence": 0.40,
                "reasoning": "Mixed data types"
            }
    
    # Calculate overall confidence
    confidences = [col["confidence"] for col in inferred_columns.values()]
    overall_confidence = sum(confidences) / len(confidences) if confidences else 0.0
    
    return {
        "columns": inferred_columns,
        "confidence": overall_confidence,
        "transformations": transformations
    }


@tool
def infer_schema_from_headerless_data(
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()]
) -> Dict[str, Any]:
    """
    DEPRECATED: Use analyze_raw_csv_structure instead.
    
    This tool is kept for backward compatibility but analyze_raw_csv_structure
    provides more comprehensive analysis.
    """
    import re
    
    context = runtime.context
    sample_data = context.file_sample
    
    if not sample_data:
        return {"error": "No sample data provided"}
    
    columns = list(sample_data[0].keys())
    
    # Check if this is actually a headerless file (columns named col_N)
    is_headerless = all(col.startswith('col_') for col in columns)
    
    if not is_headerless:
        return {
            "error": "This file appears to have headers. Schema inference is only for headerless files.",
            "columns": columns
        }
    
    # Analyze each column
    inferred_columns = {}
    
    for col in columns:
        # Extract values for this column
        values = [row.get(col) for row in sample_data if row.get(col) is not None]
        
        if not values:
            inferred_columns[col] = {
                "semantic_name": "unknown",
                "data_type": "TEXT",
                "confidence": 0.0,
                "reasoning": "No non-null values to analyze"
            }
            continue
        
        # Sample first few values for pattern analysis
        sample_values = values[:20]
        
        # Detect data type and semantic meaning
        confidence = 0.0
        semantic_name = "unknown"
        data_type = "TEXT"
        reasoning = ""
        
        # Check for date patterns
        if detect_date_column(sample_values):
            semantic_name = "date"
            data_type = "TIMESTAMP"
            confidence = 0.95
            date_format = infer_date_format(sample_values)
            reasoning = f"Column contains date values in {date_format} format"
        
        # Check for email patterns
        elif any('@' in str(v) and '.' in str(v) for v in sample_values[:10]):
            email_count = sum(1 for v in sample_values if '@' in str(v))
            if email_count / len(sample_values) > 0.7:
                semantic_name = "email"
                data_type = "VARCHAR(255)"
                confidence = 0.98
                reasoning = f"{email_count}/{len(sample_values)} values contain @ symbol"
        
        # Check for numeric patterns
        elif all(isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '').replace('-', '').isdigit()) 
                for v in sample_values):
            # Check if integers
            if all(isinstance(v, int) or (isinstance(v, str) and v.isdigit()) for v in sample_values):
                semantic_name = "id" if col == "col_0" else "number"
                data_type = "INTEGER"
                confidence = 0.85
                reasoning = "All values are integers"
            else:
                semantic_name = "decimal_value"
                data_type = "DECIMAL(10,2)"
                confidence = 0.85
                reasoning = "Values contain decimal numbers"
        
        # Check for name patterns (proper case strings)
        elif all(isinstance(v, str) for v in sample_values):
            # Check if values are proper case (first letter uppercase)
            proper_case_count = sum(1 for v in sample_values if isinstance(v, str) and v and v[0].isupper())
            
            if proper_case_count / len(sample_values) > 0.7:
                # Heuristic: shorter strings are likely first/last names
                avg_length = sum(len(str(v)) for v in sample_values) / len(sample_values)
                
                if avg_length < 15:
                    # Could be first_name or last_name
                    # Use position as hint: col_1 often first_name, col_2 often last_name
                    col_num = int(col.split('_')[1])
                    if col_num == 1:
                        semantic_name = "first_name"
                        confidence = 0.75
                        reasoning = "Proper case strings, short length, position suggests first name"
                    elif col_num == 2:
                        semantic_name = "last_name"
                        confidence = 0.75
                        reasoning = "Proper case strings, short length, position suggests last name"
                    else:
                        semantic_name = "name"
                        confidence = 0.70
                        reasoning = "Proper case strings, short length"
                else:
                    semantic_name = "text_field"
                    confidence = 0.60
                    reasoning = "Text values, longer strings"
                
                data_type = "VARCHAR(255)"
            else:
                semantic_name = "text_field"
                data_type = "TEXT"
                confidence = 0.50
                reasoning = "String values without clear pattern"
        
        else:
            semantic_name = "mixed_field"
            data_type = "TEXT"
            confidence = 0.40
            reasoning = "Mixed data types, defaulting to TEXT"
        
        inferred_columns[col] = {
            "semantic_name": semantic_name,
            "data_type": data_type,
            "confidence": confidence,
            "reasoning": reasoning,
            "sample_values": [str(v)[:50] for v in sample_values[:3]]  # First 3 values, truncated
        }
    
    # Calculate overall confidence
    confidences = [col_info["confidence"] for col_info in inferred_columns.values()]
    overall_confidence = sum(confidences) / len(confidences) if confidences else 0.0
    
    return {
        "is_headerless": True,
        "inferred_columns": inferred_columns,
        "overall_confidence": overall_confidence,
        "column_count": len(columns),
        "recommendation": "Use inferred semantic names for mapping to existing tables"
    }


@tool
def describe_file_purpose(
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()]
) -> Dict[str, Any]:
    """
    Analyze the semantic purpose and business domain of this file.
    
    CRITICAL: This should be your FIRST analysis step before any structural comparison.
    
    Look at the column names and sample data to determine:
    - What is the business purpose of this data? (e.g., "customer contact information for sales")
    - What domain/category does it belong to? (e.g., "contacts", "sales", "inventory")
    - What are the key entities? (e.g., ["customer", "contact", "lead"])
    
    For headerless files (columns named col_0, col_1, etc.), this tool will automatically
    trigger schema inference to understand the data structure first.
    
    Returns:
        Analysis of file's semantic purpose for matching with existing tables
    """
    context = runtime.context
    sample_data = context.file_sample
    
    if not sample_data:
        return {"error": "No sample data provided"}
    
    # Extract column names
    columns = list(sample_data[0].keys())
    
    # Check if this is a headerless file
    is_headerless = all(col.startswith('col_') for col in columns)
    
    # Get a few sample records for context
    sample_records = sample_data[:5]
    
    result = {
        "columns": columns,
        "sample_records": sample_records,
        "is_headerless": is_headerless
    }
    
    if is_headerless:
        result["instruction"] = (
            "HEADERLESS FILE DETECTED (columns named col_0, col_1, etc.)\n\n"
            "You MUST call infer_schema_from_headerless_data tool first to understand "
            "the semantic meaning of each column before proceeding with business purpose analysis.\n\n"
            "After schema inference, analyze:\n"
            "1. Business purpose (what is this data used for?)\n"
            "2. Data domain (contacts, products, sales, etc.)\n"
            "3. Key entities (customer, product, transaction, etc.)"
        )
    else:
        result["instruction"] = (
            "Based on these columns and sample data, determine:\n"
            "1. Business purpose (what is this data used for?)\n"
            "2. Data domain (contacts, products, sales, etc.)\n"
            "3. Key entities (customer, product, transaction, etc.)\n"
            "Provide your analysis in your response."
        )
    
    return result


@tool
def make_import_decision(
    strategy: str,
    target_table: str,
    reasoning: str,
    purpose_short: str,
    column_mapping: Dict[str, str],
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()],
    unique_columns: Optional[List[str]] = None,
    has_header: Optional[bool] = None,
    data_domain: Optional[str] = None,
    key_entities: Optional[List[str]] = None,
    expected_column_types: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Make final import decision with strategy and target table.
    
    This tool should be called when you've completed your analysis and are ready
    to make a recommendation. It records your decision for execution.
    
    Args:
        strategy: Import strategy - one of: NEW_TABLE, MERGE_EXACT, EXTEND_TABLE, ADAPT_DATA
        target_table: Name of target table (for NEW_TABLE, this is the new table name; 
                     for merge strategies, this is the existing table to merge into)
        reasoning: Clear explanation of why this strategy was chosen
        purpose_short: Brief description of what this data is for (e.g., "Customer contact list")
        column_mapping: Map from source columns to target columns (e.g., {"col_0": "date", "col_1": "first_name"})
        unique_columns: List of columns to use for duplicate detection (e.g., ["email", "first_name", "last_name"])
        has_header: For CSV files, whether the file has a header row (True/False). Required for CSV files.
        data_domain: Category/domain (e.g., "contacts", "sales") - optional
        key_entities: List of key entity types (e.g., ["customer", "contact"]) - optional
        expected_column_types: Optional map describing the detected data type for each SOURCE column
            (e.g., {"col_0": "TIMESTAMP", "col_1": "TEXT"}). These types will guide pandas coercion.
        
    Returns:
        Confirmation of decision recorded
    """
    context = runtime.context
    
    # Validate strategy
    valid_strategies = ["NEW_TABLE", "MERGE_EXACT", "EXTEND_TABLE", "ADAPT_DATA"]
    if strategy not in valid_strategies:
        return {
            "error": f"Invalid strategy '{strategy}'. Must be one of: {', '.join(valid_strategies)}"
        }
    
    # Validate column_mapping is provided
    if not column_mapping:
        return {
            "error": "column_mapping is required. Provide a mapping from source columns to target columns."
        }
    
    # For CSV files, has_header should be specified
    file_type = context.file_metadata.get("file_type", "")
    if file_type == "csv" and has_header is None:
        return {
            "error": "has_header is required for CSV files. Specify True if file has headers, False if headerless."
        }
    
    # Store decision in context (will be retrieved by caller)
    context.file_metadata["llm_decision"] = {
        "strategy": strategy,
        "target_table": target_table,
        "reasoning": reasoning,
        "purpose_short": purpose_short,
        "column_mapping": column_mapping,
        "unique_columns": unique_columns or [],
        "has_header": has_header,
        "data_domain": data_domain,
        "key_entities": key_entities or [],
        "expected_column_types": expected_column_types or {}
    }
    
    return {
        "success": True,
        "message": f"Decision recorded: {strategy} into table '{target_table}'",
        "strategy": strategy,
        "target_table": target_table,
        "purpose": purpose_short,
        "column_mapping": column_mapping,
        "has_header": has_header,
        "expected_column_types": expected_column_types or {}
    }


# Global checkpointer instance for conversation memory
_file_analyzer_checkpointer = InMemorySaver()


# Constants for loop prevention
MAX_RETRY_ATTEMPTS = 3
MAX_TOTAL_TOOL_CALLS = 10
ANALYSIS_TIMEOUT_SECONDS = 60


@before_model
def track_analysis_attempts(state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
    """Track analysis attempts and enforce limits to prevent loops."""
    # Initialize counters if not present
    if not hasattr(state, 'attempt_count'):
        return {"attempt_count": 1, "start_time": time.time()}
    
    # Check timeout
    start_time = getattr(state, 'start_time', time.time())
    if time.time() - start_time > ANALYSIS_TIMEOUT_SECONDS:
        logger.warning(f"Analysis timeout exceeded ({ANALYSIS_TIMEOUT_SECONDS}s)")
        raise TimeoutError(f"Analysis exceeded {ANALYSIS_TIMEOUT_SECONDS} second timeout")
    
    # Increment attempt count
    attempt_count = getattr(state, 'attempt_count', 0) + 1
    
    # Check if we've exceeded total tool call limit
    messages = state.get("messages", [])
    tool_call_count = len([m for m in messages if hasattr(m, 'tool_calls') and m.tool_calls])
    
    if tool_call_count >= MAX_TOTAL_TOOL_CALLS:
        logger.warning(f"Max total tool calls exceeded ({MAX_TOTAL_TOOL_CALLS})")
        raise RuntimeError(f"Analysis exceeded maximum of {MAX_TOTAL_TOOL_CALLS} tool calls")
    
    return {"attempt_count": attempt_count}


def create_file_analyzer_agent(max_iterations: int = 5, interactive_mode: bool = False):
    """
    Create an LLM-powered agent for file analysis with conversation memory.
    
    The agent has access to tools for:
    - Analyzing file structure
    - Comparing with existing tables
    - Resolving conflicts
    
    Args:
        max_iterations: Maximum number of tool calls allowed per attempt
        
    Returns:
        Configured LangChain agent with memory and retry support
    """
    model = ChatAnthropic(
        model="claude-haiku-4-5-20251001",  # Much faster than Sonnet
        api_key=settings.anthropic_api_key,
        temperature=0,  # Deterministic for consistent decisions
        max_tokens=4096
    )
    
    tools = [
        describe_file_purpose,
        analyze_raw_csv_structure,
        infer_schema_from_headerless_data,
        analyze_file_structure,
        get_existing_database_schema,
        compare_file_with_tables,
        resolve_conflict,
        make_import_decision
    ]
    
    base_system_prompt = """You are a database consolidation expert helping users organize data from multiple sources.

Your task is to analyze an uploaded file and determine the best way to import it into an existing database.

You can remember previous analysis attempts in this conversation, allowing you to:
- Learn from previous errors or conflicts
- Refine your recommendations based on user feedback
- Retry with different strategies if the first attempt had issues

Available Import Strategies:
1. NEW_TABLE - Data is unique enough to warrant a new table
2. MERGE_EXACT - File matches an existing table's schema exactly
3. EXTEND_TABLE - File is similar to an existing table but has additional columns
4. ADAPT_DATA - File data can be transformed to fit an existing table structure

**CRITICAL FIRST STEP - SEMANTIC ANALYSIS:**
Before ANY structural comparison, you MUST:
1. Call describe_file_purpose to understand what this data is about
2. Analyze the business purpose and domain of the file
3. Get existing table purposes from get_existing_database_schema
4. Look for SEMANTIC matches first (similar business purpose)

Analysis Process (SEMANTIC-FIRST):
1. **FIRST**: Call describe_file_purpose - understand what this data is for
2. Call get_existing_database_schema - see existing tables AND their purposes
3. **SEMANTIC MATCHING**: Compare file purpose with existing table purposes
   - If semantic match found (similar business purpose) → prioritize merging
   - If no semantic match → likely needs NEW_TABLE
4. Call analyze_file_structure for detailed column analysis
5. Call compare_file_with_tables for structural comparison
6. Make decision based on BOTH semantic AND structural fit
7. Call resolve_conflict if needed
8. **FINAL AND REQUIRED**: Call make_import_decision with strategy, target_table, AND purpose information

Decision Priority (MOST IMPORTANT):
- **Semantic match + reasonable structure = MERGE** (even with column name differences)
- **Semantic match + incompatible structure = You decide if reconciliation is possible**
- **No semantic match = NEW_TABLE** (even if some columns overlap)

Example: Two "customer contact list" files with different column names should MERGE because they serve the same business purpose.

Important Considerations:
- Business purpose is MORE important than exact column matches
- Column name variations (e.g., "customer_id" vs "client_id") are acceptable if purpose matches
- Data consolidation benefits should be weighed against data integrity
- Consider existing table usage patterns and row counts
- Provide clear reasoning for your recommendations
- Include confidence scores (0.0 to 1.0) based on match quality

CRITICAL: You have a maximum of {max_iterations} tool calls to complete your analysis.
Be efficient and strategic with your tool usage.

**MANDATORY FINAL STEP:**
You MUST call the make_import_decision tool before providing your final response. This tool records your decision for execution. Do NOT end your analysis without calling this tool. Your response should come AFTER calling make_import_decision, not instead of it.

**CRITICAL: When calling make_import_decision, you MUST provide:**
1. **column_mapping**: A dictionary mapping source columns to target columns
   - For headerless CSV: Map col_0, col_1, etc. to semantic names (e.g., {{"col_0": "date", "col_1": "first_name"}})
   - For files with headers: Map source headers to target table columns (e.g., {{"customer_name": "name", "email_address": "email"}})
   - This mapping is CRITICAL for proper data insertion and duplicate detection

2. **unique_columns**: List of columns to use for duplicate detection
   - Example: ["email", "first_name", "last_name"]
   - These should be the TARGET column names (after mapping)
   - Choose columns that uniquely identify a record

3. **has_header** (CSV files only): True if file has headers, False if headerless
   - This tells the system how to parse the CSV file
   - Use analyze_raw_csv_structure tool to determine this

Output Format:
After calling make_import_decision, provide a structured recommendation including:
- Recommended strategy (NEW_TABLE, MERGE_EXACT, EXTEND_TABLE, or ADAPT_DATA)
- Confidence score (0.0 to 1.0)
- Clear reasoning emphasizing SEMANTIC match
- Business purpose of the data
- If merging/extending: which table to use and why purposes align
- Column mappings (source → target)
- Unique columns for duplicate detection
- Any data quality issues or conflicts found
"""

    if interactive_mode:
        interactive_addendum = """

INTERACTIVE MODE COLLABORATION RULES:
- You are collaborating live with a human reviewer. Present the current import plan and explicitly outline actionable next steps they can choose (e.g., rename columns, change target table, request a new table, confirm duplicates strategy).
- Do NOT call the make_import_decision tool until the user explicitly approves with language such as "CONFIRM", "APPROVE", or "PROCEED". A confirmation message may also contain the token "CONFIRM IMPORT".
- When the user confirms, call make_import_decision exactly once, summarize the final plan, and state that the mapping is ready to execute.
- If the user requests changes after a confirmation, treat it as a revision: withdraw the previous decision, update the plan, and wait for a fresh confirmation before calling make_import_decision again.
- When informed that execution failed (messages beginning with "EXECUTION_FAILED"), diagnose the failure, propose concrete fixes, and wait for the user's confirmation before finalizing a new decision.
- Always respond with: (1) a concise status summary, (2) the recommended plan or revision notes, and (3) a numbered list of suggested next actions for the user.
"""
        system_prompt = f"{base_system_prompt}{interactive_addendum}"
    else:
        system_prompt = base_system_prompt

    agent = create_agent(
        model=model,
        tools=tools,
        system_prompt=system_prompt.format(max_iterations=max_iterations),
        state_schema=FileAnalysisState,
        checkpointer=_file_analyzer_checkpointer,
        middleware=[track_analysis_attempts]
    )
    
    return agent


def analyze_file_for_import(
    file_sample: List[Dict[str, Any]],
    file_metadata: Dict[str, Any],
    analysis_mode: AnalysisMode = AnalysisMode.MANUAL,
    conflict_mode: ConflictResolutionMode = ConflictResolutionMode.ASK_USER,
    user_id: Optional[str] = None,
    max_iterations: int = 5,
    thread_id: Optional[str] = None,
    messages: Optional[List[Dict[str, str]]] = None,
    interactive_mode: bool = False
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
        thread_id: Optional thread ID for conversation continuity. If not provided, uses "default".
        messages: Optional list of conversation messages to send to the agent.
                  If not provided, a default analysis prompt is used.
        interactive_mode: When True, enables the interactive collaboration system prompt.
        
    Returns:
        Analysis results with recommendations
    """
    try:
        # Get existing database schema
        schema_info = get_database_schema()
        
        # Create analysis context
        context = AnalysisContext(
            file_sample=file_sample,
            file_metadata=dict(file_metadata),
            existing_schema=schema_info,
            analysis_mode=analysis_mode,
            conflict_mode=conflict_mode,
            user_id=user_id
        )
        
        # Create and run agent
        agent = create_file_analyzer_agent(
            max_iterations=max_iterations,
            interactive_mode=interactive_mode
        )
        
        # Use default thread if none provided
        if thread_id is None:
            thread_id = "default"
        
        # Create config with thread_id for conversation continuity
        config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
        
        if messages is None:
            prompt = f"""Analyze this file for database import:

File: {file_metadata.get('name', 'unknown')}
Total Rows: {file_metadata.get('total_rows', 'unknown')}
Sample Size: {len(file_sample)}

Please analyze the file structure, compare it with existing tables, and recommend the best import strategy."""
            messages_to_send = [{"role": "user", "content": prompt}]
        else:
            messages_to_send = messages
        
        result = agent.invoke(
            {"messages": messages_to_send},
            context=context,
            config=config
        )
        
        # Extract the agent's response
        final_message = result["messages"][-1]
        response_text = final_message.content if hasattr(final_message, 'content') else str(final_message)
        
        # Count iterations used
        iterations_used = len([m for m in result["messages"] if hasattr(m, 'tool_calls') and m.tool_calls])
        
        # Extract LLM decision if it was made
        llm_decision = context.file_metadata.get("llm_decision")
        
        return {
            "success": True,
            "response": response_text,
            "iterations_used": iterations_used,
            "max_iterations": max_iterations,
            "llm_decision": llm_decision  # Will be None if LLM didn't call make_import_decision
        }
        
    except Exception as e:
        logger.error(f"Error analyzing file: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "response": f"Analysis failed: {str(e)}"
        }

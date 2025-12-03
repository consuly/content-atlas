"""
AI-powered file analysis for intelligent database consolidation.

This module uses LangChain agents with Claude Haiku to analyze uploaded files
and determine the optimal import strategy by comparing with existing database tables.
"""

from typing import List, Dict, Any, Optional, Tuple, Annotated
from typing_extensions import NotRequired
from enum import Enum
import json
import re
import numpy as np
from dataclasses import dataclass
from uuid import uuid4
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
from app.api.schemas.shared import AnalysisMode, ConflictResolutionMode, ensure_safe_table_name
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
    llm_instruction: Optional[str] = None
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
    context = runtime.context
    
    # Check if we have raw CSV rows in metadata
    raw_rows = context.file_metadata.get('raw_csv_rows')
    
    if not raw_rows:
        # Fallback to analyzing processed sample data
        return _infer_schema_from_headerless_data_impl(runtime)
    
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
        result["transformations_needed"] = _detect_column_transformations(first_row, data_rows)
        
    else:
        # First row is data - need to infer column meanings
        result["reasoning"] = "First row contains data values, not headers"
        result["data_starts_at_row"] = 0  # Data starts at row index 0
        
        # Infer semantic column names from data patterns
        inferred_schema = _infer_schema_from_data_rows(raw_rows)
        result["inferred_columns"] = inferred_schema["columns"]
        result["overall_confidence"] = inferred_schema["confidence"]
        result["transformations_needed"] = inferred_schema["transformations"]
    
    if result.get("transformations_needed"):
        # Persist detected transformations for downstream tools (LLM decision, execution)
        context.file_metadata["detected_transformations"] = result["transformations_needed"]
    elif "detected_transformations" in context.file_metadata:
        # Clear previous hints if this run produced none
        context.file_metadata.pop("detected_transformations")

    return result


def _analyze_if_header_row(first_row: List[str], second_row: List[str]) -> bool:
    """
    Determine if the first row is a header or data.
    
    Heuristics:
    - Headers contain descriptive words (name, email, date, id, etc.)
    - Headers don't contain timestamps, emails, or typical data patterns
    - Headers are usually shorter and more uniform
    """
    
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
    if not data_rows or not data_rows[0]:
        return {
            "columns": {},
            "confidence": 0.0,
            "transformations": []
        }
    
    num_columns = len(data_rows[0])
    inferred_columns: Dict[str, Dict[str, Any]] = {}
    transformations: List[Dict[str, Any]] = []
    phone_pattern = re.compile(r'^[\d\s\(\)\.\-]{10,}$')
    
    for col_idx in range(num_columns):
        column_id = f"col_{col_idx}"
        values = [row[col_idx] if col_idx < len(row) else None for row in data_rows]
        values = [v for v in values if v not in (None, "", "null")]
        
        if not values:
            inferred_columns[column_id] = {
                "semantic_name": "unknown",
                "data_type": "TEXT",
                "confidence": 0.0,
                "reasoning": "No values to analyze"
            }
            continue
        
        # Detect multi-value columns (e.g. JSON arrays stored as text)
        multi_value_info = _analyze_multi_value_list(values)
        if multi_value_info:
            transformations.append({**multi_value_info, "column": column_id})
            semantic_name = (
                f"{multi_value_info['item_type']}_list"
                if multi_value_info.get("item_type")
                else "multi_value_list"
            )
            inferred_columns[column_id] = {
                "semantic_name": semantic_name,
                "data_type": "TEXT",
                "confidence": 0.90,
                "reasoning": multi_value_info["reasoning"],
                "sample_values": [
                    ", ".join(str(item) for item in multi_value_info.get("example_items", [])[:3])
                ]
            }
            continue
        
        sample_values = values[:20]
        
        # Check for phone number patterns FIRST (before numeric check)
        phone_matches = sum(1 for v in sample_values if phone_pattern.match(str(v)))
        phone_match_ratio = phone_matches / len(sample_values) if sample_values else 0
        
        if phone_match_ratio > 0.7:
            inferred_columns[column_id] = {
                "semantic_name": "phone",
                "data_type": "TEXT",
                "confidence": 0.95,
                "reasoning": f"{phone_matches}/{len(sample_values)} values match phone number patterns",
                "sample_values": [str(v)[:50] for v in sample_values[:3]]
            }
            continue
        
        # Check for date patterns
        if detect_date_column(sample_values):
            date_format = infer_date_format(sample_values)
            inferred_columns[column_id] = {
                "semantic_name": "date",
                "data_type": "TIMESTAMP",
                "confidence": 0.95,
                "reasoning": f"Contains date values in {date_format} format",
                "sample_values": [str(v)[:50] for v in sample_values[:3]]
            }
            transformations.append({
                "column": column_id,
                "type": "date_standardization",
                "from_format": date_format,
                "to_format": "ISO 8601"
            })
            continue
        
        # Check for email
        if any('@' in str(v) and '.' in str(v) for v in sample_values[:10]):
            email_count = sum(1 for v in sample_values if '@' in str(v))
            inferred_columns[column_id] = {
                "semantic_name": "email",
                "data_type": "TEXT",
                "confidence": 0.98,
                "reasoning": f"{email_count}/{len(sample_values)} values contain @ symbol",
                "sample_values": [str(v)[:50] for v in sample_values[:3]]
            }
            continue
        
        # Check for numeric (only if not phone)
        if all(str(v).replace('.', '').replace('-', '').isdigit() for v in sample_values):
            if all(str(v).isdigit() for v in sample_values):
                inferred_columns[column_id] = {
                    "semantic_name": "id" if col_idx == 0 else "number",
                    "data_type": "INTEGER",
                    "confidence": 0.85,
                    "reasoning": "All values are integers",
                    "sample_values": [str(v)[:50] for v in sample_values[:3]]
                }
            else:
                inferred_columns[column_id] = {
                    "semantic_name": "decimal_value",
                    "data_type": "DECIMAL(10,2)",
                    "confidence": 0.85,
                    "reasoning": "Values contain decimal numbers",
                    "sample_values": [str(v)[:50] for v in sample_values[:3]]
                }
            continue
        
        # Check for proper names
        if all(isinstance(v, str) for v in sample_values):
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
                    
                    inferred_columns[column_id] = {
                        "semantic_name": semantic_name,
                        "data_type": "TEXT",
                        "confidence": 0.75,
                        "reasoning": "Proper case strings, short length",
                        "sample_values": [str(v)[:50] for v in sample_values[:3]]
                    }
                else:
                    inferred_columns[column_id] = {
                        "semantic_name": "text_field",
                        "data_type": "TEXT",
                        "confidence": 0.60,
                        "reasoning": "Text values, longer strings",
                        "sample_values": [str(v)[:50] for v in sample_values[:3]]
                    }
            else:
                inferred_columns[column_id] = {
                    "semantic_name": "text_field",
                    "data_type": "TEXT",
                    "confidence": 0.50,
                    "reasoning": "String values without clear pattern",
                    "sample_values": [str(v)[:50] for v in sample_values[:3]]
                }
            continue
        
        inferred_columns[column_id] = {
            "semantic_name": "mixed_field",
            "data_type": "TEXT",
            "confidence": 0.40,
            "reasoning": "Mixed data types",
            "sample_values": [str(v)[:50] for v in sample_values[:3]]
        }
    
    # Calculate overall confidence
    confidences = [col["confidence"] for col in inferred_columns.values()]
    overall_confidence = sum(confidences) / len(confidences) if confidences else 0.0
    
    return {
        "columns": inferred_columns,
        "confidence": overall_confidence,
        "transformations": transformations
    }


def _analyze_multi_value_list(values: List[Any]) -> Optional[Dict[str, Any]]:
    """
    Detect if the provided values represent JSON array structures stored as text.
    
    Returns metadata describing how the data should be split into separate columns.
    """
    if not values:
        return None
    
    parsed_lists: List[List[Any]] = []
    for value in values:
        if isinstance(value, (list, tuple)):
            parsed = list(value)
        else:
            if not isinstance(value, str):
                continue
            trimmed = value.strip()
            if not trimmed.startswith("[") or not trimmed.endswith("]"):
                continue
            try:
                parsed = json.loads(trimmed)
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
        if not isinstance(parsed, list):
            continue
        parsed_lists.append(parsed)
    
    if not parsed_lists:
        return None
    
    ratio = len(parsed_lists) / len(values)
    if ratio < 0.6:
        return None
    
    lengths = [len(items) for items in parsed_lists]
    if not lengths:
        return None
    
    flat_items = [
        item for items in parsed_lists for item in items
        if item not in (None, "", "null")
    ]
    item_type = "text"
    if flat_items:
        stringified = [str(item) for item in flat_items]
        email_hits = sum(1 for item in stringified if "@" in item and "." in item)
        digit_hits = sum(1 for item in stringified if item.replace(" ", "").replace("-", "").isdigit())
        if email_hits / len(stringified) >= 0.6:
            item_type = "email"
        elif digit_hits / len(stringified) >= 0.6:
            item_type = "number"
    
    reasoning = (
        f"{len(parsed_lists)}/{len(values)} values are list-like data with up to "
        f"{max(lengths)} items"
    )
    
    return {
        "type": "split_multi_value_column",
        "item_type": item_type,
        "max_items": max(lengths),
        "avg_items": sum(lengths) / len(lengths),
        "example_items": parsed_lists[0],
        "reasoning": reasoning
    }


def _detect_multi_value_transformations(
    column_names: List[str],
    data_rows: List[List[str]]
) -> List[Dict[str, Any]]:
    transformations: List[Dict[str, Any]] = []
    if not column_names or not data_rows:
        return transformations
    
    for idx, column_name in enumerate(column_names):
        column_values = []
        for row in data_rows:
            if idx < len(row):
                value = row[idx]
                if value not in (None, "", "null"):
                    column_values.append(value)
        info = _analyze_multi_value_list(column_values)
        if info:
            transformations.append({**info, "column": column_name})
    
    return transformations


def _normalize_column_identifier(name: str) -> str:
    """Normalize column names for easier pattern matching."""
    return re.sub(r'[^a-z0-9]', '', name.lower())


def _extract_digits(value: Optional[Any]) -> str:
    if value is None:
        return ""
    return re.sub(r'\D', '', str(value))


def _compose_phone_from_components(
    country: Optional[Any],
    area: Optional[Any],
    number: Optional[Any],
    extension: Optional[Any] = None
) -> Optional[str]:
    """Combine phone pieces into an international format example."""
    country_digits = _extract_digits(country)
    area_digits = _extract_digits(area)
    number_digits = _extract_digits(number)
    
    if not (country_digits or number_digits):
        return None
    
    combined = ""
    if country_digits:
        combined = f"+{country_digits}"
    else:
        combined = "+"
    
    combined += area_digits + number_digits
    
    extension_digits = _extract_digits(extension)
    if extension_digits:
        combined = f"{combined}x{extension_digits}"
    
    return combined if len(combined) > 1 else None


def _split_international_phone_number(value: Any) -> Optional[Dict[str, str]]:
    """Split international phone numbers into components using simple heuristics."""
    if value in (None, "", "null"):
        return None
    
    text = str(value).strip()
    if not text:
        return None
    
    tokens = re.split(r'[^\d]+', text.lstrip("+"))
    tokens = [token for token in tokens if token]
    digits_only = _extract_digits(text)
    
    if not tokens and len(digits_only) <= 10:
        return None
    
    if tokens:
        country_code = tokens[0]
        national_parts = tokens[1:]
        if national_parts:
            subscriber_number = "".join(national_parts)
        else:
            # Fallback if we only detected a single chunk
            if len(digits_only) > 10:
                country_code = digits_only[:-10]
                subscriber_number = digits_only[-10:]
            else:
                return None
    else:
        country_code = digits_only[:-10]
        subscriber_number = digits_only[-10:]
    
    if not subscriber_number:
        return None
    
    result = {
        "country_code": country_code,
        "subscriber_number": subscriber_number,
    }
    
    # Detect potential area code if the subscriber number looks longer than 10 digits
    if len(subscriber_number) > 10:
        result["area_code"] = subscriber_number[: len(subscriber_number) - 10]
        result["subscriber_number"] = subscriber_number[-10:]
    
    return result


def _collect_column_values(data_rows: List[List[str]], idx: int) -> List[str]:
    values: List[str] = []
    for row in data_rows:
        if idx < len(row):
            value = row[idx]
            if value not in (None, "", "null"):
                values.append(value)
    return values


def _detect_phone_component_transformations(
    column_names: List[str],
    data_rows: List[List[str]]
) -> List[Dict[str, Any]]:
    transformations: List[Dict[str, Any]] = []
    if not column_names or not data_rows:
        return transformations
    
    normalized = [_normalize_column_identifier(name) for name in column_names]
    
    country_indices = [
        idx for idx, name in enumerate(normalized)
        if any(keyword in name for keyword in ("countrycode", "dialcode", "isdcode", "countryprefix"))
    ]
    area_indices = [
        idx for idx, name in enumerate(normalized)
        if any(keyword in name for keyword in ("areacode", "regioncode", "citycode"))
    ]
    extension_indices = [
        idx for idx, name in enumerate(normalized)
        if "extension" in name or name.endswith("ext")
    ]
    international_indices = [
        idx for idx, name in enumerate(normalized)
        if any(keyword in name for keyword in ("internationalphone", "phoneinternational", "fullphone", "intlphone", "e164"))
    ]
    
    subscriber_indices: List[int] = []
    for idx, name in enumerate(normalized):
        if idx in country_indices or idx in area_indices or idx in international_indices or idx in extension_indices:
            continue
        if any(keyword in name for keyword in ("phone", "phonenumber", "mobile", "contactnumber", "cellphone")):
            subscriber_indices.append(idx)
    
    if country_indices and subscriber_indices:
        country_idx = country_indices[0]
        subscriber_idx = subscriber_indices[0]
        area_idx = area_indices[0] if area_indices else None
        extension_idx = extension_indices[0] if extension_indices else None
        
        subscriber_values = _collect_column_values(data_rows, subscriber_idx)
        if subscriber_values:
            plus_ratio = sum(1 for value in subscriber_values if isinstance(value, str) and "+" in value) / len(subscriber_values)
            if plus_ratio < 0.5:
                examples: List[str] = []
                for row in data_rows[:5]:
                    country = row[country_idx] if country_idx < len(row) else None
                    area_val = row[area_idx] if area_idx is not None and area_idx < len(row) else None
                    number = row[subscriber_idx] if subscriber_idx < len(row) else None
                    extension = row[extension_idx] if extension_idx is not None and extension_idx < len(row) else None
                    combined = _compose_phone_from_components(country, area_val, number, extension)
                    if combined:
                        examples.append(combined)
                if examples:
                    components = [
                        {"column": column_names[country_idx], "role": "country_code"}
                    ]
                    if area_idx is not None:
                        components.append({"column": column_names[area_idx], "role": "area_code"})
                    components.append({"column": column_names[subscriber_idx], "role": "subscriber_number"})
                    if extension_idx is not None:
                        components.append({"column": column_names[extension_idx], "role": "extension"})
                    
                    transformations.append({
                        "type": "compose_international_phone",
                        "components": components,
                        "target_format": "E.164",
                        "example_output": examples[:3],
                        "reasoning": "Detected separate phone components that can be combined into a single international number"
                    })
    
    # Detect columns storing full international phone numbers
    for idx in international_indices:
        values = _collect_column_values(data_rows, idx)
        if not values:
            continue
        splits = [_split_international_phone_number(value) for value in values]
        splits = [split for split in splits if split]
        if not splits:
            continue
        ratio = len(splits) / len(values)
        if ratio < 0.6:
            continue
        
        has_area = any("area_code" in split for split in splits)
        output_columns = [
            {"name": "country_code", "role": "country_code"},
        ]
        if has_area:
            output_columns.append({"name": "area_code", "role": "area_code"})
        output_columns.append({"name": "subscriber_number", "role": "subscriber_number"})
        
        transformations.append({
            "type": "split_international_phone",
            "column": column_names[idx],
            "output_columns": output_columns,
            "reasoning": f"{len(splits)}/{len(values)} values follow international phone number patterns",
            "example_components": splits[:3]
        })
    
    return transformations


def _detect_column_transformations(
    column_names: List[str],
    data_rows: List[List[str]]
) -> List[Dict[str, Any]]:
    """Aggregate all transformation hints from the raw data."""
    transformations: List[Dict[str, Any]] = []
    if not column_names or not data_rows:
        return transformations
    
    transformations.extend(_detect_multi_value_transformations(column_names, data_rows))
    transformations.extend(_detect_phone_component_transformations(column_names, data_rows))
    
    return transformations


def _normalize_column_transformations_for_decision(
    transformations: Optional[List[Any]]
) -> List[Dict[str, Any]]:
    """
    Sanitize transformation payloads provided by the LLM or default detection.
    
    Guarantees each entry is a dict with a 'type' field and normalizes common aliases.
    """
    if not transformations:
        return []

    normalized: List[Dict[str, Any]] = []

    for raw in transformations:
        if not isinstance(raw, dict):
            continue

        entry = dict(raw)  # shallow copy to avoid mutating caller state
        t_type = entry.get("type") or entry.get("transform_type") or entry.get("action")

        if not t_type:
            # Heuristic inference if the LLM omitted the type label
            if {"outputs", "source_column"} <= entry.keys():
                t_type = "split_multi_value_column"
            elif entry.get("components"):
                t_type = "compose_international_phone"
            elif entry.get("outputs") and entry.get("column"):
                t_type = "split_international_phone"

        if not t_type:
            logger.debug("Skipping transformation without type descriptor: %s", raw)
            continue

        entry["type"] = t_type

        # Normalize common field aliases
        if t_type == "split_multi_value_column":
            if "source_column" not in entry and "column" in entry:
                entry["source_column"] = entry["column"]
            if "outputs" not in entry and entry.get("targets"):
                entry["outputs"] = entry["targets"]
        elif t_type == "compose_international_phone":
            if "target_column" not in entry and "column" in entry:
                entry["target_column"] = entry["column"]
        elif t_type == "split_international_phone":
            if "source_column" not in entry and "column" in entry:
                entry["source_column"] = entry["column"]
            if "outputs" not in entry and entry.get("targets"):
                entry["outputs"] = entry["targets"]
        elif t_type == "regex_replace":
            if "source_column" not in entry and "column" in entry:
                entry["source_column"] = entry["column"]
            if "target_column" not in entry:
                entry["target_column"] = entry.get("target_field") or entry.get("source_column")
        elif t_type in {"merge_columns", "concat_columns"}:
            if "sources" not in entry and entry.get("columns"):
                entry["sources"] = entry["columns"]
            if "target_column" not in entry:
                entry["target_column"] = entry.get("target_field") or entry.get("column")
            entry["type"] = "merge_columns"  # normalize alias
        elif t_type in {"explode_list_column", "explode_list_values"}:
            if "source_column" not in entry and "column" in entry:
                entry["source_column"] = entry["column"]
            if "outputs" not in entry and entry.get("targets"):
                entry["outputs"] = entry["targets"]
            entry["type"] = "explode_list_column"

        normalized.append(entry)

    return normalized


def _normalize_row_transformations_for_decision(
    transformations: Optional[List[Any]]
) -> List[Dict[str, Any]]:
    """
    Sanitize row-level transformations before storing on the LLM decision.
    Supports explode_columns, filter_rows, regex_replace, and conditional_transform.
    """
    if not transformations:
        return []

    def _infer_filter_regex(columns: Optional[List[str]]) -> Optional[str]:
        if not columns:
            return None
        lowered = [str(col).lower() for col in columns if col]
        if any("email" in col or "mail" in col for col in lowered):
            return r"(?i)[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}"
        return r".+"

    normalized: List[Dict[str, Any]] = []
    for raw in transformations:
        if not isinstance(raw, dict):
            continue
        entry = dict(raw)
        t_type = entry.get("type") or entry.get("action")
        if not t_type:
            continue
        if t_type == "explode_columns":
            sources = entry.get("source_columns") or entry.get("columns") or []
            target = entry.get("target_column") or entry.get("target_field") or entry.get("column")
            if not sources or not target:
                logger.debug("Skipping explode_columns without sources/target: %s", raw)
                continue
            normalized.append(
                {
                    "type": "explode_columns",
                    "source_columns": sources,
                    "target_column": target,
                    "drop_source_columns": entry.get("drop_source_columns", True),
                    "include_original_row": entry.get("include_original_row", False),
                    "keep_empty_rows": entry.get("keep_empty_rows", False),
                    "dedupe_values": entry.get("dedupe_values", True),
                    "case_insensitive_dedupe": entry.get("case_insensitive_dedupe", True),
                    "strip_whitespace": entry.get("strip_whitespace", True),
                }
            )
            continue
        if t_type in {"explode_list_rows", "explode_list"}:
            source_col = entry.get("source_column") or entry.get("column")
            target = entry.get("target_column") or entry.get("target_field") or source_col
            if not source_col or not target:
                logger.debug("Skipping explode_list_rows without source/target: %s", raw)
                continue
            normalized.append(
                {
                    "type": "explode_list_rows",
                    "source_column": source_col,
                    "target_column": target,
                    "delimiter": entry.get("delimiter"),
                    "drop_source_column": entry.get("drop_source_column", True),
                    "include_original_row": entry.get("include_original_row", False),
                    "keep_empty_rows": entry.get("keep_empty_rows", False),
                    "dedupe_values": entry.get("dedupe_values", True),
                    "case_insensitive_dedupe": entry.get("case_insensitive_dedupe", True),
                    "strip_whitespace": entry.get("strip_whitespace", True),
                }
            )
            continue
        if t_type == "filter_rows":
            columns = entry.get("columns") or entry.get("source_columns")
            include_regex = entry.get("include_regex")
            exclude_regex = entry.get("exclude_regex")
            if not include_regex and not exclude_regex:
                include_regex = _infer_filter_regex(columns)
                if include_regex:
                    logger.info(
                        "Normalized filter_rows without regex; inferred include_regex='%s'",
                        include_regex,
                    )
                else:
                    logger.info(
                        "Dropping filter_rows without include/exclude regex or columns: %s",
                        raw,
                    )
                    continue
            normalized.append(
                {
                    "type": "filter_rows",
                    "include_regex": include_regex,
                    "exclude_regex": exclude_regex,
                    "columns": columns,
                }
            )
            continue
        if t_type == "regex_replace":
            columns = entry.get("columns") or entry.get("source_columns") or entry.get("target_columns")
            pattern = entry.get("pattern")
            if not pattern or not columns:
                logger.debug("Skipping regex_replace without pattern/columns: %s", raw)
                continue
            normalized.append(
                {
                    "type": "regex_replace",
                    "pattern": pattern,
                    "replacement": entry.get("replacement", ""),
                    "columns": columns,
                }
            )
            continue
        if t_type == "conditional_transform":
            normalized.append(
                {
                    "type": "conditional_transform",
                    "include_regex": entry.get("include_regex"),
                    "exclude_regex": entry.get("exclude_regex"),
                    "columns": entry.get("columns") or entry.get("source_columns"),
                    "actions": _normalize_row_transformations_for_decision(entry.get("actions") or entry.get("transformations")),
                }
            )
            continue
        if t_type in {"concat_columns", "merge_columns"}:
            sources = entry.get("sources") or entry.get("columns") or []
            target = entry.get("target_column") or entry.get("target_field") or entry.get("column")
            if not sources or not target:
                logger.debug("Skipping concat_columns without sources/target: %s", raw)
                continue
            normalized.append(
                {
                    "type": "concat_columns",
                    "sources": sources,
                    "target_column": target,
                    "separator": entry.get("separator", " "),
                    "strip_whitespace": entry.get("strip_whitespace", True),
                    "skip_nulls": entry.get("skip_nulls", True),
                    "null_replacement": entry.get("null_replacement", ""),
                }
            )
            continue

        normalized.append(entry)

    return normalized


def _infer_schema_from_headerless_data_impl(
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()]
) -> Dict[str, Any]:
    """
    DEPRECATED: Use analyze_raw_csv_structure instead.
    
    This tool is kept for backward compatibility but analyze_raw_csv_structure
    provides more comprehensive analysis.
    """
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
def infer_schema_from_headerless_data(
    runtime: Annotated[ToolRuntime[AnalysisContext], InjectedToolArg()]
) -> Dict[str, Any]:
    """
    DEPRECATED: Use analyze_raw_csv_structure instead.

    This tool is kept for backward compatibility but analyze_raw_csv_structure
    provides more comprehensive analysis.
    """
    return _infer_schema_from_headerless_data_impl(runtime)


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
    expected_column_types: Optional[Dict[str, str]] = None,
    schema_migrations: Optional[List[Dict[str, Any]]] = None,
    column_transformations: Optional[List[Dict[str, Any]]] = None,
    row_transformations: Optional[List[Dict[str, Any]]] = None,
    allow_unique_override: Optional[bool] = None,
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
        expected_column_types: REQUIRED map describing the detected data type for each SOURCE column
            (e.g., {"col_0": "TIMESTAMP", "col_1": "TEXT"}). These types will guide pandas coercion.
        schema_migrations: Optional list of schema migration actions the executor should run
            before importing (e.g., [{"action": "replace_column", ...}]).
        column_transformations: Optional list of source data transformation instructions
            (e.g., split arrays, compose phone numbers) the executor should perform before mapping.
        row_transformations: Optional list of row-level preprocessing instructions (e.g., explode email columns).
        
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
    
    expected_types = expected_column_types or {}
    missing_expected = [source_col for source_col in column_mapping.keys() if source_col not in expected_types]
    if missing_expected:
        return {
            "error": (
                "expected_column_types must include every SOURCE column from column_mapping. "
                f"Missing entries for: {missing_expected}"
            )
        }

    migrations = schema_migrations or []
    for idx, migration in enumerate(migrations):
        if not isinstance(migration, dict) or "action" not in migration:
            return {
                "error": (
                    "schema_migrations must be a list of objects with an 'action' field. "
                    f"Invalid entry at index {idx}."
                )
            }
        action = migration.get("action")
        if action == "add_column":
            new_col = migration.get("new_column") or {}
            if not isinstance(new_col, dict) or not new_col.get("name") or not new_col.get("type"):
                return {
                    "error": (
                        "add_column migration must include new_column with 'name' and 'type'. "
                        f"Invalid entry at index {idx}: expected "
                        "{'action': 'add_column', 'new_column': {'name': 'col', 'type': 'TEXT'}}"
                    )
                }

    forced_table = context.file_metadata.get("forced_target_table")
    forced_table_mode = context.file_metadata.get("forced_target_table_mode")
    if forced_table:
        normalized_forced_table = ensure_safe_table_name(str(forced_table))
        if normalized_forced_table and target_table != normalized_forced_table:
            logger.info(
                "Overriding LLM target table '%s' with user-requested table '%s'",
                target_table,
                normalized_forced_table,
            )
            target_table = normalized_forced_table
        if forced_table_mode == "existing" and strategy == "NEW_TABLE":
            logger.info("Switching strategy to ADAPT_DATA to honor existing-table request")
            strategy = "ADAPT_DATA"
        elif forced_table_mode == "new" and strategy != "NEW_TABLE":
            logger.info("Switching strategy to NEW_TABLE to honor new-table request")
            strategy = "NEW_TABLE"

    # Store decision in context (will be retrieved by caller)
    normalized_transformations = _normalize_column_transformations_for_decision(
        column_transformations
        if column_transformations is not None
        else context.file_metadata.get("detected_transformations")
    )

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
        "expected_column_types": expected_types,
        "schema_migrations": migrations,
        "column_transformations": normalized_transformations,
        "row_transformations": _normalize_row_transformations_for_decision(
            row_transformations
            if row_transformations is not None
            else context.file_metadata.get("detected_row_transformations")
        ),
        "forced_target_table": target_table if forced_table else None,
        "forced_table_mode": forced_table_mode if forced_table else None,
        "llm_instruction": context.llm_instruction,
    }
    
    return {
        "success": True,
        "message": f"Decision recorded: {strategy} into table '{target_table}'",
        "strategy": strategy,
        "target_table": target_table,
        "purpose": purpose_short,
        "column_mapping": column_mapping,
        "has_header": has_header,
        "expected_column_types": expected_types,
        "schema_migrations": migrations,
        "column_transformations": normalized_transformations,
        "row_transformations": context.file_metadata["llm_decision"]["row_transformations"],
        "allow_unique_override": allow_unique_override or False,
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
    api_key = (settings.anthropic_api_key or "").strip()
    if not api_key:
        raise RuntimeError(
            "Anthropic API key not configured. Set ANTHROPIC_API_KEY in your environment "
            "or update settings.anthropic_api_key to enable LLM analysis."
        )

    model = ChatAnthropic(
        model="claude-haiku-4-5-20251001",  # Much faster than Sonnet
        api_key=api_key,
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
9. When analyze_file_structure or other tools surface `transformations_needed`, translate them into explicit preprocessing instructions so the executor can reshape the source columns before mapping.

Schema Remediation Guidance:
- When the schema context lists "Recent Import Issues (Type Mismatches)", you must evaluate those columns before recommending a merge.
- Propose a clear fix: create a new column with the appropriate type, migrate existing values into it, and retire the old column so future imports succeed.
- Explain how the mismatch blocked previous imports and why the migration resolves it.

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
   - If the target table already exists and the schema context shows a prior dedupe key (e.g., “Dedupe key (latest import): …”), REUSE that key unless it is incompatible. Only propose a new key if reuse is impossible, and set allow_unique_override=true when doing so.
   - If the caller provided `target_table_name`/`target_table_mode`, honor that target table instead of inventing a new one. Prefer MERGE/ADAPT/EXTEND into that table when purpose matches; only NEW_TABLE if forced and mode=new.

3. **has_header** (CSV files only): True if file has headers, False if headerless
   - This tells the system how to parse the CSV file
   - Use analyze_raw_csv_structure tool to determine this
4. **expected_column_types**: Provide a SOURCE-column keyed map with detected types
   - Example: {{"email": "TEXT", "signup_date": "TIMESTAMP"}}
   - Supported values: TEXT, INTEGER, DECIMAL, TIMESTAMP, DATE, BOOLEAN
   - If uncertain, default to TEXT rather than omitting the column. Never skip a mapped source column.
5. **column_transformations**: When preprocessing is required (e.g., splitting JSON arrays, composing international phone numbers, normalizing formats), provide a list of transformation instructions.
   - Base each instruction on evidence from `transformations_needed` or your own analysis.
   - Use explicit structures so executors can apply them without guessing:
     • `{"type": "split_multi_value_column", "source_column": "emails", "outputs": [{"name": "email_one", "index": 0}, {"name": "email_two", "index": 1, "default": null}]}`  
     • `{"type": "compose_international_phone", "target_column": "phone_e164", "components": [{"role": "country_code", "column": "country_code"}, {"role": "area_code", "column": "area_code"}, {"role": "subscriber_number", "column": "phone_number"}]}`
     • `{"type": "split_international_phone", "source_column": "intl_phone", "outputs": [{"name": "country_code", "role": "country_code"}, {"name": "subscriber_number", "role": "subscriber_number"}]}`
   - If no preprocessing is needed, pass an empty list (`[]`). Do **not** omit the argument.
6. **row_transformations**: When rows must be duplicated/filtered/cleaned before mapping (e.g., split multiple email columns into entries, drop rows missing valid emails, regex-clean phone numbers), provide explicit instructions.
   
   **CRITICAL - Multi-Column Consolidation Pattern:**
   When the user instruction asks to "create new entries/rows" or "keep only one [field] per row" or "create a single column" from multiple source columns, you MUST use the `explode_columns` transformation. This is the ONLY way to:
   - Consolidate multiple columns (e.g., "Primary Email", "Personal Email") into a single target column
   - Create separate rows for each value (one row per email, one row per phone, etc.)
   - Ensure the target table has only ONE column for that field type
   
   **Example - Consolidating Multiple Email Columns:**
   User says: "Keep only primary and personal email. If row has two emails, create new entry with second email. Create only one email column."
   
   You MUST generate:
   ```
   row_transformations: [
     {
       "type": "explode_columns",
       "source_columns": ["Primary Email", "Personal Email"],
       "target_column": "email",
       "drop_source_columns": true,
       "dedupe_values": true,
       "case_insensitive_dedupe": true
     }
   ]
   column_mapping: {
     "Primary Email": "email",
     "Personal Email": "email"
   }
   db_schema: {
     "email": "TEXT"
   }
   ```
   
   This will:
   - Create ONE "email" column in the target table (not two)
   - For rows with both Primary and Personal email, create TWO separate rows
   - Each row will have only ONE email value
   - Duplicate email values will be removed
   
   **More Examples:**
   • Multiple email columns → single email column with row duplication:
     `{"type": "explode_columns", "source_columns": ["email_1", "email_2", "email_3"], "target_column": "email", "drop_source_columns": true}`
   
   • Multiple phone columns → single phone column with row duplication:
     `{"type": "explode_columns", "source_columns": ["Primary Phone", "Personal Phone", "Work Phone"], "target_column": "phone", "drop_source_columns": true}`
   
   • Filter rows to keep only those with valid emails:
     `{"type": "filter_rows", "include_regex": ".+@.+", "columns": ["email"]}`
   
   • Clean phone numbers with regex:
     `{"type": "regex_replace", "pattern": "[^0-9]", "replacement": "", "columns": ["phone_raw"]}`

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
- Any preprocessing/transformation steps you instructed (reference the column_transformations list)
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
        combined_prompt = f"{base_system_prompt}{interactive_addendum}"
    else:
        combined_prompt = base_system_prompt

    # Use direct replacement to avoid str.format collisions with literal braces in prompt examples.
    system_prompt = combined_prompt.replace("{max_iterations}", str(max_iterations))

    agent = create_agent(
        model=model,
        tools=tools,
        system_prompt=system_prompt,
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
    llm_instruction: Optional[str] = None,
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
            user_id=user_id,
            llm_instruction=llm_instruction,
        )
        
        # Create and run agent
        agent = create_file_analyzer_agent(
            max_iterations=max_iterations,
            interactive_mode=interactive_mode
        )
        
        # Use unique thread per invocation unless caller provides one explicitly
        if thread_id is None:
            thread_id = f"analysis-{uuid4()}"
        
        # Create config with thread_id for conversation continuity
        config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
        
        if messages is None:
            prompt = f"""Analyze this file for database import:

File: {file_metadata.get('name', 'unknown')}
Total Rows: {file_metadata.get('total_rows', 'unknown')}
Sample Size: {len(file_sample)}

Please analyze the file structure, compare it with existing tables, and recommend the best import strategy."""
            forced_table = file_metadata.get("forced_target_table")
            if forced_table:
                mode = file_metadata.get("forced_target_table_mode")
                mode_text = "existing table" if mode == "existing" else "new table" if mode == "new" else "specified table"
                prompt += (
                    f"\nUser request: map the data into the {mode_text} '{forced_table}'. "
                    "Prioritize this target and avoid recommending a different table."
                )
            if llm_instruction:
                prompt += (
                    "\n\nUser instruction (apply this to every file you import):\n"
                    f"{llm_instruction.strip()}"
                )
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
        error_detail = f"{e.__class__.__name__}: {e}"
        logger.error(f"Error analyzing file: {error_detail}", exc_info=True)
        return {
            "success": False,
            "error": error_detail,
            "response": f"Analysis failed: {error_detail}"
        }

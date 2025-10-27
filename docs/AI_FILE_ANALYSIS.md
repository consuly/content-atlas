# AI-Powered File Analysis

Intelligent file analysis system that uses Claude Sonnet to automatically determine the best database import strategy for uploaded files.

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [API Endpoints](#api-endpoints)
- [Configuration Options](#configuration-options)
- [Import Strategies](#import-strategies)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)

---

## Overview

The AI-powered file analysis system uses LangChain agents with Claude Sonnet to intelligently analyze uploaded files and recommend the optimal import strategy. The system:

- **Analyzes file structure** - Examines columns, data types, and data quality
- **Compares with existing tables** - Finds semantic matches in your database
- **Recommends strategies** - Suggests whether to create new tables, merge data, or extend existing tables
- **Handles conflicts** - Resolves data type mismatches and column name variations
- **Provides reasoning** - Explains recommendations with confidence scores

### Key Features

✅ **Smart Sampling** - Adapts sample size based on file size for optimal analysis  
✅ **Iteration Control** - Limits LLM calls to prevent runaway costs (max 5 iterations)  
✅ **Configurable Behavior** - Control auto-execution and conflict resolution  
✅ **Context-Aware** - Uses ToolRuntime to maintain state across tool calls  
✅ **LLM Decision Making** - No hardcoded thresholds - LLM uses reasoning  

---

## How It Works

### Analysis Pipeline

```
1. File Upload
   ↓
2. Smart Sampling (adapts to file size)
   ↓
3. LLM Agent Analysis (max 5 iterations)
   ├─ Analyze file structure
   ├─ Get existing database schema
   ├─ Compare with tables
   └─ Resolve conflicts (if any)
   ↓
4. Generate Recommendation
   ├─ Import strategy
   ├─ Confidence score
   ├─ Suggested mapping
   └─ Reasoning
   ↓
5. User Review (optional)
   ↓
6. Execute Import
```

### Smart Sampling Strategy

The system automatically determines the optimal sample size based on file size:

| File Size | Sample Size | Strategy |
|-----------|-------------|----------|
| ≤ 100 rows | All rows | Use complete dataset |
| 100-1,000 rows | 100 rows | 50 from start + 50 random |
| 1,000-10,000 rows | 200 rows | 50 from start + 150 stratified |
| > 10,000 rows | 500 rows | 50 from start + 450 stratified |

**Why this approach?**
- Beginning rows capture header patterns and initial data
- Stratified sampling ensures good population distribution
- Balances analysis quality with performance

---

## API Endpoints

### POST /analyze-file

Analyze an uploaded file and recommend import strategy.

**Request:**
```bash
curl -X POST "http://localhost:8000/analyze-file" \
  -F "file=@customers.csv" \
  -F "analysis_mode=manual" \
  -F "conflict_resolution=llm_decide" \
  -F "max_iterations=5"
```

**Parameters:**
- `file` (required): File to analyze (CSV, Excel, JSON, or XML)
- `sample_size` (optional): Number of rows to sample (auto-calculated if not provided)
- `analysis_mode` (optional): `manual`, `auto_high`, or `auto_always` (default: `manual`)
- `conflict_resolution` (optional): `ask_user`, `llm_decide`, or `prefer_flexible` (default: `ask_user`)
- `auto_execute_confidence_threshold` (optional): Minimum confidence for auto-execution (default: 0.9)
- `max_iterations` (optional): Maximum LLM iterations (default: 5, max: 10)

**Response:**
```json
{
  "success": true,
  "recommended_strategy": "merge_exact",
  "confidence": 0.95,
  "reasoning": "File structure matches existing 'customers' table with 98% column overlap...",
  "table_matches": [
    {
      "table_name": "customers",
      "similarity_score": 0.98,
      "matching_columns": ["customer_id", "name", "email"],
      "missing_columns": [],
      "extra_columns": ["phone"],
      "reasoning": "Strong semantic match with existing customer data"
    }
  ],
  "selected_table": "customers",
  "suggested_mapping": {
    "table_name": "customers",
    "db_schema": {
      "customer_id": "INTEGER",
      "name": "VARCHAR",
      "email": "VARCHAR"
    },
    "mappings": {
      "customer_id": "id",
      "name": "customer_name",
      "email": "email_address"
    }
  },
  "data_quality_issues": [
    "Column 'email' has 5% null values"
  ],
  "conflicts": [],
  "requires_user_input": false,
  "can_auto_execute": false,
  "iterations_used": 3,
  "max_iterations": 5,
  "llm_response": "Full LLM analysis text..."
}
```

### POST /analyze-b2-file

Analyze a file from Backblaze B2 storage.

**Request:**
```json
{
  "file_name": "data/customers.csv",
  "analysis_mode": "manual",
  "conflict_resolution": "llm_decide",
  "sample_size": 200,
  "max_iterations": 5
}
```

**Response:** Same as `/analyze-file`

### POST /execute-recommended-import

Execute a previously analyzed import recommendation.

**Request:**
```json
{
  "analysis_id": "550e8400-e29b-41d4-a716-446655440000",
  "confirmed_mapping": {
    "table_name": "customers",
    "db_schema": {...},
    "mappings": {...}
  },
  "force_execute": false
}
```

**Response:**
```json
{
  "success": true,
  "message": "Data imported successfully",
  "records_processed": 1500,
  "table_name": "customers"
}
```

---

## Configuration Options

### Analysis Mode

Controls whether the system requires user approval before executing imports:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `manual` | Always requires user review | Default, safest option |
| `auto_high` | Auto-execute if confidence ≥ threshold | Trusted data sources |
| `auto_always` | Always auto-execute | Fully automated pipelines |

**Example:**
```python
# Manual review required
analysis_mode = "manual"

# Auto-execute if confidence ≥ 0.9
analysis_mode = "auto_high"
auto_execute_confidence_threshold = 0.9

# Always auto-execute (use with caution!)
analysis_mode = "auto_always"
```

### Conflict Resolution Mode

Controls how the system handles schema conflicts:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `ask_user` | Stop and request user input | Maximum control |
| `llm_decide` | Let LLM resolve conflicts | Trust AI reasoning |
| `prefer_flexible` | Use most flexible data type | Conservative approach |

**Example:**
```python
# Stop for user input on conflicts
conflict_resolution = "ask_user"

# Let LLM decide based on context
conflict_resolution = "llm_decide"

# Always use TEXT for flexibility
conflict_resolution = "prefer_flexible"
```

---

## Import Strategies

The system recommends one of four strategies:

### 1. NEW_TABLE
**When:** Data is unique and doesn't match existing tables  
**Action:** Create a new table with appropriate schema  
**Example:** First-time import of product inventory data

### 2. MERGE_EXACT
**When:** File matches an existing table's schema exactly  
**Action:** Insert data into existing table  
**Example:** Monthly sales data with consistent format

### 3. EXTEND_TABLE
**When:** File is similar to existing table but has additional columns  
**Action:** Add new columns to existing table, then insert data  
**Example:** Customer data with new "phone" field

### 4. ADAPT_DATA
**When:** File data can be transformed to fit existing schema  
**Action:** Transform and map data to existing table structure  
**Example:** CRM export with different column names ("client_id" → "customer_id")

---

## Usage Examples

### Example 1: Analyze Customer Data

```python
import requests

# Upload and analyze file
with open('customers.csv', 'rb') as f:
    response = requests.post(
        'http://localhost:8000/analyze-file',
        files={'file': f},
        data={
            'analysis_mode': 'manual',
            'conflict_resolution': 'llm_decide',
            'max_iterations': 5
        }
    )

result = response.json()

if result['success']:
    print(f"Strategy: {result['recommended_strategy']}")
    print(f"Confidence: {result['confidence']}")
    print(f"Reasoning: {result['reasoning']}")
    
    # Review and execute if satisfied
    if result['confidence'] > 0.8:
        execute_response = requests.post(
            'http://localhost:8000/execute-recommended-import',
            json={
                'analysis_id': result['analysis_id'],
                'force_execute': False
            }
        )
```

### Example 2: Automated Pipeline

```python
# Fully automated import for trusted sources
response = requests.post(
    'http://localhost:8000/analyze-file',
    files={'file': open('daily_sales.csv', 'rb')},
    data={
        'analysis_mode': 'auto_always',  # Auto-execute
        'conflict_resolution': 'llm_decide',
        'max_iterations': 5
    }
)

# System automatically executes if analysis succeeds
result = response.json()
if result['can_auto_execute']:
    print(f"Auto-executed: {result['records_processed']} records imported")
```

### Example 3: Handle Conflicts

```python
# Let LLM resolve conflicts automatically
response = requests.post(
    'http://localhost:8000/analyze-file',
    files={'file': open('mixed_data.csv', 'rb')},
    data={
        'analysis_mode': 'manual',
        'conflict_resolution': 'llm_decide',  # LLM resolves conflicts
        'max_iterations': 5
    }
)

result = response.json()

if result['conflicts']:
    print(f"Found {len(result['conflicts'])} conflicts")
    for conflict in result['conflicts']:
        print(f"- {conflict['description']}")
        print(f"  Recommended: {conflict['recommended_option']}")
        print(f"  Reasoning: {conflict['reasoning']}")
```

### Example 4: Custom Sample Size

```python
# Analyze large file with custom sample
response = requests.post(
    'http://localhost:8000/analyze-file',
    files={'file': open('huge_dataset.csv', 'rb')},
    data={
        'sample_size': 1000,  # Custom sample size
        'analysis_mode': 'manual',
        'max_iterations': 5
    }
)
```

---

## Best Practices

### 1. Start with Manual Mode

Always start with `analysis_mode="manual"` for new data sources:
```python
analysis_mode = "manual"  # Review before executing
```

### 2. Use Appropriate Sample Sizes

Let the system auto-calculate for most cases:
```python
sample_size = None  # Auto-calculated based on file size
```

Override only for specific needs:
```python
sample_size = 500  # For very large files
sample_size = 50   # For quick testing
```

### 3. Monitor Iteration Usage

Check `iterations_used` to understand LLM efficiency:
```python
if result['iterations_used'] >= result['max_iterations']:
    print("Warning: Reached max iterations, analysis may be incomplete")
```

### 4. Review Confidence Scores

Use confidence scores to guide decisions:
```python
if result['confidence'] >= 0.9:
    # High confidence - safe to auto-execute
    execute_import(result)
elif result['confidence'] >= 0.7:
    # Medium confidence - review recommendation
    review_and_confirm(result)
else:
    # Low confidence - manual mapping recommended
    create_manual_mapping()
```

### 5. Handle Data Quality Issues

Always review data quality warnings:
```python
if result['data_quality_issues']:
    print("Data quality concerns:")
    for issue in result['data_quality_issues']:
        print(f"- {issue}")
    
    # Decide whether to proceed or clean data first
```

### 6. Cost Management

Limit iterations to control API costs:
```python
max_iterations = 5  # Default, good balance
max_iterations = 3  # More cost-conscious
max_iterations = 10 # Maximum allowed, for complex cases
```

### 7. Conflict Resolution Strategy

Choose based on your use case:
```python
# Maximum control
conflict_resolution = "ask_user"

# Trust AI for routine imports
conflict_resolution = "llm_decide"

# Conservative for mixed data
conflict_resolution = "prefer_flexible"
```

---

## Troubleshooting

### Analysis Failed

**Problem:** Analysis returns `success: false`

**Solutions:**
1. Check file format is supported (CSV, Excel, JSON, XML)
2. Verify file is not corrupted
3. Ensure database connection is active
4. Check Anthropic API key is configured

### Low Confidence Scores

**Problem:** Confidence score < 0.7

**Solutions:**
1. Review data quality issues
2. Check if file structure is unusual
3. Consider manual mapping for complex cases
4. Increase sample size for better analysis

### Reached Max Iterations

**Problem:** `iterations_used == max_iterations`

**Solutions:**
1. Increase `max_iterations` (up to 10)
2. Simplify file structure if possible
3. Pre-clean data before upload
4. Check for unusual patterns in data

---

## Related Documentation

- [API Reference](API_REFERENCE.md) - Complete API documentation
- [Architecture](ARCHITECTURE.md) - System design and components
- [Setup Guide](SETUP.md) - Installation and configuration
- [Testing](TESTING.md) - Testing strategies

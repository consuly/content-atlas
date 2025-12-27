# Import Failure Analysis: Marketing Agency - US.csv

## Issue Summary

**File**: Marketing Agency - US.csv  
**Status**: Failed  
**Error**: `invalid input syntax for type integer: "United States"`  
**Row**: 24  
**Table**: marketing_contacts  

## Error Details

### SQL Error
```
(psycopg2.errors.InvalidTextRepresentation) invalid input syntax for type integer: "United States"
LINE 3: ...L, '+11617265845', NULL, 'Management Consulting', 'United St...
```

### Failed Parameters
```python
{
    'company_staff_count': 'United States',  # ❌ STRING value in INTEGER column
    'first_name': 'Danny',
    'last_name': 'Williams',
    'company_name': 'GM Chamber of Commerce',
    ...
}
```

## Root Cause

**Column Mapping Mismatch**: The value `"United States"` (a country name) is being inserted into the `company_staff_count` column, which is defined as `INTEGER` in the database schema.

### Why This Happened

1. **Different Column Structure Between Files**:
   - ✅ **Marketing Agency - Texas.csv**: Successfully imported 1,085 rows
   - ❌ **Marketing Agency - US.csv**: Failed with mapping error
   
2. **Auto-Mapping System Error**:
   - Both files were processed by the LLM-powered analyzer (`app/domain/queries/analyzer.py`)
   - The Texas file established the `marketing_contacts` table schema
   - The US file tried to merge into the same table (correct strategy)
   - **BUT**: The LLM incorrectly mapped columns, causing what appears to be a "Country" or "Location" column → `company_staff_count`

3. **Column Ordering/Naming Differences**:
   - The two CSV files likely have:
     - Different column ordering
     - Different column names for the same data
     - Additional or missing columns
   
   Example scenario:
   ```
   Texas file: ID, Name, Email, Staff_Count, Country
   US file:    ID, Name, Country, Email, Staff_Count  ← Different order!
   ```

## Analysis Flow

### 1. File Analysis (app/domain/queries/analyzer.py)
The LLM analyzer performs:
- `analyze_raw_csv_structure()` - Detects headers and column types
- `compare_file_with_tables()` - Finds matching tables (found `marketing_contacts`)
- `make_import_decision()` - Generates column mapping

### 2. Import Execution (app/integrations/auto_import.py)
The executor:
- Uses the LLM's column mapping: `{source_col: target_col}`
- Coerces data types via pandas: `coerce_records_to_expected_types()`
- Inserts into database

### 3. Failure Point
The type coercion failed because:
- LLM said: `"col_X"` should map to `"company_staff_count"` (INTEGER)
- But `"col_X"` actually contains country names like "United States"
- Pandas tried: `pd.to_numeric("United States")` → `NaN` (coerced)
- Database rejected the value at insertion time

## Comparison: Success vs Failure

| Aspect | Texas File ✅ | US File ❌ |
|--------|--------------|------------|
| Status | Success | Failed |
| Rows Processed | 1,085 | 0 |
| Table | marketing_contacts | marketing_contacts |
| Mapping | Correct | Incorrect |
| Error | None | Type mismatch on row 24 |

## Technical Details

### Type Coercion Logic (auto_import.py:445-536)
```python
def coerce_records_to_expected_types(
    records: List[Dict[str, Any]],
    expected_types: Dict[str, str],
    existing_schema: Optional[Dict[str, str]] = None
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Use pandas to coerce incoming records to expected types.
    When existing_schema is provided, respects existing column types
    and converts data accordingly.
    """
```

The coercion process:
1. LLM provides `expected_column_types` (e.g., `{"col_5": "INTEGER"}`)
2. Pandas tries to convert: `pd.to_numeric(series, errors="coerce")`
3. Invalid values become `None`
4. Database insertion happens
5. **FAILURE**: Non-null string "United States" reaches database

### Why Coercion Didn't Catch This
The `errors="coerce"` parameter converts invalid values to `NaN`/`None`, but the actual error suggests the string value reached the database, meaning:
- Either coercion wasn't applied to this column
- Or the LLM didn't mark it as requiring INTEGER type
- Or the mapping was so wrong that the entire type expectation was incorrect

## Solution Options

### Option 1: Manual Re-Mapping (Recommended)
1. View the US file's actual column structure
2. Manually create correct column mapping
3. Re-import with corrected mapping

### Option 2: CSV Structure Investigation
Examine both files to understand structural differences:
```python
# Compare column headers
texas_headers = ["Research Date", "First Name", ..., "Company Staff Count", "Country"]
us_headers = ["Research Date", "First Name", ..., "Country", "Company Staff Count"]
#                                                    ^^^ Different order!
```

### Option 3: LLM Analysis Improvement
Enhance the analyzer to:
- Better detect column semantic meaning (not just position)
- Use sample data validation before finalizing mapping
- Provide confidence scores for each column mapping
- Flag uncertain mappings for user review

### Option 4: Type Validation Before Insert
Add a validation layer in `auto_import.py`:
```python
def validate_mapping_against_schema(
    records: List[Dict[str, Any]],
    column_mapping: Dict[str, str],
    target_schema: Dict[str, str]
) -> List[str]:
    """
    Validate that mapped data matches target schema types.
    Returns list of errors found.
    """
    errors = []
    for col, type in target_schema.items():
        sample_values = [r.get(col) for r in records[:100] if r.get(col)]
        if type == "INTEGER":
            non_numeric = [v for v in sample_values if not _is_numeric_like(v)]
            if non_numeric:
                errors.append(f"Column '{col}' expects INTEGER but found: {non_numeric[:3]}")
    return errors
```

## Immediate Action Required

1. **Access the original US.csv file** to examine its structure
2. **Compare column headers** between Texas and US files
3. **Manually map columns** for the US file
4. **Re-import** with correct mapping

## Prevention Recommendations

1. **Add Pre-Import Validation**:
   - Check sample data against expected types
   - Flag mismatches before attempting insert
   
2. **Improve LLM Column Matching**:
   - Use semantic analysis (column names + sample data)
   - Don't rely solely on position matching
   
3. **User Review for Similar Files**:
   - When processing multiple files for same table
   - Show mapping diff between files
   - Require confirmation if mappings differ significantly

4. **Better Error Messages**:
   - Show the actual column mapping used
   - Highlight which source column caused the error
   - Suggest correct mapping based on error analysis

## Related Code Files

- **Analyzer**: `app/domain/queries/analyzer.py` (LLM-powered file analysis)
- **Executor**: `app/integrations/auto_import.py` (Import execution)
- **Fingerprinting**: `app/domain/imports/fingerprinting.py` (Schema matching)
- **Type Coercion**: `auto_import.py:445-536` (Pandas type conversion)

## Debugging Commands

```bash
# View import history
python investigate_import_failure.py

# Check table schema
psql -d content_atlas -c "\d marketing_contacts"

# View failed import details
SELECT * FROM import_history WHERE status = 'failed' ORDER BY import_timestamp DESC LIMIT 1;

# Check file uploads table
SELECT * FROM file_uploads WHERE file_name LIKE '%Marketing Agency%';
```

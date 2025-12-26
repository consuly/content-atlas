# Data Models & Schema Guide

This guide details the supported file formats, data types, validation rules, and transformation capabilities of the ContentAtlas. Use this reference to prepare your data files for successful import.

## Table of Contents
- [Supported File Formats](#supported-file-formats)
- [Type Mapping & Validation](#type-mapping--validation)
- [Transformation Rules](#transformation-rules)
- [Deduplication Strategies](#deduplication-strategies)
- [Schema Configuration Templates](#schema-configuration-templates)

---

## Supported File Formats

The system supports the following formats. For best results, ensure your files are UTF-8 encoded.

### 1. CSV (.csv)
- **Header Row**: Recommended. If missing, columns are named `col_0`, `col_1`, etc.
- **Delimiter**: Comma (`,`) is default.
- **Quoting**: Standard double-quote encapsulation for fields containing delimiters.

### 2. Excel (.xlsx, .xls)
- **Structure**: Data should be in the first sheet by default.
- **Headers**: First row is treated as the header.
- **Formulas**: Values are extracted; formulas are not preserved.

### 3. JSON (.json)
- **Structure**: Must be an array of flat objects.
  ```json
  [
    {"name": "Alice", "email": "alice@example.com"},
    {"name": "Bob", "email": "bob@example.com"}
  ]
  ```
- **Nested Objects**: Currently not supported directly; flatten your JSON before import.

### 4. Archives (.zip)
- **Behavior**: The system extracts the archive and processes valid files (CSV/Excel) found within.
- **Grouping**: Files with similar structures (same headers) are grouped and mapped to the same target table automatically.

---

## Type Mapping & Validation

The system automatically detects and validates data types during import.

| SQL Type | Logic & Validation | Example Valid Values |
| :--- | :--- | :--- |
| **INTEGER** | Whole numbers only. Decimal values (e.g., `1.0`) are converted if whole, otherwise rejected. Currency symbols (`$`) and commas are stripped automatically. | `123`, `"$1,200"`, `1.0` |
| **DECIMAL** | Floating point numbers. | `10.50`, `"$19.99"` |
| **TIMESTAMP** | Auto-detects formats (ISO 8601, US, etc.). Converts to UTC ISO 8601. | `2023-01-01`, `01/31/2023`, `Jan 1, 2023` |
| **BOOLEAN** | Case-insensitive match. | `true`, `Yes`, `1`, `T` |
| **TEXT** | Default for strings. Preserves formatting. | `Any string` |

**Validation Failure Behavior**:
If a value cannot be coerced to the target type (e.g., "ABC" for an INTEGER column), the value is set to `NULL` for that row, and a warning is logged. The row is **not** rejected unless it violates a specific uniqueness constraint.

---

## Transformation Rules

You can define rules in the `mapping_json` to clean data *before* it enters the database.

### 1. Split Multi-Value Column
Splits a cell containing multiple values (e.g., "tag1, tag2") into separate columns.

```json
{
  "type": "split_multi_value_column",
  "column": "tags",
  "delimiter": ",",
  "outputs": [
    {"index": 0, "target_column": "tag_primary"},
    {"index": 1, "target_column": "tag_secondary"}
  ]
}
```

### 2. Merge Columns
Combines multiple columns into one.

```json
{
  "type": "merge_columns",
  "columns": ["first_name", "last_name"],
  "target_column": "full_name",
  "separator": " "
}
```

### 3. Regex Replace
Cleans data using Regular Expressions.

```json
{
  "type": "regex_replace",
  "column": "sku",
  "pattern": "^SKU-",
  "replacement": ""
}
```

### 4. Standardize Phone
Formats phone numbers to E.164 standard (e.g., `+14155552671`).

```json
{
  "type": "standardize_phone",
  "column": "phone_number",
  "default_country_code": "1"
}
```

---

## Deduplication Strategies

### File-Level Deduplication
- **Mechanism**: SHA-256 hash of the entire file content.
- **Behavior**: If you upload the exact same file twice, the second import is rejected immediately to prevent processing overhead.
- **Override**: Admins can force-import duplicates if necessary.

### Row-Level Deduplication
- **Mechanism**: Checks for existing records in the target table based on configured **Unique Columns**.
- **Configuration**:
  ```json
  "duplicate_check": {
    "enabled": true,
    "unique_columns": ["email"],
    "check_file_level": true
  }
  ```
- **Behavior**: Rows matching existing data in the `unique_columns` are skipped. New rows are inserted.

---

## Schema Configuration Templates

Use these JSON templates in your API calls (`mapping_json` field).

### Customer Import Template
*Validates emails and standardizes phone numbers.*

```json
{
  "table_name": "customers",
  "db_schema": {
    "customer_id": "INTEGER",
    "email": "VARCHAR(255)",
    "phone": "VARCHAR(20)",
    "signup_date": "TIMESTAMP"
  },
  "mappings": {
    "customer_id": "ID",
    "email": "Email Address",
    "phone": "Contact Phone",
    "signup_date": "Joined"
  },
  "rules": {
    "column_transformations": [
      {
        "type": "standardize_phone",
        "column": "Contact Phone",
        "target_column": "phone",
        "default_country_code": "1"
      }
    ]
  },
  "duplicate_check": {
    "enabled": true,
    "unique_columns": ["email"]
  }
}
```

### Sales Transaction Template
*Handles currency parsing and ID deduplication.*

```json
{
  "table_name": "sales_transactions",
  "db_schema": {
    "transaction_id": "VARCHAR(50)",
    "amount": "DECIMAL",
    "product_name": "VARCHAR(100)",
    "sale_date": "TIMESTAMP"
  },
  "mappings": {
    "transaction_id": "Txn ID",
    "amount": "Total Price",
    "product_name": "Item",
    "sale_date": "Date"
  },
  "duplicate_check": {
    "enabled": true,
    "unique_columns": ["transaction_id"]
  }
}
```

# Import Consolidation Architecture

## Overview

This document describes the unified import orchestration layer that centralizes all data import operations in Content Atlas. This consolidation eliminates code duplication and ensures consistent behavior across all import entry points.

## Problem Statement

Previously, the system had multiple entry points for data imports, each with duplicated logic:

1. **Manual Local Upload** (`/map-data`) - Direct file upload with manual mapping
2. **Manual B2 Upload** (`/map-b2-data`) - B2 file with manual mapping
3. **Async B2 Upload** (`/map-b2-data-async`) - Background processing for large B2 files
4. **AI-Powered Import** (`/analyze-file` with AUTO_ALWAYS mode) - LLM-driven automatic import

Each endpoint duplicated the same core logic:
- File processing (CSV, Excel, JSON, XML)
- Data mapping and transformation
- Schema compatibility checking
- Table creation
- Record insertion
- Import tracking
- Metadata management

This duplication created maintenance challenges and increased the risk of inconsistent behavior.

## Solution: Unified Import Orchestrator

We created a centralized `execute_data_import()` function in `app/domain/imports/orchestrator.py` that all endpoints now use.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     API Endpoints                            │
├─────────────────────────────────────────────────────────────┤
│  /map-data          │  /map-b2-data    │  /analyze-file     │
│  (local upload)     │  (B2 upload)     │  (AI-powered)      │
│                     │                  │                     │
│  /map-b2-data-async │  execute_llm_    │                    │
│  (background task)  │  import_decision │                    │
└──────────┬──────────┴──────────┬───────┴──────────┬─────────┘
           │                     │                   │
           └─────────────────────┼───────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  execute_data_import() │
                    │  (Unified Orchestrator)│
                    └────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
         ┌──────────▼──────────┐   ┌─────────▼──────────┐
         │  File Processing    │   │  Import Tracking   │
         │  - CSV, Excel, etc  │   │  - Start/Complete  │
         └──────────┬──────────┘   └─────────┬──────────┘
                    │                         │
         ┌──────────▼──────────┐   ┌─────────▼──────────┐
         │  Data Mapping       │   │  Metadata Mgmt     │
         │  - Transform data   │   │  - Store/Enrich    │
         └──────────┬──────────┘   └─────────┬──────────┘
                    │                         │
         ┌──────────▼──────────┐   ┌─────────▼──────────┐
         │  Schema Transform   │   │  Table Creation    │
         │  - Compatibility    │   │  - Dynamic schema  │
         └──────────┬──────────┘   └─────────┬──────────┘
                    │                         │
                    └────────────┬────────────┘
                                 │
                      ┌──────────▼──────────┐
                      │  Record Insertion   │
                      │  - Bulk insert      │
                      │  - Duplicate check  │
                      └─────────────────────┘
```

## Core Function: `execute_data_import()`

Located in `app/domain/imports/orchestrator.py`, this function orchestrates the entire import process:

```python
def execute_data_import(
    file_content: bytes,
    file_name: str,
    mapping_config: MappingConfig,
    source_type: str,  # "local_upload" or "b2_storage"
    source_path: Optional[str] = None,
    import_strategy: Optional[str] = None,
    metadata_info: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Central function for all data imports.
    
    Orchestrates:
    1. File processing
    2. Data mapping
    3. Schema transformation (if needed)
    4. Table creation (if needed)
    5. Data insertion
    6. Import tracking
    7. Metadata management
    """
```

### Key Features

1. **File Type Detection**: Automatically detects CSV, Excel, JSON, XML
2. **Smart Processing**: Uses chunked processing for large Excel files (>50MB)
3. **Schema Transformation**: Handles merge strategies (MERGE_EXACT, EXTEND_TABLE, ADAPT_DATA)
4. **Import Tracking**: Comprehensive tracking with start/complete timestamps
5. **Metadata Management**: Stores or enriches table metadata based on strategy
6. **Error Handling**: Consistent error handling with proper cleanup

## Refactored Endpoints

### 1. `/map-data` (Local Upload)

**Before** (100+ lines):
```python
# Duplicated logic for file processing, mapping, insertion, tracking
```

**After** (20 lines):
```python
@app.post("/map-data", response_model=MapDataResponse)
async def map_data_endpoint(file: UploadFile, mapping_json: str):
    from .import_orchestrator import execute_data_import
    
    config = MappingConfig(**json.loads(mapping_json))
    file_content = await file.read()
    
    result = execute_data_import(
        file_content=file_content,
        file_name=file.filename,
        mapping_config=config,
        source_type="local_upload"
    )
    
    return MapDataResponse(
        success=True,
        records_processed=result["records_processed"],
        table_name=result["table_name"]
    )
```

### 2. `/map-b2-data` (B2 Upload)

**Before** (100+ lines):
```python
# Duplicated logic with B2 download
```

**After** (25 lines):
```python
@app.post("/map-b2-data", response_model=MapDataResponse)
async def map_b2_data_endpoint(request: MapB2DataRequest):
    from .import_orchestrator import execute_data_import
    
    file_content = download_file_from_b2(request.file_name)
    
    result = execute_data_import(
        file_content=file_content,
        file_name=request.file_name,
        mapping_config=request.mapping,
        source_type="b2_storage",
        source_path=request.file_name
    )
    
    return MapDataResponse(
        success=True,
        records_processed=result["records_processed"],
        table_name=result["table_name"]
    )
```

### 3. `process_b2_data_async()` (Background Task)

**Before** (80+ lines):
```python
# Duplicated logic with progress updates
```

**After** (40 lines):
```python
def process_b2_data_async(task_id: str, file_name: str, mapping: MappingConfig):
    from .import_orchestrator import execute_data_import
    
    # Update progress
    task_storage[task_id] = AsyncTaskStatus(status="processing", ...)
    
    file_content = download_file_from_b2(file_name)
    
    result = execute_data_import(
        file_content=file_content,
        file_name=file_name,
        mapping_config=mapping,
        source_type="b2_storage",
        source_path=file_name
    )
    
    # Update completion status
    task_storage[task_id] = AsyncTaskStatus(status="completed", ...)
```

### 4. `execute_llm_import_decision()` (AI-Powered)

**Before** (150+ lines):
```python
# Duplicated logic with schema transformation
```

**After** (40 lines):
```python
def execute_llm_import_decision(
    file_content: bytes,
    file_name: str,
    all_records: List[Dict],
    llm_decision: Dict
) -> Dict:
    from .import_orchestrator import execute_data_import
    
    # Detect mapping and override table name
    _, detected_mapping, _, _ = detect_mapping_from_file(file_content, file_name)
    detected_mapping.table_name = llm_decision["target_table"]
    
    # Prepare metadata
    metadata_info = {
        "purpose_short": llm_decision.get("purpose_short"),
        "data_domain": llm_decision.get("data_domain"),
        "key_entities": llm_decision.get("key_entities", [])
    }
    
    result = execute_data_import(
        file_content=file_content,
        file_name=file_name,
        mapping_config=detected_mapping,
        source_type="local_upload",
        import_strategy=llm_decision["strategy"],
        metadata_info=metadata_info
    )
    
    return {
        "success": True,
        "strategy_executed": llm_decision["strategy"],
        "table_name": result["table_name"],
        "records_processed": result["records_processed"]
    }
```

## Benefits

### 1. Code Reduction
- **Before**: ~400 lines of duplicated logic across 4 entry points
- **After**: ~200 lines in unified orchestrator + ~100 lines in endpoints
- **Reduction**: ~50% less code to maintain

### 2. Consistency
- All imports use identical logic for processing, mapping, and insertion
- Consistent error handling and tracking across all entry points
- Uniform behavior for duplicate detection and schema transformation

### 3. Maintainability
- Single source of truth for import logic
- Changes to import behavior only need to be made in one place
- Easier to add new features (e.g., new file formats, validation rules)

### 4. Testability
- Can test core import logic independently of endpoints
- Easier to mock and test edge cases
- Reduced test duplication

### 5. Extensibility
- Easy to add new import sources (e.g., FTP, SFTP, cloud storage)
- Simple to add new import strategies
- Straightforward to enhance with additional features

## Testing

All existing tests pass without modification:

### API Tests (`tests/test_api.py`)
- ✅ 13/13 tests passing
- Tests cover: local upload, B2 upload, async processing, duplicate detection

### LLM Sequential Merge Test (`tests/test_llm_sequential_merge.py`)
- ✅ 1/1 test passing
- Tests AI-powered import with schema merging

## Migration Guide

### For Developers

If you need to add a new import endpoint:

1. **Don't duplicate logic** - Use `execute_data_import()`
2. **Prepare inputs**:
   - Get file content (download, upload, etc.)
   - Create `MappingConfig` object
   - Determine `source_type` and optional `import_strategy`
3. **Call orchestrator**:
   ```python
   from .import_orchestrator import execute_data_import
   
   result = execute_data_import(
       file_content=file_content,
       file_name=file_name,
       mapping_config=config,
       source_type="your_source_type",
       import_strategy="optional_strategy",
       metadata_info={"optional": "metadata"}
   )
   ```
4. **Handle result**:
   ```python
   return YourResponse(
       success=True,
       records_processed=result["records_processed"],
       table_name=result["table_name"]
   )
   ```

### For API Users

**No changes required** - All existing API endpoints work exactly as before with identical behavior.

## Future Enhancements

With the unified orchestrator in place, we can easily add:

1. **New Import Sources**:
   - FTP/SFTP servers
   - Cloud storage (S3, Azure Blob, Google Cloud Storage)
   - Database connections (MySQL, MongoDB, etc.)
   - API integrations

2. **Enhanced Features**:
   - Data validation rules
   - Custom transformation pipelines
   - Scheduled imports
   - Import templates
   - Rollback capabilities

3. **Performance Optimizations**:
   - Parallel processing for multiple files
   - Streaming for very large files
   - Caching for repeated imports

## Related Documentation

- [API Reference](./API_REFERENCE.md) - Complete API documentation
- [Architecture](./ARCHITECTURE.md) - Overall system architecture
- [Import Tracking](./IMPORT_TRACKING.md) - Import history and lineage
- [Duplicate Detection](./DUPLICATE_DETECTION.md) - Duplicate prevention strategies

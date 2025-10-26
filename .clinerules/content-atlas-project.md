## Brief overview

Project-specific guidelines for Content Atlas - a data consolidation platform designed for SMBs to consolidate data from multiple sources into PostgreSQL. The system uses LLMs to process and retrieve data, enabling natural language queries with exact answers through intelligent joins and complex requests. Core focus on avoiding duplicates, dynamic schema management, and multi-format data ingestion.

## Core library dependencies

### Data Processing with Pandas
- **Pandas is the primary library** for all data manipulation, transformation, and file processing operations
- When working with data imports, CSV/Excel processing, or data transformations, always reference pandas documentation and best practices
- Use pandas DataFrames for chunked processing (10,000 record chunks) and bulk database operations
- Leverage pandas' `to_sql()` method for efficient bulk inserts
- For data processing tasks, consult pandas documentation via MCP tools (upstash/context7-mcp) before implementation

### LLM Operations with LangChain
- **LangChain is the framework** for all LLM interactions, agent-based queries, and AI-powered features
- When implementing natural language queries, SQL generation, or AI features, reference LangChain documentation and patterns
- Use LangChain's agent patterns for database querying and context management
- Maintain database schema context for accurate SQL generation
- For LLM/AI features, consult LangChain documentation before implementation

## Technical stack

- **Backend**: FastAPI with async/await patterns for REST APIs
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Data Processing**: Pandas for transformations, NumPy for numerical operations
- **File Formats**: openpyxl (Excel), lxml (XML), native CSV/JSON support
- **LLM Integration**: Anthropic Claude via LangChain
- **Cloud Storage**: Backblaze B2 SDK
- **Validation**: Pydantic models for request/response schemas

## Architecture patterns

### Data Processing Flow
- Two-phase processing: parallel duplicate checking followed by sequential insertion
- Chunked processing for files >10,000 records (10K record chunks)
- File-level duplicate detection using SHA-256 hashing
- Row-level duplicate detection based on configurable unique columns
- Dynamic schema creation based on mapping configuration

### Code Organization
- Modular file processors using strategy pattern (`app/processors/`)
- Separate concerns: processors parse, mapper transforms and inserts
- Background task processing for long-running operations (>10K records)
- Async endpoints for large file processing with task tracking

### Database Operations
- Use SQLAlchemy ORM for schema management
- Bulk inserts via pandas `to_sql()` for performance
- Connection pooling (pool_size=20, max_overflow=10)
- Dynamic table creation with configurable schemas

## Coding conventions

### File Structure
- Keep files under 500 lines (global rule)
- Split large files into logical modules when approaching limit
- Use descriptive module names with hyphens (e.g., `csv-processor.py`)

### Type Hints and Validation
- Use type hints throughout the codebase
- Pydantic models for all API request/response schemas
- Validate data types during mapping and transformation

### Async Patterns
- Use async/await for I/O-bound operations (file uploads, database queries)
- Synchronous processing for CPU-bound operations (data transformation)
- Background tasks for operations >30 seconds

### Error Handling
- Specific exception types for different error scenarios
- Clear error messages for duplicate detection failures
- Rollback support for failed batch operations

## Data consolidation requirements

### Duplicate Prevention
- File-level: Track imported files by SHA-256 hash in `file_imports` table
- Row-level: Check uniqueness based on configured columns before insertion
- Configurable duplicate detection (can be disabled per import)
- Clear error messages indicating which records are duplicates

### Schema Management
- Support dynamic table creation from mapping configuration
- Allow merging data into existing tables
- Validate data types match target schema
- Support common PostgreSQL types: INTEGER, VARCHAR, DECIMAL, TIMESTAMP, BOOLEAN

### Data Mapping
- Flexible field mapping from source to target columns
- Support for transformation rules (future enhancement)
- Type coercion with error handling
- Preserve data integrity during transformation

## Natural language query interface

### LLM-Powered Console
- Use LangChain agents for SQL generation from natural language
- Maintain database schema context for accurate query generation
- Read-only queries for safety (SELECT only)
- Rich terminal output with formatted tables
- Error handling for invalid or unsafe queries

### Query Accuracy
- Provide complete schema context to LLM (tables, columns, types)
- Support complex joins across multiple tables
- Handle aggregations and filtering in natural language
- Return exact results, not approximations

## Performance considerations

### File Processing
- Small files (<1K records): Single-threaded, <2 seconds
- Medium files (1K-10K): Optimized bulk insert, 2-10 seconds
- Large files (>10K): Chunked with parallel duplicate checking, 10-30+ seconds
- Memory management: Process in chunks to avoid exhaustion

### Database Operations
- Use bulk inserts over individual row inserts
- Minimize database round-trips
- Connection pooling for concurrent requests
- Index unique columns for faster duplicate detection

## Testing approach

- Unit tests for individual processors and mapper functions
- Integration tests for end-to-end data import flows
- Test with various file formats and sizes
- Validate duplicate detection logic
- Test error handling and rollback scenarios

## Documentation standards

- Maintain comprehensive API documentation in `docs/API_REFERENCE.md`
- Document architecture decisions in `docs/ARCHITECTURE.md`
- Keep README.md updated with quick start and examples
- Include inline comments for complex logic
- Document configuration options and environment variables

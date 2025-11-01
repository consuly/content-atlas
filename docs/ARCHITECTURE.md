# Architecture Overview

System architecture and design documentation for the Data Mapper API.

## Table of Contents

- [System Overview](#system-overview)
- [Architecture Diagram](#architecture-diagram)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Technology Stack](#technology-stack)
- [Design Decisions](#design-decisions)
- [Scalability Considerations](#scalability-considerations)

---

## System Overview

The Data Mapper API is a Python-based application that provides a flexible system for importing data from various file formats (CSV, Excel, JSON, XML) into a PostgreSQL database with configurable schema mapping and transformation rules.

### Key Capabilities

- **Multi-format Support**: CSV, Excel, JSON, and XML file processing
- **Dynamic Schema Creation**: Automatically creates database tables based on mapping configuration
- **Intelligent Duplicate Detection**: File-level and row-level duplicate checking
- **Large File Processing**: Chunked processing with parallel duplicate checking for files >10,000 records
- **Cloud Storage Integration**: Direct integration with Backblaze B2 storage
- **Natural Language Queries**: AI-powered console for database queries
- **Async Processing**: Background task processing for long-running operations

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Layer                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   Web UI     │  │   CLI Tool   │  │  API Client  │         │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘         │
└─────────┼──────────────────┼──────────────────┼─────────────────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
┌────────────────────────────┼─────────────────────────────────────┐
│                    API Gateway (FastAPI)                         │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    REST Endpoints                        │   │
│  │  /map-data  /map-b2-data  /tables  /detect-mapping     │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────────┬─────────────────────────────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
┌─────────▼─────────┐ ┌──────▼──────┐ ┌────────▼────────┐
│  File Processors  │ │   Mapper    │ │  Query Agent    │
│  ┌─────────────┐  │ │  ┌────────┐ │ │  ┌───────────┐  │
│  │ CSV Parser  │  │ │  │ Schema │ │ │  │ LLM (AI)  │  │
│  │ Excel Parser│  │ │  │ Creator│ │ │  │ SQL Gen   │  │
│  │ JSON Parser │  │ │  │ Data   │ │ │  └───────────┘  │
│  │ XML Parser  │  │ │  │ Mapper │ │ │                 │
│  └─────────────┘  │ │  └────────┘ │ │                 │
└─────────┬─────────┘ └──────┬──────┘ └────────┬────────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│                    Data Processing Layer                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Duplicate   │  │   Parallel   │  │     Type     │          │
│  │  Detection   │  │  Processing  │  │   Coercion   │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└────────────────────────────┬─────────────────────────────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
┌─────────▼─────────┐ ┌──────▼──────┐ ┌────────▼────────┐
│   PostgreSQL      │ │  Backblaze  │ │  Task Queue     │
│   Database        │ │     B2      │ │  (Async Jobs)   │
│  ┌─────────────┐  │ │  Storage    │ │                 │
│  │ Dynamic     │  │ │             │ │                 │
│  │ Tables      │  │ │             │ │                 │
│  │ file_imports│  │ │             │ │                 │
│  └─────────────┘  │ │             │ │                 │
└───────────────────┘ └─────────────┘ └─────────────────┘
```

---

## Core Components

### 1. API Layer (FastAPI)

**Location**: `app/main.py`

The API layer provides RESTful endpoints for all operations:

- **Data Import Endpoints**: File upload and B2 integration
- **Table Management**: Query, schema, and statistics endpoints
- **Async Processing**: Background task management
- **Auto-generated Documentation**: Swagger UI and ReDoc

**Key Features**:
- Automatic request validation using Pydantic models
- Exception handling with appropriate HTTP status codes
- CORS support for web clients
- File upload handling with multipart/form-data

### 2. File Processors

**Location**: `app/processors/`

Modular processors for different file formats:

- **CSV Processor** (`csv_processor.py`): Handles CSV files using pandas
- **JSON Processor** (`json_processor.py`): Parses JSON data structures
- **XML Processor** (`xml_processor.py`): Processes XML documents

**Design Pattern**: Strategy pattern for pluggable file format support

**Common Interface**:
```python
def process(file_content: bytes) -> List[Dict[str, Any]]:
    """Process file and return list of records"""
    pass
```

### 3. Data Mapper

**Location**: `app/mapper.py`

Core component responsible for:

- **Schema Creation**: Dynamic table creation based on mapping configuration
- **Data Transformation**: Applying transformation rules to data
- **Type Coercion**: Converting data types to match database schema
- **Duplicate Detection**: File-level and row-level duplicate checking
- **Bulk Insertion**: Efficient data insertion using pandas

**Key Methods**:
- `create_table()`: Creates database tables dynamically
- `map_and_insert()`: Main orchestration method
- `_check_duplicates()`: Duplicate detection logic
- `_insert_records_chunked()`: Chunked processing for large files

### 4. Database Layer

**Location**: `app/database.py`

Database connection and session management:

- **SQLAlchemy ORM**: Object-relational mapping
- **Connection Pooling**: Efficient connection reuse
- **Session Management**: Context managers for transactions
- **Migration Support**: Schema evolution capabilities

**Configuration**:
```python
engine = create_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True
)
```

### 5. B2 Integration

**Location**: `app/integrations/b2.py`

Backblaze B2 cloud storage integration:

- **File Download**: Retrieve files from B2 buckets
- **Authentication**: API key-based authentication
- **Error Handling**: Retry logic and error recovery

### 6. Query Agent (Console)

**Location**: `app/query_agent.py`, `app/console.py`

Natural language query interface:

- **LLM Integration**: Uses Anthropic Claude for SQL generation
- **Context Management**: Maintains database schema context
- **Query Execution**: Safe, read-only query execution
- **Result Formatting**: Rich terminal output with tables

### 7. Async Task Processing

**Location**: `app/models.py` (TaskStatus)

Background task management for long-running operations:

- **Task Tracking**: UUID-based task identification
- **Progress Monitoring**: Real-time progress updates
- **Result Storage**: Task results and error messages
- **Status Management**: Pending, processing, completed, failed states

---

## Data Flow

### Standard Import Flow

```
1. Client uploads file + mapping configuration
   ↓
2. API validates request and extracts file
   ↓
3. File processor parses file into records
   ↓
4. Mapper validates schema and creates table (if needed)
   ↓
5. Duplicate detection checks (if enabled)
   ↓
6. Type coercion applied to records
   ↓
7. Records inserted into database
   ↓
8. File import tracked in file_imports table
   ↓
9. Success response returned to client
```

### Large File Processing Flow

```
1. Client uploads large file (>10,000 records)
   ↓
2. File parsed and split into chunks
   ↓
3. PHASE 1: Parallel Duplicate Checking
   ├─ Pre-load existing data once
   ├─ Check all chunks in parallel (4 workers)
   └─ Aggregate results
   ↓
4. PHASE 2: Sequential Insertion
   ├─ For each chunk:
   │  ├─ Apply type coercion
   │  ├─ Bulk insert to database
   │  └─ Log progress
   └─ Complete
   ↓
5. Record file import metadata
   ↓
6. Return success response
```

### B2 Integration Flow

```
1. Client sends B2 file reference + mapping
   ↓
2. API authenticates with B2
   ↓
3. File downloaded from B2 bucket
   ↓
4. Standard import flow continues
   ↓
5. Temporary file cleaned up
```

---

## Technology Stack

### Backend Framework
- **FastAPI**: Modern, fast web framework for building APIs
- **Uvicorn**: ASGI server for running FastAPI applications
- **Pydantic**: Data validation using Python type annotations

### Data Processing
- **Pandas**: Data manipulation and analysis
- **NumPy**: Numerical computing support
- **openpyxl**: Excel file reading/writing
- **lxml**: XML processing

### Database
- **PostgreSQL**: Primary relational database
- **SQLAlchemy**: SQL toolkit and ORM
- **psycopg2**: PostgreSQL adapter for Python

### Cloud Integration
- **b2sdk**: Backblaze B2 SDK for Python

### AI/ML
- **Anthropic SDK**: Claude AI integration for natural language queries

### Development Tools
- **pytest**: Testing framework
- **Docker**: Containerization
- **Docker Compose**: Multi-container orchestration

---

## Design Decisions

### 1. Why FastAPI?

**Chosen**: FastAPI
**Alternatives Considered**: Flask, Django REST Framework

**Reasons**:
- Automatic API documentation (Swagger/OpenAPI)
- Built-in request validation with Pydantic
- Async support for better performance
- Modern Python features (type hints)
- Excellent performance benchmarks

### 2. Why Pandas for Data Processing?

**Chosen**: Pandas
**Alternatives Considered**: Native Python, Polars

**Reasons**:
- Mature ecosystem with extensive documentation
- Efficient vectorized operations
- Built-in support for multiple file formats
- Easy integration with databases via `to_sql()`
- Excellent for data transformation and cleaning

### 3. Why PostgreSQL?

**Chosen**: PostgreSQL
**Alternatives Considered**: MySQL, MongoDB

**Reasons**:
- Robust support for dynamic schema creation
- ACID compliance for data integrity
- Excellent performance for analytical queries
- Rich data type support
- Strong community and tooling

### 4. Chunked Processing Strategy

**Decision**: Process large files in 10,000-record chunks

**Reasons**:
- Balances memory usage and performance
- Optimal size for pandas DataFrame operations
- Allows progress tracking
- Prevents memory exhaustion on large files

### 5. Two-Phase Processing (Parallel Check + Sequential Insert)

**Decision**: Separate duplicate checking from insertion

**Reasons**:
- Duplicate checking is CPU-bound (benefits from parallelism)
- Database insertion is I/O-bound (sequential is safer)
- Avoids race conditions and transaction conflicts
- Better error handling and rollback capabilities

### 6. File-Level Duplicate Detection

**Decision**: Use SHA-256 hashing for file identification

**Reasons**:
- Fast computation
- Collision-resistant
- Standard cryptographic hash
- Small storage footprint (64 characters)

---

## Scalability Considerations

### Horizontal Scaling

**Current State**: Single instance
**Future**: Multiple API instances behind load balancer

**Considerations**:
- Stateless API design enables easy horizontal scaling
- Database connection pooling must be configured per instance
- Shared database ensures consistency
- Task queue needed for distributed async processing

### Database Scaling

**Current State**: Single PostgreSQL instance
**Future**: Read replicas for query endpoints

**Strategy**:
- Write operations to primary database
- Read operations (table queries) to replicas
- Connection pooling to manage connections efficiently

### File Processing Optimization

**Current Optimizations**:
- Chunked processing for large files
- Parallel duplicate checking
- Bulk inserts using pandas

**Future Enhancements**:
- Distributed processing using Celery
- Caching layer (Redis) for frequently accessed data
- Stream processing for real-time data ingestion

### Storage Considerations

**Current**: PostgreSQL for all data
**Future**: Tiered storage strategy

**Options**:
- Hot data: PostgreSQL (recent imports)
- Warm data: Compressed tables (older data)
- Cold data: Archive to object storage (S3/B2)

---

## Security Architecture

### Authentication & Authorization

**Current**: No authentication (internal use)
**Production Recommendations**:
- API key authentication
- JWT tokens for user sessions
- Role-based access control (RBAC)

### Data Protection

**Implemented**:
- Environment variables for secrets
- Database connection encryption
- Input validation and sanitization

**Recommended**:
- Encryption at rest for sensitive data
- Audit logging for all operations
- Rate limiting to prevent abuse

### Network Security

**Recommended**:
- HTTPS/TLS for all API communication
- VPC/private network for database
- Firewall rules limiting access
- DDoS protection

---

## Performance Characteristics

### Throughput

| Operation | Records/Second | Notes |
|-----------|---------------|-------|
| Small files (<1K) | 500-1000 | Single-threaded |
| Medium files (1K-10K) | 1000-2000 | Optimized bulk insert |
| Large files (>10K) | 2000-5000 | Parallel processing |

### Latency

| Endpoint | Typical Response Time | Notes |
|----------|----------------------|-------|
| /map-data (small) | 100-500ms | Includes validation |
| /map-data (large) | 5-30s | Depends on file size |
| /tables | 50-100ms | Simple query |
| /tables/{name} | 100-500ms | With pagination |

### Resource Usage

| Component | Memory | CPU | Notes |
|-----------|--------|-----|-------|
| API (idle) | 100-200MB | <5% | Base overhead |
| Processing (small) | 200-500MB | 10-30% | Per request |
| Processing (large) | 500MB-2GB | 50-100% | Chunked processing |

---

## Related Documentation

- [API Reference](API_REFERENCE.md) - Complete API documentation
- [Setup Guide](SETUP.md) - Installation and configuration
- [Deployment Guide](DEPLOYMENT.md) - Production deployment
- [Duplicate Detection](DUPLICATE_DETECTION.md) - Duplicate detection system
- [Parallel Processing](PARALLEL_PROCESSING.md) - Large file processing

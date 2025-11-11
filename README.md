# Data Mapper API

A Python FastAPI application that accepts documents (CSV, Excel, JSON, XML) with mapping configurations and maps the data to a PostgreSQL database according to the specified schema.

## Features

- 📁 **Multi-Format Support**: CSV, Excel, JSON, and XML file processing
- 🗄️ **Dynamic Schema Creation**: Automatically creates database tables based on configuration
- 🔄 **Flexible Data Mapping**: Configurable field mappings with transformation rules
- 🚀 **High Performance**: Chunked processing with parallel duplicate checking for large files
- ☁️ **Cloud Integration**: Direct integration with Backblaze B2 storage
- 🔍 **Smart Duplicate Detection**: File-level and row-level duplicate checking
- 💬 **Natural Language Queries**: AI-powered console for database queries
- ⚡ **Async Processing**: Background task processing for long-running operations
- 📊 **RESTful API**: Complete REST API with automatic documentation

## Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Docker & Docker Compose (optional)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd content-atlas
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start PostgreSQL**
   ```bash
   docker-compose up -d db
   ```

4. **Configure environment**
   ```bash
   # Create .env file
   echo "DATABASE_URL=postgresql://postgres:postgres@localhost:5432/datamapper" > .env
   ```

5. **Run the application**
   ```bash
   uvicorn app.main:app --reload
   ```

6. **Access the API**
   - API: http://localhost:8000
   - Interactive Docs: http://localhost:8000/docs

## Documentation

### Getting Started
- 📖 [Setup Guide](docs/SETUP.md) - Detailed installation and configuration
- 🏗️ [Architecture Overview](docs/ARCHITECTURE.md) - System design and components
- 🧪 [Testing Guide](docs/TESTING.md) - Testing strategies and examples

### API Documentation
- 🔌 [API Reference](docs/API_REFERENCE.md) - Complete endpoint documentation
- 🔄 [Duplicate Detection](docs/DUPLICATE_DETECTION.md) - Duplicate detection system
- ⚡ [Parallel Processing](docs/PARALLEL_PROCESSING.md) - Large file processing

### Operations
- 🚀 [Deployment Guide](docs/DEPLOYMENT.md) - Production deployment
- 💻 [Console Interface](docs/CONSOLE.md) - Natural language query console
- 🔄 [Database Reset](docs/DATABASE_RESET.md) - Reset database for testing

## API Endpoints

### Data Import
- `POST /map-data` - Upload and map file data
- `POST /map-b2-data` - Import from Backblaze B2
- `POST /map-b2-data-async` - Async processing for large files

### Data Analysis
- `POST /detect-b2-mapping` - Auto-detect schema from file
- `POST /extract-b2-excel-csv` - Preview Excel file contents

### Table Management
- `GET /tables` - List all tables
- `GET /tables/{table_name}` - Query table data
- `GET /tables/{table_name}/schema` - Get table schema
- `GET /tables/{table_name}/stats` - Get table statistics

### Task Management
- `GET /tasks/{task_id}` - Check async task status

See [API Reference](docs/API_REFERENCE.md) for detailed documentation.

## Usage Example

```bash
# Upload a CSV file with mapping configuration
curl -X POST "http://localhost:8000/map-data" \
  -F "file=@customers.csv" \
  -F 'mapping_json={
    "table_name": "customers",
    "db_schema": {
      "id": "INTEGER",
      "name": "VARCHAR(255)",
      "email": "VARCHAR(255)"
    },
    "mappings": {
      "id": "Customer ID",
      "name": "Customer Name",
      "email": "Email Address"
    }
  }'
```

## Console Interface

Query your database using natural language:

```bash
# Start interactive console
python -m app.console

# Or run a single query
python -m app.console "Show me all customers"
```

See [Console Interface Guide](docs/CONSOLE.md) for more details.

## Docker Deployment

### Quick Start with Docker Compose

```bash
# Start the complete stack
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

See [Deployment Guide](docs/DEPLOYMENT.md) for production deployment.

## Environment Variables

### Required
- `DATABASE_URL` - PostgreSQL connection string

### Optional (for B2 integration)
- `B2_APPLICATION_KEY_ID` - Backblaze B2 key ID
- `B2_APPLICATION_KEY` - Backblaze B2 application key
- `B2_BUCKET_NAME` - B2 bucket name

### Optional (for console)
- `ANTHROPIC_API_KEY` - Anthropic API key for natural language queries

## Development

### Running Tests

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=app

# Run specific tests
pytest tests/test_api.py -v

# Skip B2 analysis tests (e.g., when B2/LLM creds unavailable)
pytest -m "not b2"

# Start development server
uvicorn app.main:app --reload
```

### Database Reset (Development Only)

Reset the database to a clean state while preserving user accounts:

```bash
# Interactive mode with confirmation
python reset_dev_db.py

# Auto-confirm (for automation)
python reset_dev_db.py --yes
```

This will:
- Drop all user-created data tables
- Clear tracking tables (file_imports, table_metadata, import_history, import_jobs, uploaded_files)
- Delete all files from B2 storage
- **Preserve** user accounts

⚠️ **Production Safety**: The script automatically detects and blocks production environments.

See [Database Reset Guide](docs/DATABASE_RESET.md) for detailed documentation.

## Project Structure

```
content-atlas/
├── app/
│   ├── main.py              # FastAPI application
│   ├── mapper.py            # Data mapping logic
│   ├── database.py          # Database connection
│   ├── console.py           # Natural language console
│   ├── processors/          # File format processors
│   │   ├── csv_processor.py
│   │   ├── json_processor.py
│   │   └── xml_processor.py
│   └── ...
├── docs/                    # Documentation
│   ├── API_REFERENCE.md
│   ├── SETUP.md
│   ├── TESTING.md
│   ├── CONSOLE.md
│   ├── DEPLOYMENT.md
│   ├── ARCHITECTURE.md
│   ├── DATABASE_RESET.md
│   ├── DUPLICATE_DETECTION.md
│   └── PARALLEL_PROCESSING.md
├── tests/                   # Test suite
├── docker-compose.yml       # Docker orchestration
├── Dockerfile              # Container definition
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Key Features Explained

### Duplicate Detection
Intelligent duplicate detection at both file and row levels. Configure uniqueness columns, enable/disable checks, and customize error messages. See [Duplicate Detection](docs/DUPLICATE_DETECTION.md).

### Large File Processing
Automatic chunked processing for files >10,000 records with parallel duplicate checking. Handles files up to 100MB+ efficiently. See [Parallel Processing](docs/PARALLEL_PROCESSING.md).

### Dynamic Schema Creation
Tables are created automatically based on your mapping configuration. Supports multiple data types including INTEGER, VARCHAR, DECIMAL, and TIMESTAMP.

### Cloud Storage Integration
Direct integration with Backblaze B2 for importing files from cloud storage without manual downloads.

## Performance

- **Small files** (<1,000 records): <2 seconds
- **Medium files** (1,000-10,000 records): 2-10 seconds
- **Large files** (10,000-50,000 records): 10-30 seconds
- **Very large files** (>50,000 records): 30+ seconds with async processing

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests.

## License

[Your License Here]

## Support

For issues, questions, or contributions:
- 📖 Check the [documentation](docs/)
- 🐛 Report bugs via GitHub issues
- 💬 Ask questions in discussions

## Related Projects

- [FastAPI](https://fastapi.tiangolo.com/) - Web framework
- [Pandas](https://pandas.pydata.org/) - Data processing
- [PostgreSQL](https://www.postgresql.org/) - Database
- [Backblaze B2](https://www.backblaze.com/b2/) - Cloud storage

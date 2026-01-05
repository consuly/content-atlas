# ContentAtlas by Consuly.ai

**Consolidate your business data from multiple sources into one powerful, queryable database.**

ContentAtlas is a data consolidation platform designed for SMBs to import data from CSV, Excel, JSON, and XML files into PostgreSQL, then query it using natural language powered by AI.

🌐 **Official Website:** [atlas.consuly.ai](https://atlas.consuly.ai)

---

## What is ContentAtlas?

ContentAtlas solves the problem of scattered business data across multiple spreadsheets and systems. It helps you:

- **Consolidate data** from various sources into a single PostgreSQL database
- **Query naturally** using AI - ask questions in plain English, get exact answers
- **Eliminate duplicates** with intelligent file and row-level duplicate detection
- **Process at scale** with support for large files and cloud storage integration

Built for small and medium businesses that need data insights without complex data engineering.

---

## ✨ Key Features

- 📁 **Multi-Format Import** - CSV, Excel, JSON, and XML file support
- 🤖 **AI-Powered Queries** - Ask questions in natural language, get precise SQL results
- 🔍 **Smart Duplicate Detection** - Prevent duplicate imports at file and row levels
- ☁️ **Cloud Storage** - Direct integration with Backblaze B2 storage
- 🗄️ **Dynamic Schemas** - Automatically create database tables from your data
- ⚡ **High Performance** - Chunked processing for files with 10,000+ records
- 🔄 **Workflow Automation** - Process ZIP archives and automate imports
- 🔐 **Secure API** - RESTful API with authentication and role-based access

---

## 🚀 Quick Start

### Backend Setup

```bash
# 1. Clone the repository
git clone https://github.com/thefoundry-app/content-atlas.git
cd content-atlas

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Start PostgreSQL (using Docker)
docker-compose up -d db

# 4. Configure environment
cp .env.example .env
# Edit .env with your database connection details

# 5. Run the application
uvicorn app.main:app --reload
```

### Frontend Setup

```bash
# 1. Navigate to frontend directory
cd frontend

# 2. Install dependencies
npm install

# 3. Configure environment
cp .env.example .env
# Edit .env to set VITE_API_URL=http://localhost:8000

# 4. Start development server
npm run dev
```

For advanced deployment options (Docker, Static Hosting, Railway), please see [docs/FRONTEND_SETUP.md](docs/FRONTEND_SETUP.md).

### Access Your Instance

- **API:** http://localhost:8000
- **Frontend:** http://localhost:5173
- **API Documentation:** http://localhost:8000/docs

**First-Time Login:**
Create an admin user to access to dashboard:
```bash
python create_admin_user.py
```

---

## 📚 Documentation

### Official Documentation
- 📘 **[Documentation Home](https://atlas.consuly.ai/documentation/)** - Complete guide to ContentAtlas
- 🚀 **[Getting Started](https://atlas.consuly.ai/documentation/getting-started/)** - Step-by-step setup and first import
- 🔌 **[API Reference](https://atlas.consuly.ai/documentation/api/)** - Complete API endpoint documentation

### Project Guides
- 🏗️ [Architecture Overview](docs/ARCHITECTURE.md) - System design and components
- 🖥️ [Frontend Setup Guide](docs/FRONTEND_SETUP.md) - Detailed frontend installation & deployment
- 🧪 [Testing Guide](docs/TESTING.md) - Running tests and development practices
- 🚀 [Deployment Guide](docs/DEPLOYMENT.md) - Production deployment instructions
- 💻 [Console Interface](docs/CONSOLE.md) - Natural language query console
- 🔧 [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues and solutions
- 📈 [Scalability and Performance](docs/SCALABILITY_AND_PERFORMANCE.md) - Handling large datasets
- 🛡️ [Database Reset](docs/DATABASE_RESET.md) - How to reset/clear database

---

## 💡 Example Usage

### Import a CSV File

```bash
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
      "name": "Full Name",
      "email": "Email Address"
    }
  }'
```

### Query with Natural Language

```bash
# Start of interactive console
python -m app.console

# Ask questions in plain English
> "Show me all customers from Texas"
> "What's the total revenue by product category?"
> "List contacts added in the last 30 days"
```

---

## 🛠️ Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=app
```

### Resetting the Database

```bash
python reset_dev_db.py
```
⚠️ **Warning**: This drops all tables and clears data.

### Environment Variables

Create a `.env` file based on `.env.example`:

```env
# Required
DATABASE_URL=postgresql://user:password@localhost:5432/datamapper

# Optional - For B2 cloud storage integration
STORAGE_ACCESS_KEY_ID=your_key_id
STORAGE_SECRET_ACCESS_KEY=your_secret_key
STORAGE_BUCKET_NAME=your_bucket_name
STORAGE_PROVIDER=b2

# Optional - For AI-powered natural language queries
ANTHROPIC_API_KEY=your_anthropic_key
```

---

## 🤝 Support & Community

- 🌐 **Website:** [atlas.consuly.ai](https://atlas.consuly.ai)
- 🐛 **Report Issues:** [GitHub Issues](https://github.com/thefoundry-app/content-atlas/issues)
- 💬 **Discussions:** [GitHub Discussions](https://github.com/thefoundry-app/content-atlas/discussions)
- 🤝 **Contributing:** [Contribution Guide](CONTRIBUTING.md)

---

## 💼 Commercial Services

Need a private installation, managed hosting, or enterprise support?

- **Private Installation**: On-premise setup with SLA and support.
- **Professional Services**: Custom integrations and data migration.

[Contact Sales](mailto:sales@consuly.ai)

---

## 📄 License

This project is licensed under the Business Source License 1.1 (BSL 1.1).
See [LICENSE](LICENSE) file for details.

---

**Built by [Consuly.ai](https://consuly.ai)**

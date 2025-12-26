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

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher
- Docker (optional, for easier database setup)

### Installation

```bash
# Clone the repository
git clone https://github.com/thefoundry-app/content-atlas.git
cd content-atlas

# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL (using Docker)
docker-compose up -d db

# Configure environment
cp .env.example .env
# Edit .env with your database connection details

# Run the application
uvicorn app.main:app --reload
```

### Access Your Instance

- **API:** http://localhost:8000
- **API Documentation:** http://localhost:8000/docs
- **Frontend:** http://localhost:5173 (see [Frontend Setup](#-frontend-setup) below)

---

## 🖥️ Frontend Setup

The ContentAtlas frontend provides a web dashboard for data import, table browsing, and natural language queries.

### Quick Start

```bash
# Navigate to frontend directory
cd frontend

# Install dependencies
npm install

# Configure API connection
cp .env.example .env
# Edit .env and set: VITE_API_URL=http://localhost:8000

# Start development server
npm run dev
```

The frontend will be available at **http://localhost:5173**

### Connecting Frontend to Backend

The frontend communicates with the backend API via the `VITE_API_URL` environment variable:

**Development (.env):**
```env
VITE_API_URL=http://localhost:8000
```

**Production:**
- For static builds: Set `VITE_API_URL` before building
- For Docker: Set `API_URL` environment variable (runtime configuration)

The backend must be running and accessible from the frontend. CORS is already configured in the FastAPI backend to allow frontend connections.

### Production Deployment

#### Option 1: Static Hosting (Vercel, Netlify)

```bash
cd frontend
npm run build
# Deploy the dist/ folder to your hosting provider
```

Set environment variable on your hosting platform:
- `VITE_API_URL=https://your-api-domain.com`

#### Option 2: Docker Container

The frontend includes a production Dockerfile:

```bash
cd frontend
docker build -t content-atlas-frontend .
docker run -p 3000:3000 -e API_URL=https://your-api-domain.com content-atlas-frontend
```

#### Option 3: Railway Deployment

Deploy both backend and frontend from the same monorepo:

1. **Backend service**: Uses root `Dockerfile`
2. **Frontend service**: Set `RAILWAY_DOCKERFILE_PATH=frontend/Dockerfile`

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md#railway-deployment) for detailed Railway setup.

#### Option 4: Complete Stack with Docker Compose

Add frontend to `docker-compose.yml`:

```yaml
services:
  db:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: datamapper
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data

  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://postgres:postgres@db:5432/datamapper
    depends_on:
      - db

  frontend:
    build: ./frontend
    ports:
      - "3000:3000"
    environment:
      API_URL: http://api:8000
    depends_on:
      - api

volumes:
  postgres_data:
```

Run the complete stack:
```bash
docker-compose up -d
```

Access at:
- **Frontend**: http://localhost:3000
- **API**: http://localhost:8000

### First-Time Setup

Create an admin user to access the dashboard:

```bash
python create_admin_user.py
```

Then login at http://localhost:5173/login with your credentials.

For detailed frontend documentation, see [docs/FRONTEND_SETUP.md](docs/FRONTEND_SETUP.md).

---

## 📖 Documentation

### Official Documentation
- 📘 **[Documentation Home](https://atlas.consuly.ai/documentation/)** - Complete guide to ContentAtlas
- 🚀 **[Getting Started](https://atlas.consuly.ai/documentation/getting-started/)** - Step-by-step setup and first import
- 🔌 **[API Reference](https://atlas.consuly.ai/documentation/api/)** - Complete API endpoint documentation

### Additional Resources
- 🏗️ [Architecture Overview](docs/ARCHITECTURE.md) - System design and components
- 🧪 [Testing Guide](docs/TESTING.md) - Running tests and development practices
- 🚀 [Deployment Guide](docs/DEPLOYMENT.md) - Production deployment instructions
- 💻 [Console Interface](docs/CONSOLE.md) - Natural language query console
- 🔧 [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues and solutions

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
# Start the interactive console
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

# Run specific test file
pytest tests/test_api.py -v
```

### Environment Variables

Create a `.env` file based on `.env.example`:

```env
# Required
DATABASE_URL=postgresql://user:password@localhost:5432/datamapper

# Optional - For B2 cloud storage integration
STORAGE_ACCESS_KEY_ID=your_key_id
STORAGE_SECRET_ACCESS_KEY=your_secret_key
STORAGE_BUCKET_NAME=your_bucket_name
STORAGE_ENDPOINT_URL=https://s3.us-west-000.backblazeb2.com
STORAGE_PROVIDER=b2

# Optional - For AI-powered natural language queries
ANTHROPIC_API_KEY=your_anthropic_key
```

### Docker Deployment

```bash
# Start complete stack (API + Database)
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

---

## 🤝 Support & Community

- 🌐 **Website:** [atlas.consuly.ai](https://atlas.consuly.ai)
- 📖 **Documentation:** [atlas.consuly.ai/documentation](https://atlas.consuly.ai/documentation/)
- 🐛 **Report Issues:** [GitHub Issues](https://github.com/thefoundry-app/content-atlas/issues)
- 💬 **Discussions:** [GitHub Discussions](https://github.com/thefoundry-app/content-atlas/discussions)

---

## 📄 License

See [LICENSE](LICENSE) file for details.

---

**Built with ❤️ by [Consuly.ai](https://consuly.ai)**

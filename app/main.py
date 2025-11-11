"""
FastAPI application entry point.

This module initializes the FastAPI application, configures middleware,
and registers all API routers.
"""
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from .core.config import settings
from .core.logging_config import configure_logging

# Import routers
from .api.routers import (
    imports, mapping, tables, tasks, query, analysis,
    import_history, uploads, auth, api_keys, public_api, jobs
)

# Backwards-compatible exports used by tests and legacy modules.
from .domain.queries.analyzer import analyze_file_for_import  # noqa: F401
from .integrations.b2 import download_file_from_b2  # noqa: F401

# Ensure logging is configured before the application starts serving requests.
configure_logging(settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown events."""
    if os.getenv("SKIP_DB_INIT") == "1":
        print("SKIP_DB_INIT=1 detected; skipping database bootstrap during startup")
        yield
        return

    # Startup: Initialize database tables
    try:
        from .domain.uploads.uploaded_files import create_uploaded_files_table
        from .core.api_key_auth import init_api_key_tables
        from .db.metadata import create_table_metadata_table
        from .domain.imports.history import create_import_history_table
        from .domain.imports.jobs import ensure_import_jobs_table
        from .core.security import init_auth_tables
        
        print("Initializing database tables...")
        create_table_metadata_table()
        print("✓ table_metadata table ready")
        
        create_import_history_table()
        print("✓ import_history table ready")
        
        create_uploaded_files_table()
        # Success message printed inside function
        
        ensure_import_jobs_table()
        print("✓ import_jobs table ready")
        
        init_auth_tables()
        print("✓ auth tables ready")
        
        init_api_key_tables()
        print("✓ api_keys table ready")
        
        print("✓ All database tables initialized successfully")
    except Exception as e:
        print(f"ERROR: Failed to initialize database tables: {e}")
        print("The application cannot start without proper database setup.")
        import traceback
        traceback.print_exc()
        raise  # Re-raise to prevent app from starting with broken database
    
    yield  # Application runs here
    
    # Shutdown: Add cleanup logic here if needed in future
    pass


# Initialize FastAPI application
app = FastAPI(
    title="Data Mapper API",
    version="1.0.0",
    description="A data consolidation platform for SMBs to consolidate data from multiple sources into PostgreSQL",
    lifespan=lifespan
)

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative frontend port
    ],
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers including Authorization
)

# Register routers
app.include_router(imports.router)
app.include_router(mapping.router)
app.include_router(tables.router)
app.include_router(tasks.router)
app.include_router(query.router)
app.include_router(analysis.router)
app.include_router(import_history.router)
app.include_router(import_history.alias_router)
app.include_router(uploads.router)
app.include_router(auth.router)
app.include_router(api_keys.router)
app.include_router(public_api.router)
app.include_router(jobs.router)


@app.get("/")
async def root():
    """Root endpoint returning API information."""
    return {
        "message": "Data Mapper API",
        "version": "1.0.0"
    }

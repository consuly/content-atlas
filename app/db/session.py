import os
import socket
from contextlib import closing

from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import declarative_base, sessionmaker

from app.core.config import settings

_engine = None


def _report_connection_failure(exc: Exception) -> None:
    """Print high-signal diagnostics when the application cannot reach Postgres."""
    print(f"Warning: Could not connect to database: {exc}")
    print("The application will start but database operations will fail until the connection succeeds.")

    try:
        url = make_url(settings.database_url)
    except Exception as parse_error:  # pragma: no cover
        print(f"  Unable to parse DATABASE_URL ({parse_error}); skipping detailed diagnostics.")
        return

    masked_url = url._replace(password="***" if url.password else None)
    print("  Database connection settings:")
    print(f"    Dialect: {masked_url.get_backend_name()} (driver: {masked_url.get_driver_name() or 'default'})")
    print(f"    Host: {masked_url.host or 'localhost'}")
    print(f"    Port: {masked_url.port or '(default)'}")
    print(f"    Database: {masked_url.database}")
    print(f"    Username: {masked_url.username}")
    print(f"    SKIP_DB_INIT: {os.getenv('SKIP_DB_INIT')!r}")

    host = masked_url.host or "localhost"
    port = masked_url.port or 5432

    try:
        with closing(socket.create_connection((host, port), timeout=2)):
            print(f"    Socket check: ✅ Able to reach {host}:{port}")
    except OSError as socket_err:
        print(f"    Socket check: ❌ Unable to reach {host}:{port} ({socket_err})")

    print("  Troubleshooting tips:")
    print("    • Ensure Postgres is running (e.g., `docker-compose up -d`).")
    print("    • Verify DATABASE_URL and network access from this environment.")
    print("    • If running inside WSL/containers, confirm the hostname resolves correctly.")


def get_engine():
    global _engine
    if _engine is None:
        try:
            _engine = create_engine(settings.database_url)
            # Test connection eagerly so failures surface immediately.
            with _engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            _report_connection_failure(e)
            # Create the engine anyway so callers can proceed (may still fail later).
            _engine = create_engine(settings.database_url)
    return _engine


# Don't create engine at import time
SessionLocal = None

Base = declarative_base()


def get_session_local():
    global SessionLocal
    if SessionLocal is None:
        engine = get_engine()
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal


def get_db():
    SessionLocal = get_session_local()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

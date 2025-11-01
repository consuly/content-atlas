from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

from app.core.config import settings

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        try:
            _engine = create_engine(settings.database_url)
            # Test connection
            with _engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            print(f"Warning: Could not connect to database: {e}")
            print("The application will start but database operations will fail.")
            _engine = create_engine(settings.database_url)  # Still create it
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

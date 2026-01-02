import threading
from typing import Dict
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class TableLockManager:
    """
    Manages thread-safe locks for database tables to ensure sequential insertion.
    This prevents race conditions during duplicate checking and insertion when
    processing multiple files in parallel that target the same table.
    """
    _locks: Dict[str, threading.Lock] = {}
    _global_lock = threading.Lock()

    @classmethod
    def get_lock(cls, table_name: str) -> threading.Lock:
        """Get or create a lock for a specific table."""
        with cls._global_lock:
            if table_name not in cls._locks:
                cls._locks[table_name] = threading.Lock()
            return cls._locks[table_name]

    @classmethod
    @contextmanager
    def acquire(cls, table_name: str):
        """Context manager to acquire and release a table lock."""
        logger.info(f"Attempting to acquire lock for table '{table_name}'")
        lock = cls.get_lock(table_name)
        lock.acquire()
        logger.info(f"Acquired lock for table '{table_name}'")
        try:
            yield
        finally:
            lock.release()
            logger.info(f"Released lock for table '{table_name}'")

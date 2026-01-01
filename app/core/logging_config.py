"""
Application-wide logging configuration helpers.

This module centralizes logging setup so that all modules share the same
configuration and emits structured, human-readable log lines to stdout and files.
"""
from __future__ import annotations

import os
import sys
import time
import logging
import logging.handlers
from typing import Optional


_is_configured = False


def configure_logging(level: Optional[str] = None, log_timezone: str = "local") -> None:
    """
    Configure root and application loggers if they have not been configured yet.

    Args:
        level: Optional log level override (e.g., "DEBUG", "INFO").
        log_timezone: Timezone for log timestamps ("local" or "UTC").
    """
    global _is_configured

    if _is_configured:
        return

    log_level = (level or "INFO").upper()

    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # Configure timezone and date format
    date_fmt = "%Y-%m-%d %H:%M:%S%z"
    if log_timezone.upper() == "UTC":
        logging.Formatter.converter = time.gmtime
        date_fmt = "%Y-%m-%d %H:%M:%SZ"

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt=date_fmt
    )

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    # File Handler
    file_handler = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, "app.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)

    # Configure "app" Logger specifically
    app_logger = logging.getLogger("app")
    app_logger.setLevel(log_level)
    
    # Clear existing handlers
    if app_logger.hasHandlers():
        app_logger.handlers.clear()
        
    app_logger.addHandler(console_handler)
    app_logger.addHandler(file_handler)
    
    # Prevent propagation to root (which might be controlled by Uvicorn)
    app_logger.propagate = False

    # Also configure root for other libs, but don't duplicate
    root_logger = logging.getLogger()
    # root_logger.addHandler(console_handler) # Optional: decide if we want 3rd party logs in our format

    # Ensure uvicorn access logs are visible
    logging.getLogger("uvicorn.access").setLevel("INFO")

    _is_configured = True

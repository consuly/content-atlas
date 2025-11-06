"""
Application-wide logging configuration helpers.

This module centralizes logging setup so that all modules share the same
configuration and emits structured, human-readable log lines to stdout.
"""
from __future__ import annotations

import logging
from logging.config import dictConfig
from typing import Optional


_is_configured = False


def configure_logging(level: Optional[str] = None) -> None:
    """
    Configure root and application loggers if they have not been configured yet.

    Args:
        level: Optional log level override (e.g., "DEBUG", "INFO").
    """
    global _is_configured

    if _is_configured:
        return

    log_level = (level or "INFO").upper()

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                    "level": log_level,
                }
            },
            "root": {
                "handlers": ["console"],
                "level": log_level,
            },
        }
    )

    # Ensure our application namespace inherits the same level while still
    # propagating to the root logger for handler reuse.
    logging.getLogger("app").setLevel(log_level)

    _is_configured = True

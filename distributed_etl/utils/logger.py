from __future__ import annotations

import logging
import os
from typing import Any

import structlog


def configure_logging(*, level: str | int | None = None) -> None:
    """Configure stdlib logging + structlog.

    Call once at process startup (e.g., from `main.py` or Prefect entrypoints).
    """

    resolved_level: int
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")
    if isinstance(level, str):
        resolved_level = logging.getLevelName(level.upper())
        if not isinstance(resolved_level, int):
            resolved_level = logging.INFO
    else:
        resolved_level = int(level)

    logging.basicConfig(
        level=resolved_level,
        format="%(message)s",
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(resolved_level),
        cache_logger_on_first_use=True,
    )


def get_logger(**bound: Any) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger().bind(**bound)


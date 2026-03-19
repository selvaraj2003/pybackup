"""
Central logging configuration for pybackup.

Usage:
    # Once at startup (CLI / server entry point):
    configure_logging(level="DEBUG", log_file="/var/log/pybackup/pybackup.log")

    # In every module:
    import logging
    logger = logging.getLogger(__name__)
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pybackup.constants import LOG_FORMAT

_configured = False


def configure_logging(
    log_level: str = "INFO",
    log_file: str | None = None,
) -> None:
    """
    Configure the root logger for pybackup.

    Idempotent — safe to call multiple times; only configures once
    unless `force=True` equivalent behaviour is needed (clear handlers
    and reconfigure by calling this explicitly).

    :param log_level: One of DEBUG / INFO / WARNING / ERROR / CRITICAL
    :param log_file:  Optional path for persistent log file output
    """
    global _configured

    level = logging.getLevelName(log_level.upper())
    if not isinstance(level, int):
        level = logging.INFO

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()  # Always reset to avoid duplicate handlers

    formatter = logging.Formatter(LOG_FORMAT)

    # ── Console ──────────────────────────────────────────────────────
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    # ── File (optional) ──────────────────────────────────────────────
    if log_file:
        log_path = Path(log_file)
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(log_path)
            fh.setLevel(level)
            fh.setFormatter(formatter)
            root.addHandler(fh)
        except OSError as exc:
            root.warning("Could not open log file %s: %s", log_file, exc)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger.  Always prefer ``logging.getLogger(__name__)``
    inside modules; use this helper only when the name is dynamic.
    """
    return logging.getLogger(name)

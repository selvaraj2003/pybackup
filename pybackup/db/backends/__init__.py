"""
pybackup.db.backends — Pluggable database backend system.
Inspired by Django's DATABASES setting.

Config in pybackup.yaml:

    database:
      backend: sqlite
      name: /var/lib/pybackup/pybackup.db

    database:
      backend: postgresql
      host: localhost
      port: 5432
      name: pybackup
      user: pybackup_user
      password: ${DB_PASSWORD}
"""

from __future__ import annotations

import importlib
import logging
from typing import Any

from pybackup.utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

_BACKENDS: dict[str, str] = {
    "sqlite": "pybackup.db.database.Database",
    "postgresql": "pybackup.db.backends.postgres_backend.PostgreSQLDatabase",
    "mysql": "pybackup.db.backends.mysql_backend.MySQLDatabase",
    "mongodb": "pybackup.db.backends.mongo_backend.MongoDatabase",
    "mssql": "pybackup.db.backends.mssql_backend.MSSQLDatabase",
}

_DRIVER_HINTS: dict[str, str] = {
    "postgresql": "pip install psycopg2-binary",
    "mysql": "pip install PyMySQL",
    "mongodb": "pip install pymongo",
    "mssql": "pip install pyodbc",
}


def get_database(config: dict[str, Any]) -> Any:
    """
    Factory — return the correct Database instance from config dict.
    Falls back to SQLite if no 'database' section present.
    """
    db_cfg = config.get("database", {})
    backend = str(db_cfg.get("backend", "sqlite")).lower().strip()

    if backend not in _BACKENDS:
        raise DatabaseError(
            f"Unknown database backend: {backend!r}",
            details={"given": backend, "supported": list(_BACKENDS.keys())},
        )

    full_path = _BACKENDS[backend]
    module_path, class_name = full_path.rsplit(".", 1)

    try:
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
    except ImportError as exc:
        hint = _DRIVER_HINTS.get(backend, "")
        raise DatabaseError(
            f"Missing driver for '{backend}' backend. Install: {hint}",
            details=str(exc),
        ) from exc
    except AttributeError as exc:
        raise DatabaseError(
            f"Backend class not found: {full_path}",
            details=str(exc),
        ) from exc

    logger.info("Using database backend: %s", backend)

    if backend == "sqlite":
        db_path = db_cfg.get("name", "/var/lib/pybackup/pybackup.db")
        return cls(db_path)
    return cls(db_cfg)

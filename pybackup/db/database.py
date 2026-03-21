"""
pybackup/db/database.py
========================
Database facade — thin wrapper over any BaseBackend.

Default: SQLite (zero config, zero dependencies).
Override with PostgreSQL or MySQL by passing a backend instance.

Examples::

    db = Database()                           # SQLite :memory:
    db = Database("/var/lib/pybackup/pb.db") # SQLite file

    from pybackup.db.backends import PostgresBackend
    db = Database(backend=PostgresBackend(host="pg", user="u", password="p", dbname="pyb"))

    from pybackup.db.backends import MySQLBackend
    db = Database(backend=MySQLBackend(host="mysql", user="u", password="p", db="pyb"))
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import Any
from pybackup.db.base import BaseBackend

logger = logging.getLogger(__name__)


class Database:
    def __init__(
        self,
        db_path: str | Path | None = None,
        *,
        backend: BaseBackend | None = None,
    ) -> None:
        if backend is not None:
            self._backend = backend
        else:
            from pybackup.db.backends.sqlite_backend import SQLiteBackend

            self._backend = SQLiteBackend(str(db_path) if db_path else ":memory:")
        self._backend.init_schema()
        logger.debug("Database ready: %s", type(self._backend).__name__)

    # ── runs ──────────────────────────────────────────────────────
    def create_run(self, job_name: str, engine: str, details: dict | None = None) -> int:
        return self._backend.create_run(job_name, engine, details)

    def finish_run(
        self, run_id: int, *, status: str, output_path: str | None = None, error: str | None = None
    ) -> None:
        self._backend.finish_run(run_id, status=status, output_path=output_path, error=error)

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        return self._backend.get_run(run_id)

    def list_runs(
        self,
        limit: int = 100,
        offset: int = 0,
        job_name: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._backend.list_runs(limit, offset, job_name, status)

    def count_runs(self, job_name: str | None = None, status: str | None = None) -> int:
        return self._backend.count_runs(job_name, status)

    def delete_run(self, run_id: int) -> bool:
        return self._backend.delete_run(run_id)

    def stats(self) -> dict[str, Any]:
        return self._backend.stats()

    # ── files ─────────────────────────────────────────────────────
    def add_file(
        self, run_id: int, file_path: str, file_size: int | None = None, checksum: str | None = None
    ) -> int:
        return self._backend.add_file(run_id, file_path, file_size, checksum)

    def list_files(self, run_id: int) -> list[dict[str, Any]]:
        return self._backend.list_files(run_id)

    # ── settings ──────────────────────────────────────────────────
    def get_setting(self, key: str, default: str | None = None) -> str | None:
        return self._backend.get_setting(key, default)

    def set_setting(self, key: str, value: str) -> None:
        self._backend.set_setting(key, value)

    # ── users/auth ────────────────────────────────────────────────
    def get_user(self, username: str) -> dict[str, Any] | None:
        return self._backend.get_user(username)

    def create_user(self, username: str, password_hash: str, role: str = "admin") -> int:
        return self._backend.create_user(username, password_hash, role)

    def update_password(self, username: str, password_hash: str) -> None:
        self._backend.update_password(username, password_hash)

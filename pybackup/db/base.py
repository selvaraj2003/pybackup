"""
pybackup/db/base.py
===================
Abstract base class for all database backends.

Backends:
    - SQLite  (default, zero config)
    - PostgreSQL
    - MySQL
    - MSSQL

Every backend MUST implement all abstract methods.
The Database class in database.py wraps the active backend.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseBackend(ABC):
    """Abstract interface every DB backend must satisfy."""

    # ── Schema ────────────────────────────────────────────────────

    @abstractmethod
    def init_schema(self) -> None:
        """Create all tables if they don't exist."""

    # ── backup_runs ───────────────────────────────────────────────

    @abstractmethod
    def create_run(self, job_name: str, engine: str, details: dict | None = None) -> int: ...

    @abstractmethod
    def finish_run(
        self, run_id: int, *, status: str, output_path: str | None = None, error: str | None = None
    ) -> None: ...

    @abstractmethod
    def get_run(self, run_id: int) -> dict[str, Any] | None: ...

    @abstractmethod
    def list_runs(
        self,
        limit: int = 100,
        offset: int = 0,
        job_name: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    def count_runs(self, job_name: str | None = None, status: str | None = None) -> int: ...

    @abstractmethod
    def delete_run(self, run_id: int) -> bool: ...

    @abstractmethod
    def stats(self) -> dict[str, Any]: ...

    # ── backup_files ──────────────────────────────────────────────

    @abstractmethod
    def add_file(
        self, run_id: int, file_path: str, file_size: int | None = None, checksum: str | None = None
    ) -> int: ...

    @abstractmethod
    def list_files(self, run_id: int) -> list[dict[str, Any]]: ...

    # ── settings ──────────────────────────────────────────────────

    @abstractmethod
    def get_setting(self, key: str, default: str | None = None) -> str | None: ...

    @abstractmethod
    def set_setting(self, key: str, value: str) -> None: ...

    # ── users (auth) ──────────────────────────────────────────────

    @abstractmethod
    def get_user(self, username: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def create_user(self, username: str, password_hash: str, role: str = "admin") -> int: ...

    @abstractmethod
    def update_password(self, username: str, password_hash: str) -> None: ...

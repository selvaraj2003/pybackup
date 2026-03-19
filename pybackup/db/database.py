"""
SQLite-backed persistence layer for pybackup.

Tables:
    backup_runs   — one row per job execution
    backup_files  — files produced by a run
    settings      — key/value store for UI preferences

All access is synchronous (no async required for SQLite).
The connection is opened per-request (thread-safe WAL mode).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from pybackup.utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

# ─── Schema ─────────────────────────────────────────────────────────

_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS backup_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name    TEXT    NOT NULL,
    engine      TEXT    NOT NULL,
    status      TEXT    NOT NULL CHECK(status IN ('running','success','failed','crashed')),
    started_at  TEXT    NOT NULL,
    finished_at TEXT,
    output_path TEXT,
    error       TEXT,
    details     TEXT    -- JSON blob for extra metadata
);

CREATE TABLE IF NOT EXISTS backup_files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     INTEGER NOT NULL REFERENCES backup_runs(id) ON DELETE CASCADE,
    file_path  TEXT    NOT NULL,
    file_size  INTEGER,
    checksum   TEXT,
    created_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_job       ON backup_runs(job_name);
CREATE INDEX IF NOT EXISTS idx_runs_status    ON backup_runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_started   ON backup_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_files_run      ON backup_files(run_id);
"""


# ─── Database class ─────────────────────────────────────────────────

class Database:
    """
    Thin wrapper around SQLite providing typed accessors for pybackup data.

    Usage::

        db = Database("/var/lib/pybackup/pybackup.db")
        run_id = db.create_run("myjob", "postgres")
        db.finish_run(run_id, status="success", output_path="/backups/...")
    """

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.db_path = str(db_path)
        if self.db_path == ":memory:":
            import sqlite3 as _s
            self._shared_conn = _s.connect(":memory:", check_same_thread=False)
        else:
            self._ensure_parent()
        self._init_schema()
        logger.debug("Database initialised: %s", self.db_path)

    # ─── Context manager ───────────────────────────────────────────

    @contextmanager
    def _connect(self) -> Generator[sqlite3.Connection, None, None]:
        # :memory: databases must reuse the same connection — each new
        # sqlite3.connect(":memory:") produces an empty, isolated database.
        if self.db_path == ":memory:":
            conn = self._shared_conn  # type: ignore[attr-defined]
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            except sqlite3.Error as exc:
                conn.rollback()
                raise DatabaseError("SQLite error",
                    details={"error": str(exc), "db": self.db_path}) from exc
            return

        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except sqlite3.Error as exc:
            conn.rollback()
            raise DatabaseError(
                "SQLite error",
                details={"error": str(exc), "db": self.db_path},
            ) from exc
        finally:
            conn.close()

    # ─── Schema init ───────────────────────────────────────────────

    def _init_schema(self) -> None:
        try:
            with self._connect() as conn:
                conn.executescript(_SCHEMA)
        except DatabaseError as exc:
            raise DatabaseError(
                "Failed to initialise database schema",
                details=exc.details,
            ) from exc

    def _ensure_parent(self) -> None:
        if self.db_path == ":memory:":
            return
        try:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise DatabaseError(
                f"Cannot create database directory: {Path(self.db_path).parent}",
                details={"error": str(exc)},
            ) from exc

    # ─── backup_runs ───────────────────────────────────────────────

    def create_run(
        self,
        job_name: str,
        engine: str,
        details: dict[str, Any] | None = None,
    ) -> int:
        """Insert a new 'running' backup run and return its id."""
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO backup_runs (job_name, engine, status, started_at, details)
                VALUES (?, ?, 'running', ?, ?)
                """,
                (job_name, engine, now, json.dumps(details or {})),
            )
            return cur.lastrowid  # type: ignore[return-value]

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        output_path: str | None = None,
        error: str | None = None,
    ) -> None:
        """Update a backup run's final status."""
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE backup_runs
                SET status=?, finished_at=?, output_path=?, error=?
                WHERE id=?
                """,
                (status, now, output_path, error, run_id),
            )

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM backup_runs WHERE id=?", (run_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_runs(
        self,
        limit: int = 100,
        offset: int = 0,
        job_name: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM backup_runs WHERE 1=1"
        params: list[Any] = []

        if job_name:
            query += " AND job_name=?"
            params.append(job_name)
        if status:
            query += " AND status=?"
            params.append(status)

        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params += [limit, offset]

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def count_runs(
        self,
        job_name: str | None = None,
        status: str | None = None,
    ) -> int:
        query = "SELECT COUNT(*) FROM backup_runs WHERE 1=1"
        params: list[Any] = []
        if job_name:
            query += " AND job_name=?"
            params.append(job_name)
        if status:
            query += " AND status=?"
            params.append(status)
        with self._connect() as conn:
            return conn.execute(query, params).fetchone()[0]

    def delete_run(self, run_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM backup_runs WHERE id=?", (run_id,))
        return cur.rowcount > 0

    # ─── Statistics ────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Return aggregate statistics for the dashboard."""
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM backup_runs").fetchone()[0]
            success = conn.execute(
                "SELECT COUNT(*) FROM backup_runs WHERE status='success'"
            ).fetchone()[0]
            failed = conn.execute(
                "SELECT COUNT(*) FROM backup_runs WHERE status IN ('failed','crashed')"
            ).fetchone()[0]
            running = conn.execute(
                "SELECT COUNT(*) FROM backup_runs WHERE status='running'"
            ).fetchone()[0]

            recent = conn.execute(
                """
                SELECT job_name, engine, status, started_at, finished_at, error
                FROM backup_runs
                ORDER BY started_at DESC
                LIMIT 10
                """
            ).fetchall()

            by_engine = conn.execute(
                """
                SELECT engine, COUNT(*) as count,
                       SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as successes
                FROM backup_runs
                GROUP BY engine
                """
            ).fetchall()

            # Last 30 days daily counts
            daily = conn.execute(
                """
                SELECT DATE(started_at) as day,
                       COUNT(*) as total,
                       SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as ok
                FROM backup_runs
                WHERE started_at >= DATE('now','-30 days')
                GROUP BY day
                ORDER BY day
                """
            ).fetchall()

        return {
            "total": total,
            "success": success,
            "failed": failed,
            "running": running,
            "success_rate": round((success / total * 100) if total else 0, 1),
            "recent": [dict(r) for r in recent],
            "by_engine": [dict(r) for r in by_engine],
            "daily": [dict(r) for r in daily],
        }

    # ─── backup_files ──────────────────────────────────────────────

    def add_file(
        self,
        run_id: int,
        file_path: str,
        file_size: int | None = None,
        checksum: str | None = None,
    ) -> int:
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO backup_files (run_id, file_path, file_size, checksum, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, file_path, file_size, checksum, now),
            )
            return cur.lastrowid  # type: ignore[return-value]

    def list_files(self, run_id: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM backup_files WHERE run_id=? ORDER BY id", (run_id,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ─── Settings ──────────────────────────────────────────────────

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM settings WHERE key=?", (key,)
            ).fetchone()
        return row[0] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

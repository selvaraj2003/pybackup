"""
pybackup/db/backends/sqlite_backend.py
=======================================
SQLite backend — default, zero dependencies beyond stdlib.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from pybackup.db.base import BaseBackend
from pybackup.utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

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
    details     TEXT
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

CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    NOT NULL UNIQUE COLLATE NOCASE,
    password_hash TEXT    NOT NULL,
    role          TEXT    NOT NULL DEFAULT 'viewer'
                          CHECK(role IN ('admin','viewer')),
    email         TEXT,
    created_at    TEXT    NOT NULL,
    last_login    TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_job     ON backup_runs(job_name);
CREATE INDEX IF NOT EXISTS idx_runs_status  ON backup_runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_started ON backup_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_files_run    ON backup_files(run_id);
"""


class SQLiteBackend(BaseBackend):
    """SQLite-backed storage. Works with both file DBs and :memory:."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        if db_path == ":memory:":
            self._shared = sqlite3.connect(":memory:", check_same_thread=False)
        else:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            self._shared = None
        logger.debug("SQLiteBackend: %s", db_path)

    # ── connection ────────────────────────────────────────────────

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        if self._shared is not None:
            conn = self._shared
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            except sqlite3.Error as exc:
                conn.rollback()
                raise DatabaseError("SQLite error", details=str(exc)) from exc
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
            raise DatabaseError("SQLite error", details=str(exc)) from exc
        finally:
            conn.close()

    # ── schema ────────────────────────────────────────────────────

    def init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(_SCHEMA)

    # ── backup_runs ───────────────────────────────────────────────

    def create_run(self, job_name: str, engine: str, details: dict | None = None) -> int:
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO backup_runs (job_name,engine,status,started_at,details)"
                " VALUES (?,?,'running',?,?)",
                (job_name, engine, now, json.dumps(details or {})),
            )
            return cur.lastrowid

    def finish_run(
        self, run_id: int, *, status: str, output_path: str | None = None, error: str | None = None
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                "UPDATE backup_runs SET status=?,finished_at=?,output_path=?,error=? WHERE id=?",
                (status, now, output_path, error, run_id),
            )

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM backup_runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    def list_runs(
        self,
        limit: int = 100,
        offset: int = 0,
        job_name: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        q = "SELECT * FROM backup_runs WHERE 1=1"
        p: list[Any] = []
        if job_name:
            q += " AND job_name=?"
            p.append(job_name)
        if status:
            q += " AND status=?"
            p.append(status)
        q += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        p += [limit, offset]
        with self._conn() as conn:
            return [dict(r) for r in conn.execute(q, p).fetchall()]

    def count_runs(
        self,
        job_name: str | None = None,
        status: str | None = None,
    ) -> int:
        q = "SELECT COUNT(*) FROM backup_runs WHERE 1=1"
        p: list[Any] = []
        if job_name:
            q += " AND job_name=?"
            p.append(job_name)
        if status:
            q += " AND status=?"
            p.append(status)
        with self._conn() as conn:
            return conn.execute(q, p).fetchone()[0]

    def delete_run(self, run_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM backup_runs WHERE id=?", (run_id,))
        return cur.rowcount > 0

    def stats(self) -> dict[str, Any]:
        with self._conn() as conn:
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
                "SELECT job_name,engine,status,started_at,finished_at,error FROM backup_runs ORDER BY started_at DESC LIMIT 10"
            ).fetchall()
            by_eng = conn.execute(
                "SELECT engine,COUNT(*) as count,SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as successes FROM backup_runs GROUP BY engine"
            ).fetchall()
            daily = conn.execute(
                "SELECT DATE(started_at) as day,COUNT(*) as total,SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as ok FROM backup_runs WHERE started_at >= DATE('now','-30 days') GROUP BY day ORDER BY day"
            ).fetchall()
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "running": running,
            "success_rate": round((success / total * 100) if total else 0, 1),
            "recent": [dict(r) for r in recent],
            "by_engine": [dict(r) for r in by_eng],
            "daily": [dict(r) for r in daily],
        }

    # ── backup_files ──────────────────────────────────────────────

    def add_file(
        self, run_id: int, file_path: str, file_size: int | None = None, checksum: str | None = None
    ) -> int:
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO backup_files (run_id,file_path,file_size,checksum,created_at) VALUES (?,?,?,?,?)",
                (run_id, file_path, file_size, checksum, now),
            )
            return cur.lastrowid

    def list_files(self, run_id: int) -> list[dict]:
        with self._conn() as conn:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM backup_files WHERE run_id=? ORDER BY id", (run_id,)
                ).fetchall()
            ]

    # ── settings ──────────────────────────────────────────────────

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # ── users ─────────────────────────────────────────────────────

    def get_user(self, username: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        return dict(row) if row else None

    def create_user(self, username: str, password_hash: str, role: str = "admin") -> int:
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO users (username,password_hash,role,created_at) VALUES (?,?,?,?)",
                (username, password_hash, role, now),
            )
            return cur.lastrowid

    def update_password(self, username: str, password_hash: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE users SET password_hash=? WHERE username=?",
                (password_hash, username),
            )

    def update_last_login(self, username: str) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute("UPDATE users SET last_login=? WHERE username=?", (now, username))

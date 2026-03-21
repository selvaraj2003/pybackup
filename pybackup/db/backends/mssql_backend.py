"""MSSQL backend for pybackup. Requires: pip install pyodbc"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from pybackup.utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

try:
    import pyodbc

    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False


class MSSQLDatabase:
    def __init__(self, cfg: dict[str, Any]) -> None:
        if not _AVAILABLE:
            raise DatabaseError("pyodbc not installed. Run: pip install pyodbc")
        self._conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={cfg.get('host', 'localhost')},{cfg.get('port', 1433)};"
            f"DATABASE={cfg.get('name', 'pybackup')};"
            f"UID={cfg.get('user', 'sa')};"
            f"PWD={cfg.get('password', '')};"
            "TrustServerCertificate=yes;"
        )
        self._init_schema()

    def _connect(self):
        try:
            return pyodbc.connect(self._conn_str)
        except Exception as exc:
            raise DatabaseError("MSSQL connect failed", details=str(exc)) from exc

    def _init_schema(self) -> None:
        tables = [
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='backup_runs')"
            " CREATE TABLE backup_runs (id INT IDENTITY PRIMARY KEY,"
            " job_name NVARCHAR(255) NOT NULL, engine NVARCHAR(100) NOT NULL,"
            " status NVARCHAR(20) NOT NULL, started_at NVARCHAR(50) NOT NULL,"
            " finished_at NVARCHAR(50), output_path NVARCHAR(MAX),"
            " error NVARCHAR(MAX), details NVARCHAR(MAX))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='backup_files')"
            " CREATE TABLE backup_files (id INT IDENTITY PRIMARY KEY,"
            " run_id INT NOT NULL, file_path NVARCHAR(MAX) NOT NULL,"
            " file_size BIGINT, checksum NVARCHAR(128),"
            " created_at NVARCHAR(50) NOT NULL)",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='settings')"
            " CREATE TABLE settings ([key] NVARCHAR(255) PRIMARY KEY,"
            " value NVARCHAR(MAX) NOT NULL)",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users')"
            " CREATE TABLE users (id INT IDENTITY PRIMARY KEY,"
            " username NVARCHAR(255) NOT NULL UNIQUE,"
            " password_hash NVARCHAR(MAX) NOT NULL,"
            " role NVARCHAR(20) NOT NULL DEFAULT 'viewer',"
            " email NVARCHAR(255), created_at NVARCHAR(50) NOT NULL,"
            " last_login NVARCHAR(50))",
        ]
        conn = self._connect()
        try:
            cur = conn.cursor()
            for t in tables:
                cur.execute(t)
            conn.commit()
        finally:
            conn.close()

    def _q(self, sql: str, params: tuple = ()) -> list[dict]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()
            try:
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            except Exception:
                return []
        except Exception as exc:
            conn.rollback()
            raise DatabaseError("MSSQL query failed", details=str(exc)) from exc
        finally:
            conn.close()

    def _q1(self, sql: str, params: tuple = ()) -> dict | None:
        rows = self._q(sql, params)
        return rows[0] if rows else None

    def create_run(
        self,
        job_name: str,
        engine: str,
        details: dict | None = None,
    ) -> int:
        now = datetime.now(tz=timezone.utc).isoformat()
        self._q(
            "INSERT INTO backup_runs(job_name,engine,status,started_at,details)"
            " VALUES(?,?,'running',?,?)",
            (job_name, engine, now, json.dumps(details or {})),
        )
        r = self._q1("SELECT SCOPE_IDENTITY() as id")
        return int(r["id"]) if r else None  # type: ignore[return-value]

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        output_path: str | None = None,
        error: str | None = None,
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self._q(
            "UPDATE backup_runs" " SET status=?,finished_at=?,output_path=?,error=? WHERE id=?",
            (status, now, output_path, error, run_id),
        )

    def get_run(self, run_id: int) -> dict | None:
        return self._q1("SELECT * FROM backup_runs WHERE id=?", (run_id,))

    def list_runs(
        self,
        limit: int = 100,
        offset: int = 0,
        job_name: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        sql, p = "SELECT * FROM backup_runs WHERE 1=1", []
        if job_name:
            sql += " AND job_name=?"
            p.append(job_name)
        if status:
            sql += " AND status=?"
            p.append(status)
        sql += f" ORDER BY started_at DESC" f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        return self._q(sql, tuple(p))

    def count_runs(
        self,
        job_name: str | None = None,
        status: str | None = None,
    ) -> int:
        sql, p = "SELECT COUNT(*) as c FROM backup_runs WHERE 1=1", []
        if job_name:
            sql += " AND job_name=?"
            p.append(job_name)
        if status:
            sql += " AND status=?"
            p.append(status)
        r = self._q1(sql, tuple(p))
        return r["c"] if r else 0  # type: ignore[index]

    def delete_run(self, run_id: int) -> bool:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM backup_runs WHERE id=?", (run_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def add_file(
        self,
        run_id: int,
        file_path: str,
        file_size: int | None = None,
        checksum: str | None = None,
    ) -> int:
        now = datetime.now(tz=timezone.utc).isoformat()
        self._q(
            "INSERT INTO backup_files"
            "(run_id,file_path,file_size,checksum,created_at)"
            " VALUES(?,?,?,?,?)",
            (run_id, file_path, file_size, checksum, now),
        )
        r = self._q1("SELECT SCOPE_IDENTITY() as id")
        return int(r["id"]) if r else None  # type: ignore[return-value]

    def list_files(self, run_id: int) -> list[dict]:
        return self._q(
            "SELECT * FROM backup_files WHERE run_id=? ORDER BY id",
            (run_id,),
        )

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        r = self._q1("SELECT value FROM settings WHERE [key]=?", (key,))
        return r["value"] if r else default  # type: ignore[index]

    def set_setting(self, key: str, value: str) -> None:
        self._q(
            "MERGE settings AS t USING (SELECT ? as k, ? as v) AS s"
            " ON t.[key]=s.k"
            " WHEN MATCHED THEN UPDATE SET value=s.v"
            " WHEN NOT MATCHED THEN INSERT([key],value) VALUES(s.k,s.v);",
            (key, value),
        )

    def stats(self) -> dict:
        total = (self._q1("SELECT COUNT(*) as c FROM backup_runs") or {}).get("c", 0)
        success = (
            self._q1("SELECT COUNT(*) as c FROM backup_runs WHERE status='success'") or {}
        ).get("c", 0)
        failed = (
            self._q1(
                "SELECT COUNT(*) as c FROM backup_runs" " WHERE status IN ('failed','crashed')"
            )
            or {}
        ).get("c", 0)
        running = (
            self._q1("SELECT COUNT(*) as c FROM backup_runs WHERE status='running'") or {}
        ).get("c", 0)
        recent = self.list_runs(limit=10)
        by_eng = self._q(
            "SELECT engine,COUNT(*) as count,"
            "SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as successes"
            " FROM backup_runs GROUP BY engine"
        )
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "running": running,
            "success_rate": round((success / total * 100) if total else 0, 1),
            "recent": recent,
            "by_engine": by_eng,
            "daily": [],
        }

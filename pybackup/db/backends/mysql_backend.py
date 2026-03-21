"""MySQL / MariaDB backend for pybackup. Requires: pip install PyMySQL"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from pybackup.utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

try:
    import pymysql
    import pymysql.cursors

    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False

_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS backup_runs (
        id INT AUTO_INCREMENT PRIMARY KEY, job_name VARCHAR(255) NOT NULL,
        engine VARCHAR(100) NOT NULL,
        status ENUM('running','success','failed','crashed') NOT NULL,
        started_at VARCHAR(50) NOT NULL, finished_at VARCHAR(50),
        output_path TEXT, error TEXT, details TEXT)""",
    """CREATE TABLE IF NOT EXISTS backup_files (
        id INT AUTO_INCREMENT PRIMARY KEY, run_id INT NOT NULL,
        file_path TEXT NOT NULL, file_size BIGINT, checksum VARCHAR(128),
        created_at VARCHAR(50) NOT NULL,
        FOREIGN KEY(run_id) REFERENCES backup_runs(id) ON DELETE CASCADE)""",
    "CREATE TABLE IF NOT EXISTS settings" " (key VARCHAR(255) PRIMARY KEY, value TEXT NOT NULL)",
    """CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(255) NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role ENUM('admin','viewer') NOT NULL DEFAULT 'viewer',
        email VARCHAR(255), created_at VARCHAR(50) NOT NULL,
        last_login VARCHAR(50))""",
]


class MySQLDatabase:
    def __init__(self, cfg: dict[str, Any]) -> None:
        if not _AVAILABLE:
            raise DatabaseError("PyMySQL not installed. Run: pip install PyMySQL")
        self._cfg = dict(
            host=cfg.get("host", "localhost"),
            port=int(cfg.get("port", 3306)),
            db=cfg.get("name", "pybackup"),
            user=cfg.get("user", "pybackup"),
            password=cfg.get("password", ""),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
        self._init_schema()

    def _connect(self):
        try:
            return pymysql.connect(**self._cfg)
        except Exception as exc:
            raise DatabaseError("MySQL connect failed", details=str(exc)) from exc

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                for sql in _SCHEMA:
                    cur.execute(sql)
            conn.commit()
        finally:
            conn.close()

    def _q(self, sql: str, params: tuple = ()) -> list[dict]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                try:
                    return list(cur.fetchall())
                except Exception:
                    return []
        except Exception as exc:
            conn.rollback()
            raise DatabaseError("MySQL query failed", details=str(exc)) from exc
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
            " VALUES(%s,%s,'running',%s,%s)",
            (job_name, engine, now, json.dumps(details or {})),
        )
        r = self._q1("SELECT LAST_INSERT_ID() as id")
        return r["id"] if r else None  # type: ignore[return-value]

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
            "UPDATE backup_runs"
            " SET status=%s,finished_at=%s,output_path=%s,error=%s WHERE id=%s",
            (status, now, output_path, error, run_id),
        )

    def get_run(self, run_id: int) -> dict | None:
        return self._q1("SELECT * FROM backup_runs WHERE id=%s", (run_id,))

    def list_runs(
        self,
        limit: int = 100,
        offset: int = 0,
        job_name: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        sql, p = "SELECT * FROM backup_runs WHERE 1=1", []
        if job_name:
            sql += " AND job_name=%s"
            p.append(job_name)
        if status:
            sql += " AND status=%s"
            p.append(status)
        sql += " ORDER BY started_at DESC LIMIT %s OFFSET %s"
        p += [limit, offset]
        return self._q(sql, tuple(p))

    def count_runs(
        self,
        job_name: str | None = None,
        status: str | None = None,
    ) -> int:
        sql, p = "SELECT COUNT(*) as c FROM backup_runs WHERE 1=1", []
        if job_name:
            sql += " AND job_name=%s"
            p.append(job_name)
        if status:
            sql += " AND status=%s"
            p.append(status)
        r = self._q1(sql, tuple(p))
        return r["c"] if r else 0  # type: ignore[index]

    def delete_run(self, run_id: int) -> bool:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM backup_runs WHERE id=%s", (run_id,))
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
            " VALUES(%s,%s,%s,%s,%s)",
            (run_id, file_path, file_size, checksum, now),
        )
        r = self._q1("SELECT LAST_INSERT_ID() as id")
        return r["id"] if r else None  # type: ignore[return-value]

    def list_files(self, run_id: int) -> list[dict]:
        return self._q(
            "SELECT * FROM backup_files WHERE run_id=%s ORDER BY id",
            (run_id,),
        )

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        r = self._q1("SELECT value FROM settings WHERE key=%s", (key,))
        return r["value"] if r else default  # type: ignore[index]

    def set_setting(self, key: str, value: str) -> None:
        self._q(
            "INSERT INTO settings(key,value) VALUES(%s,%s)"
            " ON DUPLICATE KEY UPDATE value=VALUES(value)",
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
        recent = self._q(
            "SELECT job_name,engine,status,started_at,finished_at,error"
            " FROM backup_runs ORDER BY started_at DESC LIMIT 10"
        )
        by_eng = self._q(
            "SELECT engine,COUNT(*) as count,"
            "SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as successes"
            " FROM backup_runs GROUP BY engine"
        )
        daily = self._q(
            "SELECT DATE(started_at) as day,COUNT(*) as total,"
            "SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as ok"
            " FROM backup_runs"
            " WHERE started_at >= DATE_SUB(NOW(),INTERVAL 30 DAY)"
            " GROUP BY day ORDER BY day"
        )
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "running": running,
            "success_rate": round((success / total * 100) if total else 0, 1),
            "recent": recent,
            "by_engine": by_eng,
            "daily": daily,
        }

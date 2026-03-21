"""
PostgreSQL Backup Engine using ``pg_dump``.

Supported dump formats:
- ``custom``    → pg_restore-compatible binary (default, recommended)
- ``directory`` → parallel-capable directory format
- ``plain``     → human-readable SQL
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Any

from pybackup.engine.base import BaseBackupEngine
from pybackup.utils.exceptions import BackupError
from pybackup.utils.security import get_secret

logger = logging.getLogger(__name__)

_FORMAT_EXT = {"custom": "dump", "directory": "dir", "plain": "sql"}


class PostgresBackupEngine(BaseBackupEngine):
    """Backup engine wrapping the ``pg_dump`` CLI tool."""

    def __init__(
        self,
        job_name: str,
        job_config: dict[str, Any],
        global_config: dict[str, Any],
    ) -> None:
        super().__init__(job_name, job_config, global_config)

        self.host: str = job_config.get("host", "localhost")
        self.port: int = int(job_config.get("port", 5432))
        self.database: str = job_config.get("database", "")
        self.username: str = job_config.get("username", "")
        self.password: str | None = get_secret(
            job_config.get("password"), name="postgresql.password"
        )
        self.dump_format: str = job_config.get("format", "custom")

        if not self.database:
            raise BackupError(
                "postgresql.database is required",
                details={"job": self.job_name},
            )
        if not self.username:
            raise BackupError(
                "postgresql.username is required",
                details={"job": self.job_name},
            )
        if self.dump_format not in _FORMAT_EXT:
            raise BackupError(
                f"Unsupported pg_dump format: {self.dump_format!r}",
                details={"job": self.job_name, "allowed": list(_FORMAT_EXT)},
            )

    def run(self) -> Path:
        output_dir = self.get_output_dir()
        ext = _FORMAT_EXT[self.dump_format]
        backup_file = output_dir / f"{self.database}_{self.timestamp}.{ext}"

        cmd = [
            "pg_dump",
            "-h",
            self.host,
            "-p",
            str(self.port),
            "-U",
            self.username,
            "-d",
            self.database,
        ]

        format_flag = {"custom": "c", "directory": "d", "plain": "p"}[self.dump_format]
        cmd += ["-F", format_flag]

        logger.info(
            "[%s] Starting pg_dump → %s (format=%s)",
            self.job_name,
            backup_file,
            self.dump_format,
        )

        env = self._build_env()

        try:
            if self.dump_format == "directory":
                subprocess.run(
                    cmd + ["-f", str(backup_file)],
                    env=env,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    timeout=3600,
                )
            else:
                with backup_file.open("wb") as fh:
                    subprocess.run(
                        cmd,
                        env=env,
                        stdout=fh,
                        stderr=subprocess.PIPE,
                        check=True,
                        timeout=3600,
                    )

            if self.compress and self.dump_format == "plain":
                backup_file = self._compress(backup_file)

        except subprocess.TimeoutExpired as exc:
            raise BackupError("pg_dump timed out", details={"job": self.job_name}) from exc
        except subprocess.CalledProcessError as exc:
            raise BackupError(
                "pg_dump failed",
                details={"job": self.job_name, "returncode": exc.returncode, "stderr": exc.stderr},
            ) from exc
        except FileNotFoundError as exc:
            raise BackupError(
                "pg_dump not found — is PostgreSQL client installed?",
                details={"job": self.job_name},
            ) from exc

        logger.info("[%s] PostgreSQL backup completed: %s", self.job_name, backup_file)
        return backup_file

    # ─── Helpers ───────────────────────────────────────────────────

    def _build_env(self) -> dict[str, str]:
        """Build subprocess environment, injecting PGPASSWORD if needed."""
        env = os.environ.copy()
        if self.password:
            env["PGPASSWORD"] = self.password
        return env

    def _compress(self, file_path: Path) -> Path:
        logger.info("[%s] Compressing %s", self.job_name, file_path)
        try:
            subprocess.run(
                ["gzip", "-f", str(file_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            raise BackupError(
                "gzip compression failed",
                details={"job": self.job_name, "stderr": exc.stderr},
            ) from exc
        return file_path.with_suffix(file_path.suffix + ".gz")

"""
Microsoft SQL Server Backup Engine using ``sqlcmd``.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any

from pybackup.engine.base import BaseBackupEngine
from pybackup.utils.exceptions import BackupError
from pybackup.utils.security import get_secret

logger = logging.getLogger(__name__)


class MSSQLBackupEngine(BaseBackupEngine):
    """Backup engine wrapping the ``sqlcmd`` CLI tool (BACKUP DATABASE)."""

    def __init__(
        self,
        job_name: str,
        job_config: dict[str, Any],
        global_config: dict[str, Any],
    ) -> None:
        super().__init__(job_name, job_config, global_config)

        self.host: str = job_config.get("host", "localhost")
        self.port: int = int(job_config.get("port", 1433))
        self.database: str = job_config.get("database", "")
        self.username: str = job_config.get("username", "")
        self.password: str | None = get_secret(
            job_config.get("password"), name="mssql.password"
        )
        self.encrypt: bool = job_config.get("encrypt", False)

        if not self.database:
            raise BackupError("mssql.database is required", details={"job": self.job_name})
        if not self.username:
            raise BackupError("mssql.username is required", details={"job": self.job_name})

    def run(self) -> Path:
        output_dir = self.get_output_dir()
        bak_file = output_dir / f"{self.database}_{self.timestamp}.bak"

        sql = (
            f"BACKUP DATABASE [{self.database}] "
            f"TO DISK = N'{bak_file}' "
            f"WITH INIT, COMPRESSION;"
        )

        cmd = [
            "sqlcmd",
            "-S", f"{self.host},{self.port}",
            "-U", self.username,
            "-P", self.password or "",
            "-Q", sql,
            "-b",   # Exit with error on SQL error
        ]

        if self.encrypt:
            cmd.append("-N")

        logger.info("[%s] Starting MSSQL BACKUP DATABASE → %s", self.job_name, bak_file)

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
                timeout=7200,
            )
            if result.stdout:
                logger.debug("[%s] sqlcmd output: %s", self.job_name, result.stdout)
        except subprocess.TimeoutExpired as exc:
            raise BackupError("sqlcmd timed out", details={"job": self.job_name}) from exc
        except subprocess.CalledProcessError as exc:
            raise BackupError(
                "sqlcmd BACKUP DATABASE failed",
                details={
                    "job": self.job_name,
                    "returncode": exc.returncode,
                    "stdout": exc.stdout,
                    "stderr": exc.stderr,
                },
            ) from exc
        except FileNotFoundError as exc:
            raise BackupError(
                "sqlcmd not found — is mssql-tools installed?",
                details={"job": self.job_name},
            ) from exc

        logger.info("[%s] MSSQL backup completed: %s", self.job_name, bak_file)
        return bak_file

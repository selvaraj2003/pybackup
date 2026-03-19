"""
MySQL Backup Engine using ``mysqldump``.
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


class MySQLBackupEngine(BaseBackupEngine):
    """Backup engine wrapping the ``mysqldump`` CLI tool."""

    def __init__(
        self,
        job_name: str,
        job_config: dict[str, Any],
        global_config: dict[str, Any],
    ) -> None:
        super().__init__(job_name, job_config, global_config)

        self.host: str = job_config.get("host", "localhost")
        self.port: int = int(job_config.get("port", 3306))
        self.database: str = job_config.get("database", "")
        self.username: str = job_config.get("username", "")
        self.password: str | None = get_secret(
            job_config.get("password"), name="mysql.password"
        )
        self.single_transaction: bool = job_config.get("single_transaction", True)

        if not self.database:
            raise BackupError("mysql.database is required", details={"job": self.job_name})
        if not self.username:
            raise BackupError("mysql.username is required", details={"job": self.job_name})

    def run(self) -> Path:
        output_dir = self.get_output_dir()
        sql_file = output_dir / f"{self.database}_{self.timestamp}.sql"

        cmd = [
            "mysqldump",
            "-h", self.host,
            "-P", str(self.port),
            "-u", self.username,
        ]

        if self.password:
            cmd.append(f"-p{self.password}")

        if self.single_transaction:
            cmd.append("--single-transaction")

        cmd.append(self.database)

        logger.info("[%s] Starting mysqldump → %s", self.job_name, sql_file)

        try:
            with sql_file.open("w", encoding="utf-8") as fh:
                subprocess.run(
                    cmd,
                    stdout=fh,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                    timeout=3600,
                )
        except subprocess.TimeoutExpired as exc:
            raise BackupError("mysqldump timed out", details={"job": self.job_name}) from exc
        except subprocess.CalledProcessError as exc:
            raise BackupError(
                "mysqldump failed",
                details={"job": self.job_name, "returncode": exc.returncode, "stderr": exc.stderr},
            ) from exc
        except FileNotFoundError as exc:
            raise BackupError(
                "mysqldump not found — is MySQL client installed?",
                details={"job": self.job_name},
            ) from exc

        output_path = sql_file
        if self.compress:
            output_path = self._compress(sql_file)

        logger.info("[%s] MySQL backup completed: %s", self.job_name, output_path)
        return output_path

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
        return Path(str(file_path) + ".gz")

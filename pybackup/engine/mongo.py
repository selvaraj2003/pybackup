"""
MongoDB Backup Engine using ``mongodump``.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Any

from pybackup.engine.base import BaseBackupEngine
from pybackup.utils.exceptions import BackupError
from pybackup.utils.security import get_secret, mask_secret

logger = logging.getLogger(__name__)


class MongoBackupEngine(BaseBackupEngine):
    """Backup engine wrapping the ``mongodump`` CLI tool."""

    def __init__(
        self,
        job_name: str,
        job_config: dict[str, Any],
        global_config: dict[str, Any],
    ) -> None:
        super().__init__(job_name, job_config, global_config)

        self.host: str = job_config.get("host", "localhost")
        self.port: int = int(job_config.get("port", 27017))
        self.username: str | None = job_config.get("username")
        self.password: str | None = get_secret(
            job_config.get("password"), name="mongodb.password"
        )
        self.auth_db: str = job_config.get("auth_db", "admin")
        self.database: str | None = job_config.get("database")  # None = all DBs

        logger.debug(
            "[%s] Mongo config: host=%s port=%d db=%s user=%s",
            self.job_name, self.host, self.port,
            self.database or "<all>",
            self.username or "<none>",
        )

    def run(self) -> Path:
        output_dir = self.get_output_dir()

        cmd = [
            "mongodump",
            "--host", self.host,
            "--port", str(self.port),
            "--out", str(output_dir),
        ]

        if self.username and self.password:
            cmd += [
                "--username", self.username,
                "--password", self.password,
                "--authenticationDatabase", self.auth_db,
            ]

        if self.database:
            cmd += ["--db", self.database]

        logger.info(
            "[%s] Starting mongodump → %s (db=%s)",
            self.job_name, output_dir, self.database or "<all>",
        )

        self._run_subprocess(cmd)
        logger.info("[%s] MongoDB backup completed", self.job_name)
        return output_dir

    # ─── Helpers ───────────────────────────────────────────────────

    def _run_subprocess(self, cmd: list[str]) -> None:
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
                timeout=3600,
            )
            if result.stdout:
                logger.debug("[%s] mongodump stdout: %s", self.job_name, result.stdout)
        except subprocess.TimeoutExpired as exc:
            raise BackupError(
                "mongodump timed out after 3600 s",
                details={"job": self.job_name},
            ) from exc
        except subprocess.CalledProcessError as exc:
            raise BackupError(
                "mongodump exited with a non-zero status",
                details={
                    "job": self.job_name,
                    "returncode": exc.returncode,
                    "stderr": exc.stderr,
                },
            ) from exc
        except FileNotFoundError as exc:
            raise BackupError(
                "mongodump not found — is it installed and on PATH?",
                details={"job": self.job_name},
            ) from exc

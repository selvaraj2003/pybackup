"""
Abstract base class for all pybackup backup engines.

Lifecycle:  prepare() → run() → finalize()

Concrete engines MUST implement ``run()``.
All other lifecycle hooks are optional overrides.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pybackup.utils.exceptions import BackupError

logger = logging.getLogger(__name__)


class BaseBackupEngine(ABC):
    """
    Abstract base for all backup engines.

    Constructor signature (uniform across all engines)::

        Engine(job_name, job_config, global_config)

    :param job_name:     Logical name for this backup job (from YAML)
    :param job_config:   Engine-specific config block (dict)
    :param global_config: Top-level ``global:`` config block (dict)
    """

    def __init__(
        self,
        job_name: str,
        job_config: dict[str, Any],
        global_config: dict[str, Any],
    ) -> None:
        self.job_name = job_name
        self.job_config = job_config
        self.global_config = global_config

        self.backup_root = Path(global_config.get("backup_root", "/backups"))
        self.compress = job_config.get("compress", global_config.get("compress", False))
        self.retention_days: int = global_config.get("retention_days", 7)

        # Stable timestamp for the whole job run
        self._started_at = datetime.now(tz=timezone.utc)
        self.timestamp = self._started_at.strftime("%Y%m%d_%H%M%S")

        logger.debug(
            "Initialised %s | job=%s backup_root=%s",
            self.__class__.__name__,
            self.job_name,
            self.backup_root,
        )

    # ─── Public API ────────────────────────────────────────────────

    def execute(self) -> dict[str, Any]:
        """
        Run the full backup lifecycle and return a result summary.

        :returns: Dict with keys: job_name, engine, status, output_path, error
        :raises BackupError: On any failure (wraps unexpected exceptions)
        """
        result: dict[str, Any] = {
            "job_name": self.job_name,
            "engine": self.__class__.__name__,
            "started_at": self._started_at.isoformat(),
            "status": "running",
            "output_path": None,
            "error": None,
        }

        logger.info("[%s] ▶ Backup started", self.job_name)

        try:
            self.prepare()
            output_path = self.run()
            self.finalize()

            result["status"] = "success"
            result["output_path"] = str(output_path) if output_path else None
            result["finished_at"] = datetime.now(tz=timezone.utc).isoformat()

            logger.info("[%s] ✔ Backup finished successfully", self.job_name)

        except BackupError as exc:
            result["status"] = "failed"
            result["error"] = str(exc)
            result["finished_at"] = datetime.now(tz=timezone.utc).isoformat()
            logger.error("[%s] ✘ Backup failed: %s", self.job_name, exc)
            raise

        except Exception as exc:
            result["status"] = "crashed"
            result["error"] = str(exc)
            result["finished_at"] = datetime.now(tz=timezone.utc).isoformat()
            logger.exception("[%s] ✘ Backup crashed unexpectedly", self.job_name)
            raise BackupError(
                f"Unexpected error in {self.__class__.__name__}",
                details={"original": str(exc)},
            ) from exc

        return result

    # ─── Lifecycle hooks ───────────────────────────────────────────

    def prepare(self) -> None:
        """Optional pre-backup hook. Override as needed."""

    @abstractmethod
    def run(self) -> Path | None:
        """
        Perform the actual backup.

        :returns: Path to the produced backup file/directory, or None
        :raises BackupError: On failure
        """

    def finalize(self) -> None:
        """Optional post-backup hook. Override as needed."""

    # ─── Helpers ───────────────────────────────────────────────────

    def ensure_dir(self, path: Path) -> None:
        """Create directory tree; raise BackupError on failure."""
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise BackupError(
                f"Cannot create directory: {path}",
                details={"path": str(path), "os_error": str(exc)},
            ) from exc

    def get_output_dir(self) -> Path:
        """
        Resolve the output directory for this job, creating it if needed.

        Priority: job_config["output"] → backup_root / job_name / timestamp
        """
        base = self.job_config.get("output")
        if base:
            out = Path(base) / self.timestamp
        else:
            out = self.backup_root / self.job_name / self.timestamp

        self.ensure_dir(out)
        return out

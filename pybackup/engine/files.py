"""
File and configuration backup engine.

Supports:
- Recursive directory copy with exclude patterns
- Optional gzip compression (tar.gz archive)
"""

from __future__ import annotations

import fnmatch
import logging
import shutil
import tarfile
from pathlib import Path
from typing import Any

from pybackup.engine.base import BaseBackupEngine
from pybackup.utils.exceptions import BackupError

logger = logging.getLogger(__name__)


class FilesBackupEngine(BaseBackupEngine):
    """Backup engine for filesystem paths (configs, app data, etc.)."""

    def run(self) -> Path:
        source_raw = self.job_config.get("source")
        if not source_raw:
            raise BackupError(
                "Missing required config key 'source'",
                details={"job": self.job_name},
            )

        source = Path(source_raw)
        if not source.exists():
            raise BackupError(
                f"Source path does not exist: {source}",
                details={"job": self.job_name, "source": str(source)},
            )

        exclude_patterns: list[str] = self.job_config.get("exclude", [])
        output_dir = self.get_output_dir()

        logger.info(
            "[%s] Backing up %s → %s (compress=%s)",
            self.job_name, source, output_dir, self.compress,
        )

        try:
            if self.compress:
                return self._backup_compressed(source, output_dir, exclude_patterns)
            else:
                return self._backup_copy(source, output_dir, exclude_patterns)
        except BackupError:
            raise
        except Exception as exc:
            raise BackupError(
                "File backup failed unexpectedly",
                details={"job": self.job_name, "error": str(exc)},
            ) from exc

    # ─── Private helpers ───────────────────────────────────────────

    def _backup_copy(
        self,
        source: Path,
        destination: Path,
        exclude_patterns: list[str],
    ) -> Path:
        """Recursively copy files, honouring exclude patterns."""
        copied = 0
        skipped = 0

        for item in source.rglob("*"):
            rel = item.relative_to(source)

            if self._is_excluded(rel, exclude_patterns):
                logger.debug("[%s] Excluded: %s", self.job_name, rel)
                skipped += 1
                continue

            target = destination / rel
            if item.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, target)
                copied += 1

        logger.info(
            "[%s] Copy complete: %d files copied, %d excluded",
            self.job_name, copied, skipped,
        )
        return destination

    def _backup_compressed(
        self,
        source: Path,
        destination: Path,
        exclude_patterns: list[str],
    ) -> Path:
        """Create a gzip-compressed tar archive, honouring exclude patterns."""
        archive_path = destination.parent / f"{destination.name}.tar.gz"

        logger.info("[%s] Creating archive: %s", self.job_name, archive_path)

        def _filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
            if self._is_excluded(Path(tarinfo.name), exclude_patterns):
                return None
            return tarinfo

        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(source, arcname=source.name, filter=_filter)

        logger.info("[%s] Archive created: %s", self.job_name, archive_path)
        return archive_path

    @staticmethod
    def _is_excluded(path: Path, patterns: list[str]) -> bool:
        """Return True if any path component matches an exclude pattern."""
        for part in path.parts:
            for pattern in patterns:
                if fnmatch.fnmatch(part, pattern):
                    return True
        return False

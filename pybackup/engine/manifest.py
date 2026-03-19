"""
Backup manifest engine.

Creates JSON sidecar files that describe:
- Which engine/job produced the backup
- File list with sizes and checksums
- Timing metadata
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pybackup.utils.exceptions import ManifestError

logger = logging.getLogger(__name__)


class BackupManifest:
    """
    Create and read JSON manifest files alongside backup archives.

    Example::

        manifest = BackupManifest("/backups/postgres/job1/20240101_120000")
        manifest_path = manifest.create(
            engine="postgres",
            job_name="prod-db",
            files=[{"path": "prod_db.dump", "size": 1024, "sha256": "abc..."}],
        )
    """

    SUPPORTED_FORMATS = ("json",)

    def __init__(self, output_dir: str | Path, fmt: str = "json") -> None:
        """
        :param output_dir: Directory where manifests are written
        :param fmt:        Manifest format (currently only 'json')
        :raises ManifestError: If format is unsupported
        """
        self.output_dir = Path(output_dir)
        self.fmt = fmt.lower()

        if self.fmt not in self.SUPPORTED_FORMATS:
            raise ManifestError(
                f"Unsupported manifest format: {self.fmt!r}",
                details={"supported": self.SUPPORTED_FORMATS},
            )

        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ManifestError(
                f"Cannot create manifest directory: {self.output_dir}",
                details={"error": str(exc)},
            ) from exc

    # ─── Public API ────────────────────────────────────────────────

    def create(
        self,
        engine: str,
        job_name: str,
        files: list[dict[str, Any]],
        extra: dict[str, Any] | None = None,
    ) -> Path:
        """
        Write a new manifest file and return its path.

        :param engine:   Engine name (e.g. "postgres", "files")
        :param job_name: Job identifier from YAML
        :param files:    List of file metadata dicts
        :param extra:    Optional engine-specific metadata
        :return:         Path to the written manifest
        :raises ManifestError: On write failure
        """
        ts = datetime.now(tz=timezone.utc)
        manifest: dict[str, Any] = {
            "version": 1,
            "engine": engine,
            "job": job_name,
            "created_at": ts.isoformat(),
            "file_count": len(files),
            "files": files,
            "extra": extra or {},
        }

        path = self._manifest_path(engine, job_name, ts)

        try:
            path.write_text(
                json.dumps(manifest, indent=2, default=str),
                encoding="utf-8",
            )
        except OSError as exc:
            raise ManifestError(
                "Failed to write manifest file",
                details={"path": str(path), "error": str(exc)},
            ) from exc

        logger.info("Manifest written: %s", path)
        return path

    def load(self, manifest_path: str | Path) -> dict[str, Any]:
        """
        Load and parse an existing manifest file.

        :param manifest_path: Path to the manifest JSON
        :return:              Parsed manifest dictionary
        :raises ManifestError: If file is missing or malformed
        """
        path = Path(manifest_path)

        if not path.exists():
            raise ManifestError(
                f"Manifest not found: {path}",
                details={"path": str(path)},
            )

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ManifestError(
                "Failed to parse manifest file",
                details={"path": str(path), "error": str(exc)},
            ) from exc

        return data

    # ─── Internals ─────────────────────────────────────────────────

    def _manifest_path(self, engine: str, job_name: str, ts: datetime) -> Path:
        stamp = ts.strftime("%Y%m%d_%H%M%S")
        return self.output_dir / f"{engine}_{job_name}_{stamp}.manifest.json"

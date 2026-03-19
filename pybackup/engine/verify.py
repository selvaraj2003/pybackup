"""
Backup integrity verifier using streaming checksums.

Supports any algorithm available via ``hashlib`` (sha256, sha512, md5, …).
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from pybackup.utils.exceptions import VerificationError

logger = logging.getLogger(__name__)


class BackupVerifier:
    """
    Verify backup files against expected checksums.

    Example::

        verifier = BackupVerifier(algorithm="sha256")
        checksum = verifier.generate_checksum("/backups/mydb.dump")
        verifier.verify_file("/backups/mydb.dump", checksum)
    """

    def __init__(self, algorithm: str = "sha256", chunk_size: int = 65536) -> None:
        """
        :param algorithm:  Hash algorithm name (must be in ``hashlib.algorithms_available``)
        :param chunk_size: Read chunk size in bytes (larger = faster for big files)
        :raises VerificationError: If algorithm is unsupported
        """
        if algorithm not in hashlib.algorithms_available:
            raise VerificationError(
                f"Unsupported checksum algorithm: {algorithm!r}",
                details={"available": sorted(hashlib.algorithms_available)},
            )
        self.algorithm = algorithm
        self.chunk_size = chunk_size

    # ─── Public API ────────────────────────────────────────────────

    def generate_checksum(self, file_path: str | Path) -> str:
        """
        Calculate and return the checksum hex-digest for a file.

        :param file_path: Path to the backup file
        :return:          Hex-digest string
        :raises VerificationError: If file is missing or unreadable
        """
        path = Path(file_path)
        self._assert_exists(path)

        checksum = self._calculate(path)
        logger.debug("Checksum(%s) %s  %s", self.algorithm, checksum, path)
        return checksum

    def verify_file(self, file_path: str | Path, expected_checksum: str) -> bool:
        """
        Verify a file against an expected checksum.

        :param file_path:          Path to the backup file
        :param expected_checksum:  Expected hex-digest
        :return:                   True on success
        :raises VerificationError: On mismatch or file errors
        """
        path = Path(file_path)
        self._assert_exists(path)

        logger.info("Verifying %s [%s]", path.name, self.algorithm)
        actual = self._calculate(path)

        if actual != expected_checksum:
            raise VerificationError(
                f"Checksum mismatch for {path.name}",
                details={
                    "file": str(path),
                    "algorithm": self.algorithm,
                    "expected": expected_checksum,
                    "actual": actual,
                },
            )

        logger.info("✔ Checksum verified: %s", path.name)
        return True

    def write_checksum_file(self, file_path: str | Path) -> Path:
        """
        Write a ``.sha256`` (or other algo) sidecar file next to the backup.

        :param file_path: Backup file to checksum
        :return:          Path to the written sidecar file
        """
        path = Path(file_path)
        checksum = self.generate_checksum(path)
        sidecar = path.with_suffix(path.suffix + f".{self.algorithm}")
        sidecar.write_text(f"{checksum}  {path.name}\n", encoding="utf-8")
        logger.info("Checksum file written: %s", sidecar)
        return sidecar

    # ─── Internals ─────────────────────────────────────────────────

    def _calculate(self, path: Path) -> str:
        try:
            hasher = hashlib.new(self.algorithm)
            with path.open("rb") as fh:
                while chunk := fh.read(self.chunk_size):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except OSError as exc:
            raise VerificationError(
                f"Cannot read file for checksumming: {path}",
                details={"path": str(path), "error": str(exc)},
            ) from exc

    @staticmethod
    def _assert_exists(path: Path) -> None:
        if not path.exists():
            raise VerificationError(
                f"Backup file not found: {path}",
                details={"path": str(path)},
            )

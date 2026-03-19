"""
Custom exception hierarchy for pybackup.

All pybackup-specific errors inherit from PyBackupError.
Every exception accepts an optional `details` parameter for
structured context (dict, str, etc.) separate from the message.
"""

from __future__ import annotations
from typing import Any


class PyBackupError(Exception):
    """
    Base exception for all pybackup errors.

    Attributes:
        message:  Human-readable error description.
        details:  Optional structured context (dict, str, list, …).
    """

    def __init__(self, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | details={self.details}"
        return self.message

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "details": self.details,
        }


class ConfigError(PyBackupError):
    """
    Raised when configuration is invalid or cannot be loaded.

    Examples:
    - YAML syntax errors
    - Missing required fields
    - Invalid types or unsupported options
    """


class EngineError(PyBackupError):
    """
    Raised by backup engines for operation-level failures.

    Examples:
    - External process (pg_dump, mongodump) failed
    - Source directory does not exist
    - Permission denied
    """


class BackupError(PyBackupError):
    """
    Raised for higher-level backup failures.

    Examples:
    - One or more jobs fail
    - Partial completion
    - Pre/post-hook failure
    """


class SecurityError(PyBackupError):
    """
    Raised for security-related issues.

    Examples:
    - Missing required credentials
    - Unsafe file permissions
    - Secret resolution failure
    """


class ManifestError(PyBackupError):
    """
    Raised when manifest creation or parsing fails.

    Examples:
    - Unable to write manifest file
    - Invalid/corrupt manifest
    - Checksum mismatch in manifest
    """


class VerificationError(PyBackupError):
    """
    Raised when backup verification fails.

    Examples:
    - Checksum mismatch
    - Missing backup files
    - Corrupted archive
    """


class DatabaseError(PyBackupError):
    """
    Raised for internal pybackup database (SQLite) errors.

    Examples:
    - Schema migration failure
    - Query execution error
    """


class ServerError(PyBackupError):
    """
    Raised for web server errors.

    Examples:
    - Port already in use
    - Static asset not found
    """

"""
Security helpers for pybackup.

Responsibilities:
- Secure secret resolution from env vars or plain values
- Secret masking for logging
- File permission checks
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Optional

from pybackup.utils.exceptions import SecurityError


def get_secret(
    value: Optional[str],
    *,
    required: bool = False,
    name: str = "SECRET",
) -> Optional[str]:
    """
    Resolve a secret value securely.

    Resolution order:
    1. ``${VAR_NAME}`` syntax  → expanded via os.path.expandvars
    2. ALL_CAPS plain string   → looked up in os.environ
    3. Anything else           → returned as-is

    :param value:    Raw value from config
    :param required: Raise SecurityError if the resolved value is empty
    :param name:     Logical name used in error messages
    :return:         Resolved secret or None
    :raises SecurityError: When ``required=True`` and secret is missing
    """
    if not value:
        if required:
            raise SecurityError(
                f"{name} is required but was not provided",
                details={"field": name},
            )
        return None

    # Expand ${VAR} or $VAR syntax
    resolved = os.path.expandvars(value)

    # If value looks like a bare env var name (e.g. "MYSQL_PASSWORD")
    if resolved == value and value.replace("_", "").isupper() and " " not in value:
        resolved = os.environ.get(value, "")

    if not resolved:
        if required:
            raise SecurityError(
                f"{name} could not be resolved from environment",
                details={"field": name, "raw": mask_secret(value)},
            )
        return None

    return resolved


def mask_secret(secret: Optional[str], show_last: int = 2) -> str:
    """
    Mask a secret for safe logging.

    Example::

        mask_secret("supersecret123")  →  "***********23"

    :param secret:    Value to mask
    :param show_last: Number of trailing characters to reveal
    :return:          Masked string
    """
    if not secret:
        return "******"
    if len(secret) <= show_last:
        return "*" * len(secret)
    return "*" * (len(secret) - show_last) + secret[-show_last:]


def check_file_permissions(path: Path, max_mode: int = 0o600) -> None:
    """
    Raise SecurityError if file permissions are too permissive.

    :param path:     Path to check
    :param max_mode: Maximum allowed permission bits (e.g. 0o600)
    :raises SecurityError: If permissions exceed max_mode
    """
    try:
        file_stat = path.stat()
        current_mode = stat.S_IMODE(file_stat.st_mode)
        if current_mode & ~max_mode:
            raise SecurityError(
                f"Insecure file permissions on {path}",
                details={
                    "path": str(path),
                    "current": oct(current_mode),
                    "max_allowed": oct(max_mode),
                },
            )
    except OSError as exc:
        raise SecurityError(
            f"Cannot stat file {path}: {exc}",
            details={"path": str(path)},
        ) from exc

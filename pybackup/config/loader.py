"""
YAML configuration loader with environment variable expansion.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from pybackup.utils.exceptions import ConfigError


# ─── Env var expansion ──────────────────────────────────────────────

def _expand_env(value: Any) -> Any:
    """Recursively expand ``${VAR}`` / ``$VAR`` in string config values."""
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    if isinstance(value, str):
        return os.path.expandvars(value)
    return value


# ─── Public API ─────────────────────────────────────────────────────

def load_config(config_path: str) -> dict[str, Any]:
    """
    Load, expand, and validate a pybackup YAML configuration file.

    :param config_path: Absolute or relative path to ``pybackup.yaml``
    :return:            Fully resolved configuration dictionary
    :raises ConfigError: If the file is missing, unreadable, or invalid
    """
    path = Path(config_path)

    if not path.exists():
        raise ConfigError(
            f"Config file not found: {path}",
            details={"path": str(path)},
        )
    if not path.is_file():
        raise ConfigError(
            f"Config path is not a regular file: {path}",
            details={"path": str(path)},
        )

    try:
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(
            "Invalid YAML syntax in config file",
            details={"path": str(path), "yaml_error": str(exc)},
        ) from exc
    except OSError as exc:
        raise ConfigError(
            f"Unable to read config file: {exc}",
            details={"path": str(path)},
        ) from exc

    config = _expand_env(raw)
    _validate(config)
    return config


# ─── Validation ─────────────────────────────────────────────────────

_REQUIRED_TOP_LEVEL = ("version", "global")
_SUPPORTED_ENGINES = ("files", "mongodb", "postgresql", "mysql", "mssql")


def _validate(config: dict[str, Any]) -> None:
    """
    Validate the structure and types of a loaded configuration dict.

    :raises ConfigError: On any validation failure
    """
    for field in _REQUIRED_TOP_LEVEL:
        if field not in config:
            raise ConfigError(
                f"Missing required top-level field: '{field}'",
                details={"missing_field": field},
            )

    global_cfg = config["global"]

    if "backup_root" not in global_cfg:
        raise ConfigError(
            "global.backup_root is required",
            details={"section": "global"},
        )

    retention = global_cfg.get("retention_days")
    if retention is not None and not isinstance(retention, int):
        raise ConfigError(
            "global.retention_days must be an integer",
            details={"got": type(retention).__name__},
        )

    for engine in _SUPPORTED_ENGINES:
        engine_cfg = config.get(engine)
        if not engine_cfg or not engine_cfg.get("enabled"):
            continue

        jobs = engine_cfg.get("jobs")
        if jobs is not None and not isinstance(jobs, list):
            raise ConfigError(
                f"{engine}.jobs must be a list",
                details={"engine": engine, "got": type(jobs).__name__},
            )

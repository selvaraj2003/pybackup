"""
Global constants for pybackup.
"""

from pathlib import Path
import sys

# ─── Application ────────────────────────────────────────────────────
APP_NAME = "pybackup-engine"
APP_VERSION = "2.0.0"

# ─── Platform-aware base directories ────────────────────────────────
# Windows : C:\Users\<name>\pybackup\
# Linux   : /var/lib/pybackup/   (or ~/pybackup/ if no root)
# macOS   : ~/Library/Application Support/pybackup/

if sys.platform == "win32":
    _BASE = Path.home() / ".pybackup"
    _LOG_BASE = Path.home() / ".pybackup" / "logs"
    _CONFIG_DIR = Path.home() / ".pybackup"
elif sys.platform == "darwin":
    _BASE = Path.home() / "Library" / "Application Support" / "pybackup"
    _LOG_BASE = _BASE / "logs"
    _CONFIG_DIR = _BASE
else:
    import os

    if os.geteuid() == 0:
        _BASE = Path("/var/lib/pybackup")
        _LOG_BASE = Path("/var/log/pybackup")
        _CONFIG_DIR = Path("/etc/pybackup")
    else:
        _BASE = Path.home() / ".pybackup"
        _LOG_BASE = Path.home() / ".pybackup" / "logs"
        _CONFIG_DIR = Path.home() / ".pybackup"

# ─── Default paths ──────────────────────────────────────────────────
DEFAULT_CONFIG_PATH = _CONFIG_DIR / "pybackup.yaml"
DEFAULT_ENV_FILE = _CONFIG_DIR / "pybackup.env"
DEFAULT_BACKUP_ROOT = _BASE / "backups"
DEFAULT_WORK_DIR = _BASE
DEFAULT_LOG_DIR = _LOG_BASE
DEFAULT_LOG_FILE = _LOG_BASE / "pybackup.log"
DEFAULT_DB_PATH = _BASE / "pybackup.db"

# ─── Backup defaults ────────────────────────────────────────────────
DEFAULT_RETENTION_DAYS = 7
DEFAULT_COMPRESS = True
DEFAULT_VERIFY_CHECKSUM = True

# ─── Database ports ─────────────────────────────────────────────────
DEFAULT_MYSQL_PORT = 3306
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_MONGODB_PORT = 27017
DEFAULT_MSSQL_PORT = 1433

# ─── File backup ────────────────────────────────────────────────────
DEFAULT_EXCLUDES = ["*.log", "*.tmp", "*.cache", "__pycache__", ".git"]

# ─── Logging ────────────────────────────────────────────────────────
DEFAULT_LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"

# ─── Web server ─────────────────────────────────────────────────────
DEFAULT_SERVER_HOST = "0.0.0.0"
DEFAULT_SERVER_PORT = 8200

# ─── Exit codes ─────────────────────────────────────────────────────
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_CRASH = 2

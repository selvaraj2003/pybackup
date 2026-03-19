"""
Global constants for pybackup.
"""

from pathlib import Path

# ─── Application ────────────────────────────────────────────────────
APP_NAME = "pybackup"
APP_VERSION = "1.0.0"

# ─── Default paths ──────────────────────────────────────────────────
DEFAULT_CONFIG_PATH = Path("/etc/pybackup/pybackup.yaml")
DEFAULT_ENV_FILE = Path("/etc/pybackup/pybackup.env")
DEFAULT_BACKUP_ROOT = Path("/backups")
DEFAULT_WORK_DIR = Path("/var/lib/pybackup")
DEFAULT_LOG_DIR = Path("/var/log/pybackup")
DEFAULT_LOG_FILE = DEFAULT_LOG_DIR / "pybackup.log"
DEFAULT_DB_PATH = DEFAULT_WORK_DIR / "pybackup.db"

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

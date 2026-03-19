#!/usr/bin/env python3
"""
PyBackup CLI
============

Commands:
    run          — execute backup jobs from a YAML config
    verify       — verify backup file integrity
    config-check — validate configuration without running
    serve        — start the web dashboard
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click

from pybackup.config.loader import load_config
from pybackup.constants import (
    DEFAULT_SERVER_HOST,
    DEFAULT_SERVER_PORT,
    DEFAULT_DB_PATH,
)
from pybackup.utils.exceptions import PyBackupError
from pybackup.utils.logger import configure_logging

logger = logging.getLogger("pybackup.cli")


# ─── Main group ─────────────────────────────────────────────────────

@click.group()
@click.version_option("1.0.0", prog_name="pybackup")
def main() -> None:
    """PyBackup — production-ready backup engine with web UI."""


# ─── run ────────────────────────────────────────────────────────────

@main.command()
@click.option(
    "--config", "-c",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to pybackup YAML configuration file",
)
@click.option("--dry-run", is_flag=True, help="Validate config and print jobs without running")
def run(config: Path, dry_run: bool) -> None:
    """Run backup jobs defined in the configuration file."""
    try:
        cfg = load_config(str(config))
        global_cfg = cfg.get("global", {})
        configure_logging(
            log_level=global_cfg.get("log_level", "INFO"),
            log_file=global_cfg.get("log_file"),
        )

        logger.info("PyBackup starting — config: %s", config)

        if dry_run:
            _print_jobs(cfg)
            click.secho("Dry run complete — no backups executed.", fg="cyan")
            return

        # Import engines here to avoid circular imports at module level
        from pybackup.engine.files    import FilesBackupEngine
        from pybackup.engine.mongo    import MongoBackupEngine
        from pybackup.engine.postgres import PostgresBackupEngine
        from pybackup.engine.mysql    import MySQLBackupEngine
        from pybackup.engine.mssql    import MSSQLBackupEngine
        from pybackup.db.database     import Database

        db_path = global_cfg.get("db_path", str(DEFAULT_DB_PATH))
        db = Database(db_path)

        engine_map = {
            "files":      FilesBackupEngine,
            "mongodb":    MongoBackupEngine,
            "postgresql": PostgresBackupEngine,
            "mysql":      MySQLBackupEngine,
            "mssql":      MSSQLBackupEngine,
        }

        failures = []

        for engine_key, EngineClass in engine_map.items():
            engine_cfg = cfg.get(engine_key, {})
            if not engine_cfg or not engine_cfg.get("enabled"):
                continue

            jobs = engine_cfg.get("jobs") or [engine_cfg]
            for job in jobs:
                job_name = job.get("name", engine_key)
                run_id = db.create_run(job_name, engine_key)
                try:
                    result = EngineClass(job_name, job, global_cfg).execute()
                    db.finish_run(run_id, status="success", output_path=result.get("output_path"))
                    click.secho(f"  ✔ {job_name}", fg="green")
                except PyBackupError as exc:
                    db.finish_run(run_id, status="failed", error=str(exc))
                    click.secho(f"  ✘ {job_name}: {exc}", fg="red")
                    failures.append(job_name)

        if failures:
            click.secho(f"\n{len(failures)} job(s) failed: {', '.join(failures)}", fg="red")
            sys.exit(1)

        logger.info("All backup jobs completed successfully")
        click.secho("\nAll jobs completed ✔", fg="green")

    except PyBackupError as exc:
        click.secho(f"Error: {exc}", fg="red", err=True)
        sys.exit(1)
    except Exception as exc:
        click.secho(f"Unexpected error: {exc}", fg="red", err=True)
        logger.exception("Unhandled exception in run command")
        sys.exit(2)


# ─── verify ─────────────────────────────────────────────────────────

@main.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=Path))
@click.option("--checksum", "-s", required=True, help="Expected checksum hex-digest")
@click.option("--algorithm", "-a", default="sha256", show_default=True,
              help="Hash algorithm (sha256, sha512, md5, …)")
def verify(file_path: Path, checksum: str, algorithm: str) -> None:
    """Verify a backup file against an expected checksum."""
    from pybackup.engine.verify import BackupVerifier

    configure_logging()
    try:
        verifier = BackupVerifier(algorithm=algorithm)
        verifier.verify_file(file_path, checksum)
        click.secho(f"✔ Checksum verified ({algorithm}): {file_path.name}", fg="green")
    except PyBackupError as exc:
        click.secho(f"✘ Verification failed: {exc}", fg="red", err=True)
        sys.exit(1)


# ─── checksum ───────────────────────────────────────────────────────

@main.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=Path))
@click.option("--algorithm", "-a", default="sha256", show_default=True)
def checksum(file_path: Path, algorithm: str) -> None:
    """Generate and print the checksum of a backup file."""
    from pybackup.engine.verify import BackupVerifier

    configure_logging()
    try:
        verifier = BackupVerifier(algorithm=algorithm)
        digest = verifier.generate_checksum(file_path)
        click.echo(f"{digest}  {file_path}")
    except PyBackupError as exc:
        click.secho(f"Error: {exc}", fg="red", err=True)
        sys.exit(1)


# ─── config-check ───────────────────────────────────────────────────

@main.command(name="config-check")
@click.option(
    "--config", "-c",
    required=True,
    type=click.Path(exists=True, path_type=Path),
)
def config_check(config: Path) -> None:
    """Validate a pybackup YAML configuration file."""
    try:
        cfg = load_config(str(config))
        _print_jobs(cfg)
        click.secho("\nConfiguration is valid ✔", fg="green")
    except Exception as exc:
        click.secho(f"Invalid configuration: {exc}", fg="red", err=True)
        sys.exit(1)


# ─── serve ──────────────────────────────────────────────────────────

@main.command()
@click.option("--host",   default=DEFAULT_SERVER_HOST, show_default=True, help="Bind address")
@click.option("--port",   default=DEFAULT_SERVER_PORT, show_default=True, type=int, help="Port")
@click.option("--db",     default=str(DEFAULT_DB_PATH), show_default=True, help="SQLite database path")
@click.option("--config", "-c", type=click.Path(path_type=Path), default=None,
              help="Optional YAML config for log level etc.")
def serve(host: str, port: int, db: str, config: Path | None) -> None:
    """Start the PyBackup web dashboard."""
    try:
        log_level = "INFO"
        if config and config.exists():
            cfg = load_config(str(config))
            log_level = cfg.get("global", {}).get("log_level", "INFO")

        configure_logging(log_level)

        from pybackup.db.database  import Database
        from pybackup.server.httpserver import PyBackupServer

        database = Database(db)
        server = PyBackupServer(db=database, host=host, port=port)

        click.secho(f"PyBackup dashboard → http://{host}:{port}", fg="cyan", bold=True)
        click.secho(f"Database: {db}", fg="bright_black")
        click.secho("Press Ctrl-C to stop.\n", fg="bright_black")

        server.start()

    except PyBackupError as exc:
        click.secho(f"Error: {exc}", fg="red", err=True)
        sys.exit(1)
    except OSError as exc:
        click.secho(f"Cannot start server: {exc}", fg="red", err=True)
        sys.exit(1)


# ─── Helpers ────────────────────────────────────────────────────────

def _print_jobs(cfg: dict) -> None:
    engines = ("files", "mongodb", "postgresql", "mysql", "mssql")
    click.secho("\nEnabled engines:", bold=True)
    for key in engines:
        ec = cfg.get(key)
        if ec and ec.get("enabled"):
            jobs = ec.get("jobs") or [ec]
            for j in jobs:
                name = j.get("name", key)
                click.secho(f"  • {key:<14} {name}", fg="cyan")


# ─── Entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    main()

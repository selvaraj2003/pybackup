#!/usr/bin/env python3
"""
PyBackup CLI
============

All actions go through this single CLI entry point.

Commands:
    run            — execute backup jobs from a YAML config
    serve          — start the web dashboard
    verify         — verify backup file integrity
    checksum       — generate checksum for a backup file
    config-check   — validate configuration without running
    tables         — show database tables, row counts and recent activity
    add-run        — manually add a backup run record

User management (subgroup):
    user add          — create a new dashboard user
    user list         — list all users
    user delete       — delete a user
    user set-password — reset a user's password (admin, no current pw needed)
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
@click.version_option("2.0.0", prog_name="pybackup")
def main() -> None:
    """PyBackup — production-ready backup engine with web UI.

    All backup, dashboard, and user management actions live here.
    Run any command with --help for full options.

    \b
    Quick start:
      pybackup user add --username admin --role admin
      pybackup serve
      pybackup run -c pybackup.yaml
    """


# ─── run ────────────────────────────────────────────────────────────


@main.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to pybackup YAML configuration file",
)
@click.option(
    "--dry-run", is_flag=True, help="Validate config and print jobs — no backups executed"
)
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

        from pybackup.engine.files import FilesBackupEngine
        from pybackup.engine.mongo import MongoBackupEngine
        from pybackup.engine.postgres import PostgresBackupEngine
        from pybackup.engine.mysql import MySQLBackupEngine
        from pybackup.engine.mssql import MSSQLBackupEngine
        from pybackup.db.backends import get_database

        db = get_database(cfg)

        engine_map = {
            "files": FilesBackupEngine,
            "mongodb": MongoBackupEngine,
            "postgresql": PostgresBackupEngine,
            "mysql": MySQLBackupEngine,
            "mssql": MSSQLBackupEngine,
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


# ─── serve ──────────────────────────────────────────────────────────


@main.command()
@click.option(
    "--host",
    default=DEFAULT_SERVER_HOST,
    show_default=True,
    help="Bind address (0.0.0.0 = all interfaces)",
)
@click.option(
    "--port", default=DEFAULT_SERVER_PORT, show_default=True, type=int, help="Port number"
)
@click.option(
    "--db",
    default=str(DEFAULT_DB_PATH),
    show_default=True,
    help="SQLite database path (override via config database.backend)",
)
@click.option(
    "--config",
    "-c",
    type=click.Path(path_type=Path),
    default=None,
    help="Optional YAML config (for log level, database backend, etc.)",
)
def serve(host: str, port: int, db: str, config: Path | None) -> None:
    """Start the PyBackup web dashboard.

    \b
    Examples:
      pybackup serve
      pybackup serve --port 8200
      pybackup serve --config pybackup.yaml
    """
    try:
        full_cfg: dict = {}
        log_level = "INFO"

        if config and config.exists():
            full_cfg = load_config(str(config))
            log_level = full_cfg.get("global", {}).get("log_level", "INFO")
            db = full_cfg.get("global", {}).get("db_path", db)

        configure_logging(log_level)

        from pybackup.db.backends import get_database
        from pybackup.server.httpserver import PyBackupServer
        from pybackup.auth import UserDB

        if "database" in full_cfg:
            database = get_database(full_cfg)
        else:
            from pybackup.db.database import Database

            database = Database(db)

        user_db = UserDB(db)

        click.secho("\n  PyBackup Dashboard", bold=True, fg="bright_white")
        click.secho("  ─────────────────────────────────────", fg="bright_black")
        click.secho(f"  Dashboard → http://{host}:{port}", fg="cyan", bold=True)
        click.secho(f"  Database  : {db}", fg="bright_black")

        if not user_db.has_any_user():
            click.secho(
                "\n  ⚠  No users found. Create one first:\n"
                "     pybackup user add --username admin --role admin\n",
                fg="yellow",
            )
        else:
            click.secho(f"  Users     : {user_db.count_admins()} admin(s)", fg="bright_black")

        click.secho("  Press Ctrl-C to stop.\n", fg="bright_black")

        server = PyBackupServer(db=database, user_db=user_db, host=host, port=port)
        server.start()

    except PyBackupError as exc:
        click.secho(f"Error: {exc}", fg="red", err=True)
        sys.exit(1)
    except OSError as exc:
        click.secho(f"Cannot start server: {exc}", fg="red", err=True)
        sys.exit(1)


# ─── verify ─────────────────────────────────────────────────────────


@main.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=Path))
@click.option("--checksum", "-s", required=True, help="Expected checksum hex-digest")
@click.option(
    "--algorithm",
    "-a",
    default="sha256",
    show_default=True,
    help="Hash algorithm (sha256, sha512, md5, …)",
)
def verify(file_path: Path, checksum: str, algorithm: str) -> None:
    """Verify a backup file against an expected checksum.

    \b
    Example:
      pybackup verify /backups/db.dump --checksum abc123...
    """
    from pybackup.engine.verify import BackupVerifier

    configure_logging()
    try:
        BackupVerifier(algorithm=algorithm).verify_file(file_path, checksum)
        click.secho(f"✔ Checksum verified ({algorithm}): {file_path.name}", fg="green")
    except PyBackupError as exc:
        click.secho(f"✘ Verification failed: {exc}", fg="red", err=True)
        sys.exit(1)


# ─── checksum ───────────────────────────────────────────────────────


@main.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=Path))
@click.option("--algorithm", "-a", default="sha256", show_default=True, help="Hash algorithm")
def checksum(file_path: Path, algorithm: str) -> None:
    """Generate and print the checksum of a backup file.

    \b
    Example:
      pybackup checksum /backups/db.dump
      pybackup checksum /backups/db.dump --algorithm sha512
    """
    from pybackup.engine.verify import BackupVerifier

    configure_logging()
    try:
        digest = BackupVerifier(algorithm=algorithm).generate_checksum(file_path)
        click.echo(f"{digest}  {file_path}")
    except PyBackupError as exc:
        click.secho(f"Error: {exc}", fg="red", err=True)
        sys.exit(1)


# ─── config-check ───────────────────────────────────────────────────


@main.command(name="config-check")
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True, path_type=Path),
)
def config_check(config: Path) -> None:
    """Validate a pybackup YAML configuration file.

    \b
    Example:
      pybackup config-check -c pybackup.yaml
    """
    try:
        cfg = load_config(str(config))
        _print_jobs(cfg)
        click.secho("\nConfiguration is valid ✔", fg="green")
    except Exception as exc:
        click.secho(f"Invalid configuration: {exc}", fg="red", err=True)
        sys.exit(1)


# ─── tables ─────────────────────────────────────────────────────────


@main.command()
@click.option("--db", default=str(DEFAULT_DB_PATH), show_default=True, help="Database path")
@click.option(
    "--config",
    "-c",
    type=click.Path(path_type=Path),
    default=None,
    help="Optional YAML config (to resolve database backend)",
)
def tables(db: str, config: Path | None) -> None:
    """Show database tables, row counts, recent runs and users.

    \b
    Examples:
      pybackup tables
      pybackup tables --db ./mybackup.db
      pybackup tables --config pybackup.yaml
    """
    full_cfg: dict = {}
    if config and config.exists():
        full_cfg = load_config(str(config))
        db = full_cfg.get("global", {}).get("db_path", db)

    configure_logging("WARNING")

    if "database" in full_cfg:
        from pybackup.db.backends import get_database

        database = get_database(full_cfg)
    else:
        from pybackup.db.database import Database

        database = Database(db)

    click.secho("\n  PyBackup — Database Info", bold=True)
    click.secho(f"  Path: {db}", fg="bright_black")
    click.secho("  " + "─" * 44, fg="bright_black")

    # ── Table row counts ────────────────────────────────────────────
    click.secho("\n  Tables", bold=True)
    try:
        stats = database.stats()
        rows = [
            ("backup_runs", stats["total"]),
            ("backup_files", len(database.list_files(0)) if stats["total"] else 0),
            ("settings", 0),
            ("users", 0),
        ]
        # Get settings count
        try:
            if hasattr(database, "_backend"):
                import sqlite3

                conn = sqlite3.connect(db)
                rows[2] = ("settings", conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0])
                rows[3] = ("users", conn.execute("SELECT COUNT(*) FROM users").fetchone()[0])
                conn.close()
        except Exception:
            pass
        for tbl, count in rows:
            click.secho(f"  📋 {tbl:<20} {count:>6} rows", fg="cyan")
    except Exception as exc:
        click.secho(f"  ✘ Cannot read tables: {exc}", fg="red")

    # ── Stats summary ───────────────────────────────────────────────
    click.secho("\n  Backup Summary", bold=True)
    try:
        stats = database.stats()
        click.secho(f"  Total runs   : {stats['total']}", fg="cyan")
        click.secho(f"  Successful   : {stats['success']}", fg="green")
        click.secho(f"  Failed       : {stats['failed']}", fg="red")
        click.secho(f"  Running      : {stats['running']}", fg="blue")
        click.secho(
            f"  Success rate : {stats['success_rate']}%",
            fg="green" if stats["success_rate"] >= 90 else "yellow",
        )
    except Exception as exc:
        click.secho(f"  ✘ {exc}", fg="red")

    # ── Recent runs ─────────────────────────────────────────────────
    click.secho("\n  Recent Runs (last 10)", bold=True)
    try:
        runs = database.list_runs(limit=10)
        if not runs:
            click.secho("  No runs yet.", fg="bright_black")
        else:
            click.secho(
                f"  {'#':<5} {'Job':<22} {'Engine':<14} {'Status':<10} Started",
                fg="bright_black",
            )
            click.secho("  " + "─" * 68, fg="bright_black")
            for r in runs:
                color = {
                    "success": "green",
                    "failed": "red",
                    "crashed": "red",
                    "running": "blue",
                }.get(r["status"], "white")
                started = (r.get("started_at") or "")[:16].replace("T", " ")
                click.secho(
                    f"  #{r['id']:<4} {r['job_name']:<22} {r['engine']:<14} ",
                    nl=False,
                )
                click.secho(f"{r['status']:<10}", fg=color, nl=False)
                click.secho(f" {started}")
    except Exception as exc:
        click.secho(f"  ✘ {exc}", fg="red")

    # ── Users ───────────────────────────────────────────────────────
    click.secho("\n  Users", bold=True)
    try:
        from pybackup.auth import UserDB

        udb = UserDB(db)
        users = udb.list_users()
        if not users:
            click.secho("  No users yet.", fg="bright_black")
            click.secho(
                "  Create one: pybackup user add --username admin --role admin",
                fg="yellow",
            )
        else:
            for u in users:
                role_color = "cyan" if u["role"] == "admin" else "bright_black"
                last = (u.get("last_login") or "never")[:16].replace("T", " ")
                click.secho(f"  👤 {u['username']:<20} ", nl=False)
                click.secho(f"role={u['role']:<8}", fg=role_color, nl=False)
                click.secho(f"  last login: {last}")
    except Exception as exc:
        click.secho(f"  ✘ Cannot read users: {exc}", fg="red")

    click.echo()


# ─── add-run ────────────────────────────────────────────────────────


@main.command(name="add-run")
@click.option("--job", required=True, help="Job name")
@click.option(
    "--engine",
    required=True,
    type=click.Choice(["files", "postgresql", "mongodb", "mysql", "mssql", "manual"]),
    help="Backup engine type",
)
@click.option(
    "--status",
    default="success",
    show_default=True,
    type=click.Choice(["success", "failed", "crashed", "running"]),
    help="Run status",
)
@click.option("--output", default=None, help="Output path of the backup")
@click.option("--error", default=None, help="Error message (for failed runs)")
@click.option("--db", default=str(DEFAULT_DB_PATH), show_default=True, help="Database path")
@click.option(
    "--config", "-c", type=click.Path(path_type=Path), default=None, help="Optional YAML config"
)
def add_run(
    job: str,
    engine: str,
    status: str,
    output: str | None,
    error: str | None,
    db: str,
    config: Path | None,
) -> None:
    """Manually add a backup run record to the database.

    Useful for importing historical runs or testing the dashboard.

    \b
    Examples:
      pybackup add-run --job prod-db --engine postgresql --status success
      pybackup add-run --job nightly --engine files --status failed --error "disk full"
    """
    full_cfg: dict = {}
    if config and config.exists():
        full_cfg = load_config(str(config))
        db = full_cfg.get("global", {}).get("db_path", db)

    configure_logging("WARNING")

    if "database" in full_cfg:
        from pybackup.db.backends import get_database

        database = get_database(full_cfg)
    else:
        from pybackup.db.database import Database

        database = Database(db)

    run_id = database.create_run(job, engine)
    database.finish_run(run_id, status=status, output_path=output, error=error)
    click.secho(f"✔ Run #{run_id} added:  {job}  [{engine}]  →  {status}", fg="green")


# ─── user group ─────────────────────────────────────────────────────


@main.group()
def user() -> None:
    """Manage dashboard users.

    \b
    Commands:
      pybackup user add          --username admin --role admin
      pybackup user list
      pybackup user delete       --username alice
      pybackup user set-password --username admin
    """


@user.command(name="add")
@click.option("--username", "-u", required=True, help="Username")
@click.option("--password", "-p", default=None, help="Password (prompted if omitted)")
@click.option(
    "--role",
    "-r",
    default="viewer",
    show_default=True,
    type=click.Choice(["admin", "viewer"]),
    help="User role",
)
@click.option("--email", "-e", default=None, help="Email address (optional)")
@click.option("--db", default=str(DEFAULT_DB_PATH), help="Database path")
def user_add(
    username: str,
    password: str | None,
    role: str,
    email: str | None,
    db: str,
) -> None:
    """Add a new dashboard user."""
    from pybackup.auth import UserDB
    from pybackup.utils.exceptions import SecurityError

    configure_logging()

    if not password:
        password = click.prompt("Password", hide_input=True, confirmation_prompt=True)

    if len(password) < 8:
        click.secho("Error: Password must be at least 8 characters.", fg="red", err=True)
        sys.exit(1)

    try:
        udb = UserDB(db)
        uid = udb.create_user(username, password, role=role, email=email)
        click.secho(f"✔ User created: {username}  (id={uid}  role={role})", fg="green")
    except SecurityError as exc:
        click.secho(f"Error: {exc}", fg="red", err=True)
        sys.exit(1)


@user.command(name="list")
@click.option("--db", default=str(DEFAULT_DB_PATH), help="Database path")
def user_list(db: str) -> None:
    """List all dashboard users."""
    from pybackup.auth import UserDB

    configure_logging()
    udb = UserDB(db)
    users = udb.list_users()

    if not users:
        click.echo("No users found.")
        click.secho(
            "Create one: pybackup user add --username admin --role admin",
            fg="yellow",
        )
        return

    click.secho(
        f"\n  {'ID':<5} {'Username':<20} {'Role':<10} " f"{'Email':<26} {'Last Login'}",
        bold=True,
    )
    click.secho("  " + "─" * 72, fg="bright_black")
    for u in users:
        role_color = "cyan" if u["role"] == "admin" else "white"
        last = (u.get("last_login") or "—")[:16].replace("T", " ")
        click.secho(f"  {u['id']:<5} {u['username']:<20} ", nl=False)
        click.secho(f"{u['role']:<10}", fg=role_color, nl=False)
        click.secho(f" {(u['email'] or '—'):<26} {last}")
    click.echo()


@user.command(name="delete")
@click.option("--username", "-u", required=True, help="Username to delete")
@click.option("--db", default=str(DEFAULT_DB_PATH), help="Database path")
def user_delete(username: str, db: str) -> None:
    """Delete a dashboard user."""
    from pybackup.auth import UserDB

    configure_logging()
    udb = UserDB(db)
    user = udb.get_by_username(username)

    if user is None:
        click.secho(f"User not found: {username}", fg="red", err=True)
        sys.exit(1)

    if udb.count_admins() <= 1 and user["role"] == "admin":
        click.secho("Cannot delete the last admin account.", fg="red", err=True)
        sys.exit(1)

    if not click.confirm(f"Delete user '{username}'?"):
        click.echo("Aborted.")
        return

    udb.delete_user(user["id"])
    click.secho(f"✔ User deleted: {username}", fg="green")


@user.command(name="set-password")
@click.option("--username", "-u", required=True, help="Username")
@click.option("--db", default=str(DEFAULT_DB_PATH), help="Database path")
def user_set_password(username: str, db: str) -> None:
    """Reset a user's password (admin operation — no current password needed)."""
    from pybackup.auth import UserDB

    configure_logging()
    udb = UserDB(db)
    user = udb.get_by_username(username)

    if user is None:
        click.secho(f"User not found: {username}", fg="red", err=True)
        sys.exit(1)

    new_pw = click.prompt("New password", hide_input=True, confirmation_prompt=True)
    if len(new_pw) < 8:
        click.secho("Password must be at least 8 characters.", fg="red", err=True)
        sys.exit(1)

    udb.update_password(user["id"], new_pw)
    click.secho(f"✔ Password updated for {username}", fg="green")


# ─── Helpers ────────────────────────────────────────────────────────


def _print_jobs(cfg: dict) -> None:
    engines = ("files", "mongodb", "postgresql", "mysql", "mssql")
    click.secho("\nEnabled engines:", bold=True)
    for key in engines:
        ec = cfg.get(key)
        if ec and ec.get("enabled"):
            jobs = ec.get("jobs") or [ec]
            for j in jobs:
                click.secho(f"  • {key:<14} {j.get('name', key)}", fg="cyan")


# ─── Entry point ────────────────────────────────────────────────────

# if __name__ == "__main__":
#     main()

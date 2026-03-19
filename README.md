# PyBackup

> Production-grade backup engine for files and databases — with a built-in web dashboard.

[![PyPI version](https://img.shields.io/pypi/v/pybackup)](https://pypi.org/project/pybackup)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://python.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

## Features

| Feature | Detail |
|---|---|
| **Engines** | Files, MongoDB, PostgreSQL, MySQL, MS SQL Server |
| **Web Dashboard** | Pure Python stdlib — no Flask/FastAPI/Django |
| **Database** | SQLite by default (WAL mode, zero config) |
| **CLI** | `run`, `verify`, `checksum`, `config-check`, `serve` |
| **Security** | Secrets resolved from env vars, never hard-coded |
| **Fonts** | Space Grotesk headings · Inter body |
| **Theme** | Dark / Light toggle |

---

## Install

```bash
pip install pybackup
```

---

## Quick Start

### 1. Write a config

```yaml
# pybackup.yaml
version: 1

global:
  backup_root: /backups
  retention_days: 7
  compress: true
  log_level: INFO

postgresql:
  enabled: true
  name: prod-db
  host: localhost
  port: 5432
  database: myapp
  username: backup_user
  password: ${PGPASSWORD}    # resolved from env

files:
  enabled: true
  name: app-configs
  source: /etc/myapp
  exclude: ["*.log", "*.tmp"]
```

### 2. Run backups

```bash
pybackup run --config pybackup.yaml
```

### 3. Start the dashboard

```bash
pybackup serve --port 8741
# → http://localhost:8741
```

---

## CLI Reference

```
pybackup run           -c config.yaml [--dry-run]
pybackup verify        FILE -s CHECKSUM [-a sha256]
pybackup checksum      FILE [-a sha256]
pybackup config-check  -c config.yaml
pybackup serve         [--host 0.0.0.0] [--port 8741] [--db /path/to.db]
```

---

## Dashboard

The built-in web server serves a single-page dashboard with:

- **Stats cards** — total runs, success, failed, success rate
- **Activity chart** — 30-day bar chart (Chart.js)
- **Engine doughnut** — breakdown by engine
- **Run history** — filterable, paginated table with delete support
- **Run detail modal** — full metadata including error messages
- **Settings** — theme, retention, log level persisted to SQLite
- **Dark / Light** theme toggle (Space Grotesk + Inter fonts)

---

## REST API

All served under `/api/`:

| Method | Path | Description |
|---|---|---|
| GET | `/api/stats` | Dashboard statistics |
| GET | `/api/runs` | Paginated run list (`?limit=&offset=&job=&status=`) |
| POST | `/api/runs` | Create a run entry |
| GET | `/api/runs/:id` | Single run detail + files |
| DELETE | `/api/runs/:id` | Delete a run |
| GET | `/api/settings` | All settings |
| POST | `/api/settings` | Update settings |

---

## Project Structure

```
pybackup/
├── cli.py              # Click CLI entry point
├── constants.py        # Global defaults
├── config/
│   └── loader.py       # YAML loader + validation
├── engine/
│   ├── base.py         # Abstract BaseBackupEngine
│   ├── files.py        # File/dir backup
│   ├── mongo.py        # MongoDB (mongodump)
│   ├── postgres.py     # PostgreSQL (pg_dump)
│   ├── mysql.py        # MySQL (mysqldump)
│   ├── mssql.py        # MSSQL (sqlcmd)
│   ├── verify.py       # Checksum verification
│   └── manifest.py     # JSON manifests
├── db/
│   └── database.py     # SQLite persistence layer
├── server/
│   ├── httpserver.py   # Pure stdlib HTTP server + Router
│   └── handlers.py     # REST API handlers
├── static/
│   ├── index.html      # SPA dashboard
│   ├── css/app.css     # Styles (Space Grotesk + Inter)
│   └── js/app.js       # Vanilla JS SPA
└── utils/
    ├── exceptions.py   # Exception hierarchy
    ├── logger.py       # Logging setup
    └── security.py     # Secret resolution + masking
```

---

## License

MIT

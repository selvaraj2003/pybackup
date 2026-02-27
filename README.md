# PyBackup – Production Backup Engine

A production-ready Python CLI tool for automated backup of files, databases,
and system configurations.

---

## Overview

PyBackup is a lightweight, extensible backup engine designed for Linux servers
and DevOps environments.

It solves the problem of managing **multiple backup types**
using a single, unified configuration and command-line interface.

**Intended for:**

- System Administrators
- DevOps Engineers
- Backend Developers
- Small to medium production environments

The goal is to provide a **simple, scriptable,
and reliable backup solution** without vendor lock-in.

---

## Features

- YAML-based configuration
- CLI-driven execution
- File & config backups
- Backup verification & checksums

- MongoDB backups
- PostgreSQL backups
- MySQL backups
- MS SQL Server backups

- Env-based secret handling
- Cron & systemd friendly
- Modular architecture
- Production-safe logging

---

## Installation

### Using pip

```bash
pip install pybackup
```

### From source

```bash
git clone https://github.com/selvaraj2003/pybackup.git
cd pybackup
pip install .
```

---

## Requirements

- Python 3.9+
- Linux (recommended)
- mongodump
- pg_dump
- mysqldump
- sqlcmd (MS SQL Server)

---

## Configuration

```yaml
version: 1
global:
  backup_root: /backups
  retention_days: 7
  log_level: INFO

files:
  enabled: true
  jobs:
    - name: nginx_config
      source: /etc/nginx
      output: /backups/files/nginx
```

Secrets are provided via environment variables.

---

## Usage

```bash
pybackup --help
```

```bash
pybackup run --config /etc/pybackup/pybackup.yaml
```

---

## Scheduling

### Cron

```bash
0 2 * * * /usr/bin/pybackup run --config /etc/pybackup/pybackup.yaml
```

### Systemd

```ini
[Unit]
Description=PyBackup Service

[Service]
ExecStart=/usr/bin/pybackup run --config /etc/pybackup/pybackup.yaml
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

---

## Project Structure

```
pybackup/
├── pybackup/
│   ├── cli.py
│   ├── engine/
│   ├── config/
│   ├── utils/
│   └── constants.py
├── tests/
├── scripts/
├── examples/
├── README.md
└── pyproject.toml
```

---

## License

MIT License © 2026 Selvaraj Iyyappan
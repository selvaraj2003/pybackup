"""
tests/test_all.py
=================
Complete PyBackup test suite — all modules in one file.

Sections:
  1.  Exceptions
  2.  Security helpers
  3.  Config loader
  4.  Backup verifier
  5.  Backup manifest
  6.  Files engine
  7.  MongoDB engine
  8.  PostgreSQL engine
  9.  MySQL engine
  10. MSSQL engine
  11. Database (SQLite)
  12. HTTP router
  13. REST API handlers
  14. CLI commands

Run with pytest:
    pytest tests/test_all.py -v
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── pytest compatibility shim (allows running without pytest installed) ──
try:
    import pytest
except ImportError:

    class _PytestShim:
        class approx:
            def __init__(self, expected, **kw):
                self.expected = expected
                self.kw = kw

            def __eq__(self, other):
                abs_tol = self.kw.get("abs", 1e-6)
                return abs(other - self.expected) <= abs_tol

            def __repr__(self):
                return f"approx({self.expected})"

        @staticmethod
        def raises(exc, match=None):
            import re

            class _Raises:
                def __enter__(self):
                    return self

                def __exit__(self, tp, val, tb):
                    if tp is None:
                        raise AssertionError(f"Expected {exc} but no exception raised")
                    if not issubclass(tp, exc):
                        return False
                    if match and not re.search(match, str(val)):
                        raise AssertionError(f"Pattern {match!r} not found in {val!r}")
                    return True

            return _Raises()

        @staticmethod
        def fixture(fn=None, **kw):
            if fn:
                return fn
            return lambda f: f

    pytest = _PytestShim()


# ════════════════════════════════════════════════════════════════════
# Shared helpers
# ════════════════════════════════════════════════════════════════════


def _global(tmp_path, **kw):
    return {"backup_root": str(tmp_path / "backups"), "compress": False, **kw}


def _proc_ok(**kw):
    m = MagicMock()
    m.returncode = 0
    m.stdout = kw.get("stdout", b"")
    m.stderr = kw.get("stderr", b"")
    return m


def _make_yaml(tmp_path: Path, **overrides) -> Path:
    """
    Build a minimal valid YAML config.

    Pass global={"key": "val"} to merge extra keys into the [global] section.
    All other kwargs become top-level engine sections.

    Example:
        _make_yaml(tmp_path,
                   global={"db_path": str(tmp_path / "test.db")},
                   files={"enabled": True, "jobs": [...]})
    """
    cfg = {
        "version": 1,
        "global": {
            "backup_root": str(tmp_path / "backups"),
            "retention_days": 7,
            "compress": False,
            "log_level": "WARNING",
            "db_path": str(tmp_path / "pybackup.db"),  # always safe path
            **overrides.pop("global", {}),
        },
        **overrides,
    }
    p = tmp_path / "pybackup.yaml"
    p.write_text(yaml.dump(cfg))
    return p


def _run_cli(*args):
    from click.testing import CliRunner
    from pybackup.cli import main

    try:
        r = CliRunner(mix_stderr=False)
    except TypeError:
        r = CliRunner()
    result = r.invoke(main, list(args))
    return result.exit_code, result.output


# ════════════════════════════════════════════════════════════════════
# 1. Exceptions
# ════════════════════════════════════════════════════════════════════


class TestExceptions:
    def test_str_with_details(self):
        from pybackup.utils.exceptions import BackupError

        e = BackupError("oops", details={"key": "val"})
        assert "oops" in str(e)
        assert "key" in str(e)

    def test_str_without_details(self):
        from pybackup.utils.exceptions import ConfigError

        assert str(ConfigError("bad")) == "bad"

    def test_to_dict(self):
        from pybackup.utils.exceptions import BackupError

        d = BackupError("fail", details={"x": 1}).to_dict()
        assert d["error"] == "BackupError"
        assert d["message"] == "fail"
        assert d["details"] == {"x": 1}

    def test_hierarchy(self):
        from pybackup.utils.exceptions import (
            PyBackupError,
            ConfigError,
            BackupError,
            EngineError,
            SecurityError,
            ManifestError,
            VerificationError,
            DatabaseError,
            ServerError,
        )

        for cls in (
            ConfigError,
            BackupError,
            EngineError,
            SecurityError,
            ManifestError,
            VerificationError,
            DatabaseError,
            ServerError,
        ):
            assert issubclass(cls, PyBackupError)

    def test_details_none_by_default(self):
        from pybackup.utils.exceptions import EngineError

        assert EngineError("x").details is None

    def test_catch_as_base(self):
        from pybackup.utils.exceptions import PyBackupError, ConfigError

        with pytest.raises(PyBackupError):
            raise ConfigError("caught as base")


# ════════════════════════════════════════════════════════════════════
# 2. Security helpers
# ════════════════════════════════════════════════════════════════════


class TestSecurity:
    def test_mask_long(self):
        from pybackup.utils.security import mask_secret

        assert mask_secret("supersecret") == "*********et"

    def test_mask_short(self):
        from pybackup.utils.security import mask_secret

        assert mask_secret("ab") == "**"

    def test_mask_none(self):
        from pybackup.utils.security import mask_secret

        assert mask_secret(None) == "******"

    def test_mask_show_last(self):
        from pybackup.utils.security import mask_secret

        assert mask_secret("password", show_last=4) == "****word"

    def test_get_secret_plain(self):
        from pybackup.utils.security import get_secret

        assert get_secret("mypassword") == "mypassword"

    def test_get_secret_env_var(self, monkeypatch):
        from pybackup.utils.security import get_secret

        monkeypatch.setenv("_PB_TEST_SECRET", "resolved")
        assert get_secret("_PB_TEST_SECRET") == "resolved"

    def test_get_secret_dollar_syntax(self, monkeypatch):
        from pybackup.utils.security import get_secret

        monkeypatch.setenv("_PB_PW", "pw123")
        assert get_secret("${_PB_PW}") == "pw123"

    def test_get_secret_required_missing(self):
        from pybackup.utils.security import get_secret
        from pybackup.utils.exceptions import SecurityError

        with pytest.raises(SecurityError):
            get_secret(None, required=True, name="DB_PASS")

    def test_get_secret_optional_none(self):
        from pybackup.utils.security import get_secret

        assert get_secret(None) is None


# ════════════════════════════════════════════════════════════════════
# 3. Config loader
# ════════════════════════════════════════════════════════════════════


class TestConfig:
    def test_valid(self, tmp_path):
        from pybackup.config.loader import load_config

        p = _make_yaml(tmp_path)
        cfg = load_config(str(p))
        assert cfg["version"] == 1
        assert "backup_root" in cfg["global"]

    def test_missing_file(self, tmp_path):
        from pybackup.config.loader import load_config
        from pybackup.utils.exceptions import ConfigError

        with pytest.raises(ConfigError, match="not found"):
            load_config(str(tmp_path / "ghost.yaml"))

    def test_missing_version(self, tmp_path):
        from pybackup.config.loader import load_config
        from pybackup.utils.exceptions import ConfigError

        p = tmp_path / "c.yaml"
        p.write_text(yaml.dump({"global": {"backup_root": str(tmp_path)}}))
        with pytest.raises(ConfigError, match="version"):
            load_config(str(p))

    def test_missing_global(self, tmp_path):
        from pybackup.config.loader import load_config
        from pybackup.utils.exceptions import ConfigError

        p = tmp_path / "c.yaml"
        p.write_text(yaml.dump({"version": 1}))
        with pytest.raises(ConfigError, match="global"):
            load_config(str(p))

    def test_missing_backup_root(self, tmp_path):
        from pybackup.config.loader import load_config
        from pybackup.utils.exceptions import ConfigError

        p = tmp_path / "c.yaml"
        p.write_text(yaml.dump({"version": 1, "global": {"retention_days": 7}}))
        with pytest.raises(ConfigError, match="backup_root"):
            load_config(str(p))

    def test_retention_must_be_int(self, tmp_path):
        from pybackup.config.loader import load_config
        from pybackup.utils.exceptions import ConfigError

        p = tmp_path / "c.yaml"
        p.write_text(
            yaml.dump(
                {"version": 1, "global": {"backup_root": str(tmp_path), "retention_days": "seven"}}
            )
        )
        with pytest.raises(ConfigError, match="integer"):
            load_config(str(p))

    def test_env_expansion(self, tmp_path, monkeypatch):
        from pybackup.config.loader import load_config

        monkeypatch.setenv("_PB_ROOT", "/env/backups")
        p = tmp_path / "c.yaml"
        p.write_text("version: 1\nglobal:\n  backup_root: ${_PB_ROOT}\n")
        cfg = load_config(str(p))
        assert cfg["global"]["backup_root"] == "/env/backups"

    def test_invalid_yaml(self, tmp_path):
        from pybackup.config.loader import load_config
        from pybackup.utils.exceptions import ConfigError

        p = tmp_path / "c.yaml"
        p.write_text("version: 1\nglobal: {unclosed: [")
        with pytest.raises(ConfigError):
            load_config(str(p))

    def test_jobs_must_be_list(self, tmp_path):
        from pybackup.config.loader import load_config
        from pybackup.utils.exceptions import ConfigError

        p = tmp_path / "c.yaml"
        p.write_text(
            yaml.dump(
                {
                    "version": 1,
                    "global": {"backup_root": str(tmp_path)},
                    "files": {"enabled": True, "jobs": "not-a-list"},
                }
            )
        )
        with pytest.raises(ConfigError, match="list"):
            load_config(str(p))


# ════════════════════════════════════════════════════════════════════
# 4. Backup verifier
# ════════════════════════════════════════════════════════════════════


class TestVerifier:
    def test_generate_and_verify(self, tmp_path):
        from pybackup.engine.verify import BackupVerifier

        f = tmp_path / "b.dump"
        f.write_bytes(b"backup data " * 1000)
        v = BackupVerifier("sha256")
        cs = v.generate_checksum(str(f))
        assert v.verify_file(str(f), cs)

    def test_mismatch_raises(self, tmp_path):
        from pybackup.engine.verify import BackupVerifier
        from pybackup.utils.exceptions import VerificationError

        f = tmp_path / "b.dump"
        f.write_bytes(b"data")
        with pytest.raises(VerificationError, match="mismatch"):
            BackupVerifier("sha256").verify_file(str(f), "dead" * 16)

    def test_missing_file_raises(self):
        from pybackup.engine.verify import BackupVerifier
        from pybackup.utils.exceptions import VerificationError

        with pytest.raises(VerificationError, match="not found"):
            BackupVerifier("sha256").verify_file("/ghost/file.dump", "abc")

    def test_unsupported_algo_raises(self):
        from pybackup.engine.verify import BackupVerifier
        from pybackup.utils.exceptions import VerificationError

        with pytest.raises(VerificationError, match="Unsupported"):
            BackupVerifier("fake_algo_xyz")

    def test_sha512(self, tmp_path):
        from pybackup.engine.verify import BackupVerifier

        f = tmp_path / "f.bin"
        f.write_bytes(os.urandom(1024))
        v = BackupVerifier("sha512")
        cs = v.generate_checksum(str(f))
        assert len(cs) == 128
        assert v.verify_file(str(f), cs)

    def test_write_sidecar(self, tmp_path):
        from pybackup.engine.verify import BackupVerifier

        f = tmp_path / "backup.tar.gz"
        f.write_bytes(b"archive")
        sidecar = BackupVerifier("sha256").write_checksum_file(str(f))
        assert sidecar.exists()
        assert "backup.tar.gz" in sidecar.read_text()


# ════════════════════════════════════════════════════════════════════
# 5. Backup manifest
# ════════════════════════════════════════════════════════════════════


class TestManifest:
    def test_create_and_load(self, tmp_path):
        from pybackup.engine.manifest import BackupManifest

        m = BackupManifest(str(tmp_path))
        files = [{"path": "/backups/db.dump", "size": 1024}]
        p = m.create("postgres", "prod", files, extra={"host": "localhost"})
        assert p.exists()
        data = m.load(str(p))
        assert data["engine"] == "postgres"
        assert data["file_count"] == 1
        assert data["extra"]["host"] == "localhost"

    def test_unsupported_format(self, tmp_path):
        from pybackup.engine.manifest import BackupManifest
        from pybackup.utils.exceptions import ManifestError

        with pytest.raises(ManifestError, match="Unsupported"):
            BackupManifest(str(tmp_path), fmt="xml")

    def test_load_missing(self, tmp_path):
        from pybackup.engine.manifest import BackupManifest
        from pybackup.utils.exceptions import ManifestError

        m = BackupManifest(str(tmp_path))
        with pytest.raises(ManifestError, match="not found"):
            m.load(str(tmp_path / "ghost.json"))

    def test_empty_files_list(self, tmp_path):
        from pybackup.engine.manifest import BackupManifest

        m = BackupManifest(str(tmp_path))
        p = m.create("files", "job", [])
        data = m.load(str(p))
        assert data["file_count"] == 0
        assert data["files"] == []


# ════════════════════════════════════════════════════════════════════
# 6. Files engine
# ════════════════════════════════════════════════════════════════════


def _files_eng(src, out, compress=False, exclude=None, tmp=None):
    from pybackup.engine.files import FilesBackupEngine

    return FilesBackupEngine(
        "j",
        {"source": str(src), "output": str(out), "compress": compress, "exclude": exclude or []},
        {"backup_root": str(tmp or out), "compress": compress},
    )


class TestFilesEngine:
    def test_copy_files_and_dirs(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "a.txt").write_text("hello")
        (s / "sub").mkdir()
        (s / "sub" / "b.txt").write_text("world")
        r = _files_eng(s, tmp_path / "o").run()
        assert (r / "a.txt").read_text() == "hello"
        assert (r / "sub" / "b.txt").exists()

    def test_content_preserved(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        content = "line1\nñéü\n"
        (s / "f.txt").write_text(content, encoding="utf-8")
        r = _files_eng(s, tmp_path / "o").run()
        assert (r / "f.txt").read_text(encoding="utf-8") == content

    def test_empty_source(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        r = _files_eng(s, tmp_path / "o").run()
        assert r.exists()
        assert list(r.iterdir()) == []

    def test_exclude_by_extension(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "app.py").write_text("x")
        (s / "x.log").write_text("l")
        r = _files_eng(s, tmp_path / "o", exclude=["*.log"]).run()
        assert (r / "app.py").exists()
        assert not (r / "x.log").exists()

    def test_exclude_multiple_patterns(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "keep.py").write_text("x")
        (s / "drop.log").write_text("l")
        (s / "drop.tmp").write_text("t")
        r = _files_eng(s, tmp_path / "o", exclude=["*.log", "*.tmp"]).run()
        assert (r / "keep.py").exists()
        assert not (r / "drop.log").exists()
        assert not (r / "drop.tmp").exists()

    def test_exclude_directory(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "app.py").write_text("x")
        (s / "__pycache__").mkdir()
        (s / "__pycache__" / "x.pyc").write_bytes(b"\x00")
        r = _files_eng(s, tmp_path / "o", exclude=["__pycache__"]).run()
        assert (r / "app.py").exists()
        assert not (r / "__pycache__").exists()

    def test_exclude_nested(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "a" / "b").mkdir(parents=True)
        (s / "a" / "b" / "deep.log").write_text("l")
        (s / "a" / "keep.txt").write_text("k")
        r = _files_eng(s, tmp_path / "o", exclude=["*.log"]).run()
        assert (r / "a" / "keep.txt").exists()
        assert not (r / "a" / "b" / "deep.log").exists()

    def test_compress_produces_tar_gz(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "d.txt").write_text("data")
        r = _files_eng(s, tmp_path / "o", compress=True).run()
        assert r.suffix == ".gz"
        assert tarfile.is_tarfile(str(r))

    def test_compress_valid_archive(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "readme.txt").write_text("content")
        (s / "sub").mkdir()
        (s / "sub" / "b.txt").write_text("sub")
        r = _files_eng(s, tmp_path / "o", compress=True).run()
        with tarfile.open(str(r), "r:gz") as tf:
            names = tf.getnames()
        assert any("readme.txt" in n for n in names)

    def test_compress_respects_excludes(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "keep.py").write_text("code")
        (s / "drop.log").write_text("log")
        r = _files_eng(s, tmp_path / "o", compress=True, exclude=["*.log"]).run()
        with tarfile.open(str(r), "r:gz") as tf:
            names = tf.getnames()
        assert any("keep.py" in n for n in names)
        assert not any("drop.log" in n for n in names)

    def test_unicode_filenames(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "données.txt").write_text("fr")
        (s / "日本語.txt").write_text("jp")
        r = _files_eng(s, tmp_path / "o").run()
        assert len(list(r.iterdir())) == 2

    def test_binary_files(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        data = bytes(range(256)) * 100
        (s / "bin.bin").write_bytes(data)
        r = _files_eng(s, tmp_path / "o").run()
        assert (r / "bin.bin").read_bytes() == data

    def test_execute_returns_success_dict(self, tmp_path):
        s = tmp_path / "s"
        s.mkdir()
        (s / "f.txt").write_text("x")
        result = _files_eng(s, tmp_path / "o").execute()
        assert result["status"] == "success"
        assert result["output_path"] is not None
        assert "started_at" in result and "finished_at" in result

    def test_missing_source_key(self, tmp_path):
        from pybackup.engine.files import FilesBackupEngine
        from pybackup.utils.exceptions import BackupError

        with pytest.raises(BackupError, match="source"):
            FilesBackupEngine("j", {}, {"backup_root": str(tmp_path)}).run()

    def test_nonexistent_source(self, tmp_path):
        from pybackup.utils.exceptions import BackupError

        with pytest.raises(BackupError, match="does not exist"):
            _files_eng(tmp_path / "ghost", tmp_path / "o").run()

    def test_error_has_details(self, tmp_path):
        from pybackup.utils.exceptions import BackupError

        try:
            _files_eng(tmp_path / "ghost", tmp_path / "o").run()
        except BackupError as exc:
            assert exc.details is not None


# ════════════════════════════════════════════════════════════════════
# 7. MongoDB engine
# ════════════════════════════════════════════════════════════════════


def _mongo_eng(tmp_path, **kw):
    from pybackup.engine.mongo import MongoBackupEngine

    d = dict(
        host="localhost",
        port=27017,
        username="admin",
        password="secret",
        auth_db="admin",
        output=str(tmp_path / "out"),
    )
    d.update(kw)
    return MongoBackupEngine("mj", d, _global(tmp_path))


class TestMongoEngine:
    @patch("subprocess.run")
    def test_success_returns_path(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        r = _mongo_eng(tmp_path).run()
        assert isinstance(r, Path)
        assert mr.call_count == 1

    @patch("subprocess.run")
    def test_host_in_command(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mongo_eng(tmp_path, host="db.x").run()
        cmd = mr.call_args[0][0]
        assert "--host" in cmd and "db.x" in cmd

    @patch("subprocess.run")
    def test_port_in_command(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mongo_eng(tmp_path, port=27018).run()
        cmd = mr.call_args[0][0]
        assert "--port" in cmd and "27018" in cmd

    @patch("subprocess.run")
    def test_auth_flags_when_credentials(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mongo_eng(tmp_path, username="admin", password="pass").run()
        cmd = mr.call_args[0][0]
        assert "--username" in cmd and "--password" in cmd
        assert "--authenticationDatabase" in cmd

    @patch("subprocess.run")
    def test_no_auth_flags_without_credentials(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mongo_eng(tmp_path, username=None, password=None).run()
        cmd = mr.call_args[0][0]
        assert "--username" not in cmd and "--password" not in cmd

    @patch("subprocess.run")
    def test_specific_db_flag(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mongo_eng(tmp_path, database="myapp").run()
        cmd = mr.call_args[0][0]
        assert "--db" in cmd and "myapp" in cmd

    @patch("subprocess.run")
    def test_all_dbs_no_db_flag(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mongo_eng(tmp_path, database=None).run()
        assert "--db" not in mr.call_args[0][0]

    @patch("subprocess.run")
    def test_timeout_set(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mongo_eng(tmp_path).run()
        assert mr.call_args[1].get("timeout", 0) >= 60

    @patch("subprocess.run")
    def test_check_true(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mongo_eng(tmp_path).run()
        assert mr.call_args[1].get("check") is True

    @patch("subprocess.run")
    def test_password_from_env(self, mr, tmp_path, monkeypatch):
        monkeypatch.setenv("_PB_MONGO_PW", "envpw")
        mr.return_value = _proc_ok()
        _mongo_eng(tmp_path, password="${_PB_MONGO_PW}").run()
        assert "envpw" in mr.call_args[0][0]

    @patch("subprocess.run")
    def test_execute_success_dict(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        r = _mongo_eng(tmp_path).execute()
        assert r["status"] == "success"

    @patch("subprocess.run")
    def test_nonzero_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = subprocess.CalledProcessError(1, "mongodump", stderr="auth failed")
        with pytest.raises(BackupError):
            _mongo_eng(tmp_path).run()

    @patch("subprocess.run")
    def test_stderr_in_details(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = subprocess.CalledProcessError(1, "mongodump", stderr="SASL fail")
        try:
            _mongo_eng(tmp_path).run()
        except BackupError as exc:
            assert "SASL fail" in str(exc.details)

    @patch("subprocess.run")
    def test_timeout_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = subprocess.TimeoutExpired("mongodump", 3600)
        with pytest.raises(BackupError, match="timed out"):
            _mongo_eng(tmp_path).run()

    @patch("subprocess.run")
    def test_not_found_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = FileNotFoundError()
        with pytest.raises(BackupError, match="not found"):
            _mongo_eng(tmp_path).run()


# ════════════════════════════════════════════════════════════════════
# 8. PostgreSQL engine
# ════════════════════════════════════════════════════════════════════


def _pg_eng(tmp_path, **kw):
    from pybackup.engine.postgres import PostgresBackupEngine

    d = dict(
        host="localhost",
        port=5432,
        database="testdb",
        username="bkp",
        password="pgpw",
        format="custom",
        compress=False,
        output=str(tmp_path / "out"),
    )
    d.update(kw)
    return PostgresBackupEngine("pg", d, _global(tmp_path))


class TestPostgresEngine:
    @patch("subprocess.run")
    def test_custom_format_dump_ext(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        r = _pg_eng(tmp_path, format="custom").run()
        assert r.suffix == ".dump"
        assert "c" in mr.call_args[0][0]

    @patch("subprocess.run")
    def test_plain_format_sql_ext(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        assert _pg_eng(tmp_path, format="plain").run().suffix == ".sql"

    @patch("subprocess.run")
    def test_directory_format_dir_ext(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        assert _pg_eng(tmp_path, format="directory").run().suffix == ".dir"

    @patch("subprocess.run")
    def test_directory_uses_dash_f(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _pg_eng(tmp_path, format="directory").run()
        assert "-f" in mr.call_args[0][0]

    @patch("subprocess.run")
    def test_pgpassword_in_env_not_cmd(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _pg_eng(tmp_path, password="topsecret").run()
        cmd = mr.call_args[0][0]
        env = mr.call_args[1].get("env", {})
        assert "topsecret" not in " ".join(str(c) for c in cmd)
        assert env.get("PGPASSWORD") == "topsecret"

    @patch("subprocess.run")
    def test_no_pgpassword_when_none(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _pg_eng(tmp_path, password=None).run()
        assert "PGPASSWORD" not in mr.call_args[1].get("env", {})

    @patch("subprocess.run")
    def test_env_inherits_os(self, mr, tmp_path, monkeypatch):
        monkeypatch.setenv("_PB_CUSTOM", "yes")
        mr.return_value = _proc_ok()
        _pg_eng(tmp_path).run()
        assert mr.call_args[1].get("env", {}).get("_PB_CUSTOM") == "yes"

    @patch("subprocess.run")
    def test_plain_compress_calls_gzip(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _pg_eng(tmp_path, format="plain", compress=True).run()
        assert mr.call_count == 2
        assert "gzip" in mr.call_args_list[1][0][0]

    @patch("subprocess.run")
    def test_custom_compress_no_gzip(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _pg_eng(tmp_path, format="custom", compress=True).run()
        assert mr.call_count == 1

    @patch("subprocess.run")
    def test_gzip_failure_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        def side(cmd, **kw):
            if "gzip" in cmd:
                raise subprocess.CalledProcessError(1, "gzip", stderr=b"no space")
            return _proc_ok()

        mr.side_effect = side
        with pytest.raises(BackupError, match="compression"):
            _pg_eng(tmp_path, format="plain", compress=True).run()

    def test_missing_database_raises(self, tmp_path):
        from pybackup.utils.exceptions import BackupError

        with pytest.raises(BackupError, match="database"):
            _pg_eng(tmp_path, database="")

    def test_missing_username_raises(self, tmp_path):
        from pybackup.utils.exceptions import BackupError

        with pytest.raises(BackupError, match="username"):
            _pg_eng(tmp_path, username="")

    def test_bad_format_raises(self, tmp_path):
        from pybackup.utils.exceptions import BackupError

        with pytest.raises(BackupError, match="Unsupported"):
            _pg_eng(tmp_path, format="xml")

    @patch("subprocess.run")
    def test_timeout_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = subprocess.TimeoutExpired("pg_dump", 3600)
        with pytest.raises(BackupError, match="timed out"):
            _pg_eng(tmp_path).run()

    @patch("subprocess.run")
    def test_not_found_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = FileNotFoundError()
        with pytest.raises(BackupError, match="not found"):
            _pg_eng(tmp_path).run()

    @patch("subprocess.run")
    def test_execute_success(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        r = _pg_eng(tmp_path).execute()
        assert r["status"] == "success"
        assert r["engine"] == "PostgresBackupEngine"


# ════════════════════════════════════════════════════════════════════
# 9. MySQL engine
# ════════════════════════════════════════════════════════════════════


def _mysql_eng(tmp_path, **kw):
    from pybackup.engine.mysql import MySQLBackupEngine

    d = dict(
        host="localhost",
        port=3306,
        database="appdb",
        username="bkp",
        password="mypw",
        single_transaction=True,
        compress=False,
        output=str(tmp_path / "out"),
    )
    d.update(kw)
    return MySQLBackupEngine("my", d, _global(tmp_path))


class TestMySQLEngine:
    @patch("subprocess.run")
    def test_success_returns_path(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        r = _mysql_eng(tmp_path).run()
        assert isinstance(r, Path)

    @patch("subprocess.run")
    def test_host_in_command(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mysql_eng(tmp_path, host="mysql.internal").run()
        cmd = mr.call_args[0][0]
        assert "-h" in cmd and "mysql.internal" in cmd

    @patch("subprocess.run")
    def test_database_in_command(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mysql_eng(tmp_path, database="orders").run()
        assert "orders" in mr.call_args[0][0]

    @patch("subprocess.run")
    def test_single_transaction_flag(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mysql_eng(tmp_path, single_transaction=True).run()
        assert "--single-transaction" in mr.call_args[0][0]

    @patch("subprocess.run")
    def test_no_single_transaction(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mysql_eng(tmp_path, single_transaction=False).run()
        assert "--single-transaction" not in mr.call_args[0][0]

    @patch("subprocess.run")
    def test_compress_calls_gzip(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mysql_eng(tmp_path, compress=True).run()
        assert mr.call_count == 2
        assert "gzip" in mr.call_args_list[1][0][0]

    @patch("subprocess.run")
    def test_no_compress_single_call(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mysql_eng(tmp_path, compress=False).run()
        assert mr.call_count == 1

    def test_missing_database_raises(self, tmp_path):
        from pybackup.utils.exceptions import BackupError

        with pytest.raises(BackupError, match="database"):
            _mysql_eng(tmp_path, database="")

    def test_missing_username_raises(self, tmp_path):
        from pybackup.utils.exceptions import BackupError

        with pytest.raises(BackupError, match="username"):
            _mysql_eng(tmp_path, username="")

    @patch("subprocess.run")
    def test_timeout_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = subprocess.TimeoutExpired("mysqldump", 3600)
        with pytest.raises(BackupError, match="timed out"):
            _mysql_eng(tmp_path).run()

    @patch("subprocess.run")
    def test_not_found_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = FileNotFoundError()
        with pytest.raises(BackupError, match="not found"):
            _mysql_eng(tmp_path).run()

    @patch("subprocess.run")
    def test_nonzero_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = subprocess.CalledProcessError(1, "mysqldump", stderr="access denied")
        with pytest.raises(BackupError):
            _mysql_eng(tmp_path).run()


# ════════════════════════════════════════════════════════════════════
# 10. MSSQL engine
# ════════════════════════════════════════════════════════════════════


def _mssql_eng(tmp_path, **kw):
    from pybackup.engine.mssql import MSSQLBackupEngine

    d = dict(
        host="localhost",
        port=1433,
        database="AppDB",
        username="sa",
        password="mssqlpw",
        encrypt=False,
        output=str(tmp_path / "out"),
    )
    d.update(kw)
    return MSSQLBackupEngine("ms", d, _global(tmp_path))


class TestMSSQLEngine:
    @patch("subprocess.run")
    def test_success_returns_path(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        r = _mssql_eng(tmp_path).run()
        assert isinstance(r, Path)
        assert r.suffix == ".bak"

    @patch("subprocess.run")
    def test_sqlcmd_called(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mssql_eng(tmp_path).run()
        assert "sqlcmd" in mr.call_args[0][0]

    @patch("subprocess.run")
    def test_database_in_sql(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mssql_eng(tmp_path, database="ProdDB").run()
        cmd = mr.call_args[0][0]
        assert "ProdDB" in " ".join(cmd)

    @patch("subprocess.run")
    def test_host_port_combined(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mssql_eng(tmp_path, host="sql.server", port=1434).run()
        cmd = mr.call_args[0][0]
        assert "sql.server,1434" in cmd

    @patch("subprocess.run")
    def test_encrypt_flag(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mssql_eng(tmp_path, encrypt=True).run()
        assert "-N" in mr.call_args[0][0]

    @patch("subprocess.run")
    def test_no_encrypt_flag(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        _mssql_eng(tmp_path, encrypt=False).run()
        assert "-N" not in mr.call_args[0][0]

    def test_missing_database_raises(self, tmp_path):
        from pybackup.utils.exceptions import BackupError

        with pytest.raises(BackupError, match="database"):
            _mssql_eng(tmp_path, database="")

    def test_missing_username_raises(self, tmp_path):
        from pybackup.utils.exceptions import BackupError

        with pytest.raises(BackupError, match="username"):
            _mssql_eng(tmp_path, username="")

    @patch("subprocess.run")
    def test_timeout_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = subprocess.TimeoutExpired("sqlcmd", 7200)
        with pytest.raises(BackupError, match="timed out"):
            _mssql_eng(tmp_path).run()

    @patch("subprocess.run")
    def test_not_found_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = FileNotFoundError()
        with pytest.raises(BackupError, match="not found"):
            _mssql_eng(tmp_path).run()

    @patch("subprocess.run")
    def test_nonzero_raises(self, mr, tmp_path):
        from pybackup.utils.exceptions import BackupError

        mr.side_effect = subprocess.CalledProcessError(1, "sqlcmd", stderr="Login failed")
        with pytest.raises(BackupError):
            _mssql_eng(tmp_path).run()

    @patch("subprocess.run")
    def test_execute_success(self, mr, tmp_path):
        mr.return_value = _proc_ok()
        r = _mssql_eng(tmp_path).execute()
        assert r["status"] == "success"


# ════════════════════════════════════════════════════════════════════
# 11. Database (SQLite)
# ════════════════════════════════════════════════════════════════════


@pytest.fixture
def db():
    from pybackup.db.database import Database

    return Database(":memory:")


class TestDatabase:
    def test_create_and_finish_run(self, db):
        rid = db.create_run("myjob", "postgres")
        db.finish_run(rid, status="success", output_path="/b/db.dump")
        run = db.get_run(rid)
        assert run["status"] == "success"
        assert run["output_path"] == "/b/db.dump"
        assert run["job_name"] == "myjob"

    def test_list_runs_pagination(self, db):
        for i in range(10):
            rid = db.create_run(f"job-{i}", "files")
            db.finish_run(rid, status="success")
        p1 = db.list_runs(limit=4, offset=0)
        p2 = db.list_runs(limit=4, offset=4)
        assert len(p1) == 4 and len(p2) == 4
        assert p1[0]["id"] != p2[0]["id"]

    def test_filter_by_status(self, db):
        for s in ["success", "failed", "success", "crashed"]:
            rid = db.create_run("j", "files")
            db.finish_run(rid, status=s)
        successes = db.list_runs(status="success")
        assert all(r["status"] == "success" for r in successes)
        assert len(successes) == 2

    def test_filter_by_job(self, db):
        for name in ["alpha", "beta", "alpha"]:
            rid = db.create_run(name, "files")
            db.finish_run(rid, status="success")
        alphas = db.list_runs(job_name="alpha")
        assert all(r["job_name"] == "alpha" for r in alphas)
        assert len(alphas) == 2

    def test_delete_run(self, db):
        rid = db.create_run("j", "postgres")
        db.finish_run(rid, status="success")
        assert db.delete_run(rid)
        assert db.get_run(rid) is None

    def test_delete_nonexistent(self, db):
        assert not db.delete_run(9999)

    def test_add_and_list_files(self, db):
        rid = db.create_run("j", "postgres")
        db.add_file(rid, "/b/prod.dump", file_size=2048, checksum="abc")
        files = db.list_files(rid)
        assert len(files) == 1
        assert files[0]["file_size"] == 2048

    def test_count_runs(self, db):
        for _ in range(5):
            rid = db.create_run("j", "files")
            db.finish_run(rid, status="success")
        assert db.count_runs() == 5
        assert db.count_runs(status="failed") == 0

    def test_settings_upsert(self, db):
        db.set_setting("theme", "dark")
        assert db.get_setting("theme") == "dark"
        db.set_setting("theme", "light")
        assert db.get_setting("theme") == "light"

    def test_settings_default(self, db):
        assert db.get_setting("nope", "fallback") == "fallback"

    def test_stats(self, db):
        data = [
            ("success", "files"),
            ("failed", "postgres"),
            ("success", "postgres"),
            ("crashed", "mysql"),
            ("success", "mongo"),
        ]
        for s, e in data:
            rid = db.create_run("j", e)
            db.finish_run(rid, status=s)
        stats = db.stats()
        assert stats["total"] == 5
        assert stats["success"] == 3
        assert stats["failed"] == 2
        assert 0 <= stats["success_rate"] <= 100
        assert isinstance(stats["by_engine"], list)
        assert isinstance(stats["daily"], list)
        assert isinstance(stats["recent"], list)

    def test_running_status_tracked(self, db):
        db.create_run("j", "files")  # left as 'running'
        assert db.stats()["running"] == 1

    def test_file_db_created(self, tmp_path):
        from pybackup.db.database import Database

        db_path = tmp_path / "test.db"
        db2 = Database(str(db_path))
        rid = db2.create_run("j", "files")
        db2.finish_run(rid, status="success")
        assert db_path.exists()
        assert db2.count_runs() == 1


# ════════════════════════════════════════════════════════════════════
# 12. HTTP router
# ════════════════════════════════════════════════════════════════════


class TestRouter:
    def _router(self):
        from pybackup.server.httpserver import Router

        return Router()

    def test_exact_match(self):
        r = self._router()

        def fn(req, db):
            return (200, {}, b"ok")

        r.add("GET", "/api/stats", fn)
        matched, params = r.match("GET", "/api/stats")
        assert matched is fn and params == {}

    def test_param_match(self):
        r = self._router()

        def fn(req, db):
            return (200, {}, b"ok")

        r.add("GET", "/api/runs/:id", fn)
        matched, params = r.match("GET", "/api/runs/42")
        assert matched is fn and params["id"] == "42"

    def test_delete_with_param(self):
        r = self._router()

        def fn(req, db):
            return (200, {}, b"del")

        r.add("DELETE", "/api/runs/:id", fn)
        matched, params = r.match("DELETE", "/api/runs/7")
        assert matched is fn and params["id"] == "7"

    def test_no_match_returns_none(self):
        r = self._router()
        fn, _ = r.match("GET", "/api/nothing")
        assert fn is None

    def test_method_mismatch(self):
        r = self._router()
        r.add("GET", "/api/runs", lambda r, d: None)
        fn, _ = r.match("POST", "/api/runs")
        assert fn is None

    def test_exact_and_param_same_prefix(self):
        r = self._router()

        def fn_list(req, db):
            return (200, {}, b"list")

        def fn_detail(req, db):
            return (200, {}, b"detail")

        r.add("GET", "/api/runs", fn_list)
        r.add("GET", "/api/runs/:id", fn_detail)
        matched_list, _ = r.match("GET", "/api/runs")
        matched_detail, p = r.match("GET", "/api/runs/5")
        assert matched_list is fn_list
        assert matched_detail is fn_detail
        assert p["id"] == "5"

    def test_param_is_string(self):
        r = self._router()
        r.add("GET", "/api/runs/:id", lambda req, db: None)
        _, params = r.match("GET", "/api/runs/123")
        assert isinstance(params["id"], str)


# ════════════════════════════════════════════════════════════════════
# 13. REST API handlers (integration)
# ════════════════════════════════════════════════════════════════════


@pytest.fixture
def router_and_db():
    from pybackup.db.database import Database
    from pybackup.server.httpserver import Router, PyBackupHandler
    from pybackup.server.handlers import register_routes
    from pybackup.auth import UserDB, sessions
    import os
    import tempfile

    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "test.db")
    db = Database(db_path)
    udb = UserDB(db_path)
    uid = udb.create_user("testadmin", "Admin1234!", role="admin")
    token = sessions.create(uid, "testadmin", "admin")
    router = Router()
    register_routes(router)
    PyBackupHandler.user_db = udb
    return router, db, token


def _call(router_db_tok, method, path, body=b"", token=None):
    from pybackup.server.httpserver import Request

    if isinstance(router_db_tok, tuple) and len(router_db_tok) == 3:
        router, db, auto_tok = router_db_tok
        if token is None:
            token = auto_tok
    else:
        router, db = router_db_tok
    fn, params = router.match(method, path)
    assert fn is not None, f"No route: {method} {path}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = Request(
        method, path, {}, type("H", (), {"get": lambda self, k, d="": headers.get(k, d)})(), body
    )
    r.path_params = params
    status, _, resp = fn(r, db)
    return status, json.loads(resp)


class TestAPIHandlers:
    """API handler tests — all requests use auth token from router_and_db fixture."""

    def test_stats_empty(self, router_and_db):
        router, db, tok = router_and_db
        s, d = _call(router_and_db, "GET", "/api/stats")
        assert s == 200 and d["total"] == 0

    def test_create_run(self, router_and_db):
        router, db, tok = router_and_db
        body = json.dumps(
            {"job_name": "ci", "engine": "files", "status": "success", "output_path": "/ci/bk"}
        ).encode()
        s, d = _call(router_and_db, "POST", "/api/runs", body)
        assert s == 201 and d["job_name"] == "ci"

    def test_list_runs(self, router_and_db):
        router, db, tok = router_and_db
        for i in range(3):
            rid = db.create_run(f"j{i}", "postgres")
            db.finish_run(rid, status="success")
        s, d = _call(router_and_db, "GET", "/api/runs")
        assert s == 200 and d["total"] == 3

    def test_get_run_by_id(self, router_and_db):
        router, db, tok = router_and_db
        rid = db.create_run("myjob", "mysql")
        db.finish_run(rid, status="success")
        s, d = _call(router_and_db, "GET", f"/api/runs/{rid}")
        assert s == 200 and d["job_name"] == "myjob"

    def test_get_run_404(self, router_and_db):
        s, d = _call(router_and_db, "GET", "/api/runs/9999")
        assert s == 404 and "error" in d

    def test_delete_run(self, router_and_db):
        router, db, tok = router_and_db
        rid = db.create_run("j", "postgres")
        db.finish_run(rid, status="success")
        s, d = _call(router_and_db, "DELETE", f"/api/runs/{rid}")
        assert s == 200 and d["deleted"] == rid
        assert db.get_run(rid) is None

    def test_delete_run_404(self, router_and_db):
        s, d = _call(router_and_db, "DELETE", "/api/runs/9999")
        assert s == 404

    def test_update_and_get_settings(self, router_and_db):
        body = json.dumps({"theme": "light", "retention_days": "30"}).encode()
        _call(router_and_db, "POST", "/api/settings", body)
        s, d = _call(router_and_db, "GET", "/api/settings")
        assert s == 200 and d["theme"] == "light" and d["retention_days"] == "30"

    def test_stats_accuracy(self, router_and_db):
        router, db, tok = router_and_db
        for st in ["success", "success", "failed"]:
            rid = db.create_run("j", "files")
            db.finish_run(rid, status=st)
        s, d = _call(router_and_db, "GET", "/api/stats")
        assert d["total"] == 3 and d["success"] == 2 and d["failed"] == 1
        assert d["success_rate"] == pytest.approx(66.7, abs=0.1)

    def test_run_includes_files(self, router_and_db):
        router, db, tok = router_and_db
        rid = db.create_run("j", "postgres")
        db.finish_run(rid, status="success")
        db.add_file(rid, "/b/prod.dump", file_size=1024)
        s, d = _call(router_and_db, "GET", f"/api/runs/{rid}")
        assert s == 200 and len(d["files"]) == 1
        assert d["files"][0]["file_path"] == "/b/prod.dump"

    def test_invalid_json_body(self, router_and_db):
        s, d = _call(router_and_db, "POST", "/api/runs", b"not-json")
        assert s == 400

    def test_settings_ignore_unknown_keys(self, router_and_db):
        body = json.dumps({"theme": "dark", "evil_key": "x"}).encode()
        s, d = _call(router_and_db, "POST", "/api/settings", body)
        assert s == 200 and "evil_key" not in d["updated"]


class TestCLI:
    # ── version / help ────────────────────────────────────────────

    def test_version(self):
        code, out = _run_cli("--version")
        assert code == 0 and ("1.0.0" in out or "2.0.0" in out)

    def test_help_lists_commands(self):
        code, out = _run_cli("--help")
        assert code == 0
        for cmd in ("run", "serve", "verify", "checksum", "config-check"):
            assert cmd in out

    def test_run_help(self):
        code, out = _run_cli("run", "--help")
        assert code == 0 and "--config" in out and "--dry-run" in out

    def test_serve_help(self):
        code, out = _run_cli("serve", "--help")
        assert code == 0 and "--port" in out and "--host" in out

    # ── config-check ──────────────────────────────────────────────

    def test_config_check_valid(self, tmp_path):
        code, out = _run_cli("config-check", "--config", str(_make_yaml(tmp_path)))
        assert code == 0 and "valid" in out.lower()

    def test_config_check_missing_file(self, tmp_path):
        code, _ = _run_cli("config-check", "--config", str(tmp_path / "ghost.yaml"))
        assert code != 0

    def test_config_check_invalid_yaml(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text("version: 1\nglobal: {unclosed: [")
        code, _ = _run_cli("config-check", "--config", str(p))
        assert code != 0

    def test_config_check_missing_version(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(yaml.dump({"global": {"backup_root": str(tmp_path)}}))
        code, _ = _run_cli("config-check", "--config", str(p))
        assert code != 0

    def test_config_check_shows_engines(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        p = _make_yaml(
            tmp_path,
            files={
                "enabled": True,
                "jobs": [{"name": "docs", "source": str(src)}],
            },
        )
        code, out = _run_cli("config-check", "--config", str(p))
        assert code == 0 and "files" in out and "docs" in out

    # ── run --dry-run ─────────────────────────────────────────────

    def test_dry_run_no_backup(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        (src / "f.txt").write_text("x")
        p = _make_yaml(
            tmp_path,
            files={
                "enabled": True,
                "jobs": [{"name": "t", "source": str(src), "output": str(tmp_path / "out")}],
            },
        )
        code, out = _run_cli("run", "--config", str(p), "--dry-run")
        assert code == 0
        assert not (tmp_path / "out").exists()

    def test_dry_run_shows_job_names(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        p = _make_yaml(
            tmp_path,
            files={
                "enabled": True,
                "jobs": [{"name": "my-files", "source": str(src)}],
            },
        )
        code, out = _run_cli("run", "--config", str(p), "--dry-run")
        assert code == 0 and "my-files" in out

    # ── run (real) ────────────────────────────────────────────────

    def test_run_files_engine(self, tmp_path):
        """
        Real file backup run.
        db_path is explicitly set inside the config so the CLI never tries
        to create /var/lib/pybackup/ (which requires root in CI).
        """
        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_text("hello")
        (src / "b.txt").write_text("world")
        p = _make_yaml(
            tmp_path,
            # ↓ the one fix: route the DB to a writable temp path
            **{"global": {"db_path": str(tmp_path / "ci.db")}},
            files={
                "enabled": True,
                "jobs": [
                    {
                        "name": "bk",
                        "source": str(src),
                        "output": str(tmp_path / "out"),
                        "compress": False,
                    }
                ],
            },
        )
        code, out = _run_cli("run", "--config", str(p))
        assert code == 0

    def test_run_nonexistent_source_exits_1(self, tmp_path):
        p = _make_yaml(
            tmp_path,
            files={
                "enabled": True,
                "jobs": [
                    {
                        "name": "bad",
                        "source": str(tmp_path / "ghost"),
                        "output": str(tmp_path / "out"),
                    }
                ],
            },
        )
        code, out = _run_cli("run", "--config", str(p))
        assert code == 1

    # ── checksum ──────────────────────────────────────────────────

    def test_checksum_sha256(self, tmp_path):
        f = tmp_path / "backup.dump"
        f.write_bytes(b"data" * 500)
        code, out = _run_cli("checksum", str(f))
        assert code == 0
        parts = out.strip().split()
        assert len(parts) == 2 and len(parts[0]) == 64

    def test_checksum_sha512(self, tmp_path):
        f = tmp_path / "backup.dump"
        f.write_bytes(b"data")
        code, out = _run_cli("checksum", str(f), "--algorithm", "sha512")
        assert code == 0 and len(out.strip().split()[0]) == 128

    def test_checksum_missing_file(self, tmp_path):
        code, _ = _run_cli("checksum", str(tmp_path / "ghost.dump"))
        assert code != 0

    # ── verify ────────────────────────────────────────────────────

    def test_verify_pass(self, tmp_path):
        from pybackup.engine.verify import BackupVerifier

        f = tmp_path / "b.dump"
        f.write_bytes(b"correct" * 200)
        cs = BackupVerifier("sha256").generate_checksum(str(f))
        code, out = _run_cli("verify", str(f), "--checksum", cs)
        assert code == 0 and "verified" in out.lower()

    def test_verify_fail(self, tmp_path):
        f = tmp_path / "b.dump"
        f.write_bytes(b"tampered")
        code, out = _run_cli("verify", str(f), "--checksum", "a" * 64)
        assert code != 0

    def test_verify_missing_file(self, tmp_path):
        code, _ = _run_cli("verify", str(tmp_path / "ghost.dump"), "--checksum", "a" * 64)
        assert code != 0

    # ── serve (live HTTP smoke test) ──────────────────────────────

    def _start_server(self, port: int):
        import os
        from pybackup.db.database import Database
        from pybackup.server.httpserver import PyBackupHandler, Router
        from pybackup.server.handlers import register_routes
        from pybackup.auth import UserDB
        from http.server import ThreadingHTTPServer

        tmp = tempfile.mkdtemp()
        db_path = os.path.join(tmp, "srv.db")
        db = Database(db_path)
        udb = UserDB(db_path)
        udb.create_user("testadmin", "Test1234!", role="admin")
        for i in range(3):
            rid = db.create_run(f"job-{i}", "files")
            db.finish_run(rid, status="success" if i < 2 else "failed")

        router = Router()
        register_routes(router)
        PyBackupHandler.router = router
        PyBackupHandler.db = db
        PyBackupHandler.user_db = udb
        httpd = ThreadingHTTPServer(("127.0.0.1", port), PyBackupHandler)
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t._httpd = httpd  # type: ignore[attr-defined]
        t.start()
        time.sleep(0.3)
        return t

    def test_serve_index_html(self):
        port = 19910
        t = self._start_server(port)
        try:
            r = urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=3)
            assert r.status == 200
            assert "html" in r.headers["content-type"]
            assert "PyBackup" in r.read().decode()
        finally:
            t._httpd.shutdown()

    def test_serve_api_stats(self):
        port = 19911
        t = self._start_server(port)
        try:
            # Login first to get a token
            login_data = json.dumps({"username": "testadmin", "password": "Test1234!"}).encode()
            lr = urllib.request.urlopen(
                urllib.request.Request(
                    f"http://127.0.0.1:{port}/api/auth/login",
                    data=login_data,
                    headers={"Content-Type": "application/json"},
                ),
                timeout=3,
            )
            tok = json.loads(lr.read())["token"]
            # Now call stats with auth
            r = urllib.request.urlopen(
                urllib.request.Request(
                    f"http://127.0.0.1:{port}/api/stats", headers={"Authorization": f"Bearer {tok}"}
                ),
                timeout=3,
            )
            assert r.status == 200
            d = json.loads(r.read())
            assert d["total"] == 3
        finally:
            t._httpd.shutdown()

    def test_serve_css(self):
        port = 19912
        t = self._start_server(port)
        try:
            r = urllib.request.urlopen(f"http://127.0.0.1:{port}/css/app.css", timeout=3)
            assert r.status == 200 and "css" in r.headers["content-type"]
        finally:
            t._httpd.shutdown()

    def test_serve_js(self):
        port = 19913
        t = self._start_server(port)
        try:
            r = urllib.request.urlopen(f"http://127.0.0.1:{port}/js/app.js", timeout=3)
            assert r.status == 200 and "javascript" in r.headers["content-type"]
        finally:
            t._httpd.shutdown()

    def test_serve_404_unknown_api(self):
        port = 19914
        t = self._start_server(port)
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/nope", timeout=3)
            assert False, "Expected 404"
        except urllib.error.HTTPError as e:
            assert e.code == 404
        finally:
            t._httpd.shutdown()

"""
pybackup.auth — password hashing, session tokens, user management.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from pybackup.utils.exceptions import SecurityError

logger = logging.getLogger(__name__)

_ITERATIONS = 600_000
_HASH_ALG = "sha256"
_SALT_BYTES = 32
_TOKEN_BYTES = 32
SESSION_TTL = 60 * 60 * 8  # 8 hours


# ── Password ─────────────────────────────────────────────────────────


def hash_password(plain: str) -> str:
    salt = os.urandom(_SALT_BYTES)
    digest = hashlib.pbkdf2_hmac(_HASH_ALG, plain.encode(), salt, _ITERATIONS)
    return f"pbkdf2${salt.hex()}${digest.hex()}"


def verify_password(plain: str, stored: str) -> bool:
    try:
        _, salt_hex, digest_hex = stored.split("$")
        expected = bytes.fromhex(digest_hex)
        actual = hashlib.pbkdf2_hmac(
            _HASH_ALG, plain.encode(), bytes.fromhex(salt_hex), _ITERATIONS
        )
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


# ── Session ───────────────────────────────────────────────────────────


@dataclass
class Session:
    token: str
    user_id: int
    username: str
    role: str
    created_at: float = field(default_factory=time.time)

    def is_expired(self) -> bool:
        return (time.time() - self.created_at) > SESSION_TTL


class SessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, Session] = {}

    def create(self, user_id: int, username: str, role: str) -> str:
        token = secrets.token_hex(_TOKEN_BYTES)
        self._sessions[token] = Session(token=token, user_id=user_id, username=username, role=role)
        return token

    def get(self, token: str) -> Session | None:
        s = self._sessions.get(token)
        if s is None:
            return None
        if s.is_expired():
            del self._sessions[token]
            return None
        return s

    def delete(self, token: str) -> None:
        self._sessions.pop(token, None)


sessions = SessionStore()


# ── User DB ───────────────────────────────────────────────────────────

_USER_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    NOT NULL UNIQUE COLLATE NOCASE,
    password_hash TEXT    NOT NULL,
    role          TEXT    NOT NULL DEFAULT 'viewer'
                          CHECK(role IN ('admin','viewer')),
    email         TEXT,
    created_at    TEXT    NOT NULL,
    last_login    TEXT
);
"""


class UserDB:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        if db_path == ":memory:":
            self._shared: sqlite3.Connection | None = sqlite3.connect(
                ":memory:", check_same_thread=False
            )
        else:
            self._shared = None
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init()

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        if self.db_path == ":memory:":
            conn = self._shared
            assert conn is not None
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            except sqlite3.Error as exc:
                conn.rollback()
                raise SecurityError("Auth DB error", details=str(exc)) from exc
            return

        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            yield conn
            conn.commit()
        except sqlite3.Error as exc:
            conn.rollback()
            raise SecurityError("Auth DB error", details=str(exc)) from exc
        finally:
            conn.close()

    def _init(self) -> None:
        with self._conn() as conn:
            conn.executescript(_USER_SCHEMA)
            # Migration: add email column to pre-existing databases
            try:
                conn.execute("ALTER TABLE users ADD COLUMN email TEXT")
            except Exception:
                pass  # column already exists

    def create_user(
        self,
        username: str,
        password: str,
        role: str = "viewer",
        email: str | None = None,
    ) -> int:
        if role not in ("admin", "viewer"):
            raise SecurityError(f"Invalid role: {role!r}")
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            with self._conn() as conn:
                cur = conn.execute(
                    "INSERT INTO users (username,password_hash,role,email,created_at)"
                    " VALUES(?,?,?,?,?)",
                    (username, hash_password(password), role, email, now),
                )
                logger.info("User created: %s (role=%s)", username, role)
                return cur.lastrowid  # type: ignore[return-value]
        except sqlite3.IntegrityError as exc:
            raise SecurityError(f"Username already exists: {username!r}") from exc

    def get_by_username(self, username: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE username=? COLLATE NOCASE", (username,)
            ).fetchone()
        return dict(row) if row else None

    def get_by_id(self, user_id: int) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        return dict(row) if row else None

    def list_users(self) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id,username,role,email,created_at,last_login" " FROM users ORDER BY id"
            ).fetchall()
        return [dict(r) for r in rows]

    def update_password(self, user_id: int, new_password: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE users SET password_hash=? WHERE id=?",
                (hash_password(new_password), user_id),
            )
        logger.info("Password updated user_id=%d", user_id)

    def update_last_login(self, user_id: int) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute("UPDATE users SET last_login=? WHERE id=?", (now, user_id))

    def delete_user(self, user_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM users WHERE id=?", (user_id,))
        return cur.rowcount > 0

    def has_any_user(self) -> bool:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] > 0

    def count_admins(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM users WHERE role='admin'").fetchone()[0]

    def authenticate(self, username: str, password: str) -> dict[str, Any] | None:
        user = self.get_by_username(username)
        if user is None:
            verify_password(password, hash_password("dummy"))
            return None
        if not verify_password(password, user["password_hash"]):
            logger.warning("Failed login: %s", username)
            return None
        self.update_last_login(user["id"])
        logger.info("User logged in: %s", username)
        return user

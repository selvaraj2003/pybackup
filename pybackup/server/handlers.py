"""
REST API handlers — pybackup dashboard.

Routes:
  GET    /api/auth/setup-needed      — check if any user exists
  POST   /api/auth/login             — login, returns session token
  POST   /api/auth/logout            — logout
  GET    /api/auth/me                — current user info
  POST   /api/auth/change-password   — change own password
  GET    /api/users                  — list users (admin only)
  POST   /api/users                  — create user (admin only)
  DELETE /api/users/:id              — delete user (admin only)
  GET    /api/stats
  GET    /api/runs
  POST   /api/runs
  GET    /api/runs/:id
  DELETE /api/runs/:id
  GET    /api/settings
  POST   /api/settings
"""

from __future__ import annotations
import logging

from pybackup.auth import sessions

logger = logging.getLogger(__name__)


# ── Auth helpers ─────────────────────────────────────────────────────


def _get_session(req):
    """Extract session from Authorization header or cookie."""
    auth = req.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return sessions.get(auth[7:])
    # Also accept token from cookie
    cookie = req.headers.get("Cookie", "")
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("pb_token="):
            return sessions.get(part[9:])
    return None


def _require_auth(req):
    """Return (session, None) or (None, error_response)."""
    from pybackup.server.httpserver import error_response

    s = _get_session(req)
    if s is None:
        return None, error_response("Authentication required", 401)
    return s, None


def _require_admin(req):
    s, err = _require_auth(req)
    if err:
        return None, err
    from pybackup.server.httpserver import error_response

    if s.role != "admin":
        return None, error_response("Admin access required", 403)
    return s, None


# ── Auth handlers ────────────────────────────────────────────────────


def handle_setup_needed(req, db):
    from pybackup.server.httpserver import json_response
    from pybackup.server.httpserver import PyBackupHandler

    user_db = PyBackupHandler.user_db
    return json_response({"setup_needed": not user_db.has_any_user()})


def handle_login(req, db):
    from pybackup.server.httpserver import json_response, error_response

    try:
        body = req.json()
    except Exception:
        return error_response("Invalid JSON body")

    username = str(body.get("username", "")).strip()
    password = str(body.get("password", ""))

    if not username or not password:
        return error_response("Username and password are required")

    from pybackup.server.httpserver import PyBackupHandler

    user_db = PyBackupHandler.user_db
    user = user_db.authenticate(username, password)
    if user is None:
        return error_response("Invalid username or password", 401)

    token = sessions.create(user["id"], user["username"], user["role"])
    return json_response(
        {
            "token": token,
            "username": user["username"],
            "role": user["role"],
        }
    )


def handle_logout(req, db):
    from pybackup.server.httpserver import json_response

    s = _get_session(req)
    if s:
        sessions.delete(s.token)
    return json_response({"ok": True})


def handle_me(req, db):
    from pybackup.server.httpserver import json_response

    s, err = _require_auth(req)
    if err:
        return err
    return json_response({"user_id": s.user_id, "username": s.username, "role": s.role})


def handle_change_password(req, db):
    from pybackup.server.httpserver import json_response, error_response

    s, err = _require_auth(req)
    if err:
        return err
    try:
        body = req.json()
    except Exception:
        return error_response("Invalid JSON body")

    current = str(body.get("current_password", ""))
    new_pw = str(body.get("new_password", ""))
    confirm = str(body.get("confirm_password", ""))

    if not current or not new_pw or not confirm:
        return error_response("All password fields are required")
    if new_pw != confirm:
        return error_response("New passwords do not match")
    if len(new_pw) < 8:
        return error_response("Password must be at least 8 characters")

    from pybackup.server.httpserver import PyBackupHandler

    user_db = PyBackupHandler.user_db
    user = user_db.get_by_id(s.user_id)
    if user is None:
        return error_response("User not found", 404)

    from pybackup.auth import verify_password

    if not verify_password(current, user["password_hash"]):
        return error_response("Current password is incorrect", 401)

    user_db.update_password(s.user_id, new_pw)
    # Invalidate all sessions for this user
    sessions.delete(s.token)
    return json_response({"ok": True, "message": "Password changed. Please log in again."})


# ── User management (admin only) ──────────────────────────────────────


def handle_list_users(req, db):
    from pybackup.server.httpserver import json_response

    s, err = _require_admin(req)
    if err:
        return err
    from pybackup.server.httpserver import PyBackupHandler

    user_db = PyBackupHandler.user_db
    users = user_db.list_users()
    # Never return password hashes
    for u in users:
        u.pop("password_hash", None)
    return json_response({"users": users})


def handle_create_user(req, db):
    from pybackup.server.httpserver import json_response, error_response

    s, err = _require_admin(req)
    if err:
        return err
    try:
        body = req.json()
    except Exception:
        return error_response("Invalid JSON body")

    username = str(body.get("username", "")).strip()
    password = str(body.get("password", ""))
    role = str(body.get("role", "viewer"))
    email = body.get("email")

    if not username or not password:
        return error_response("Username and password are required")
    if len(password) < 8:
        return error_response("Password must be at least 8 characters")

    from pybackup.utils.exceptions import SecurityError
    from pybackup.server.httpserver import PyBackupHandler

    user_db = PyBackupHandler.user_db
    try:
        uid = user_db.create_user(username, password, role=role, email=email)
        user = user_db.get_by_id(uid)
        user.pop("password_hash", None)
        return json_response(user, 201)
    except SecurityError as exc:
        return error_response(str(exc), 400)


def handle_delete_user(req, db):
    from pybackup.server.httpserver import json_response, error_response

    s, err = _require_admin(req)
    if err:
        return err

    uid = _parse_id(req)
    if uid is None:
        return error_response("Invalid user id", 400)
    if uid == s.user_id:
        return error_response("Cannot delete your own account", 400)

    from pybackup.server.httpserver import PyBackupHandler

    user_db = PyBackupHandler.user_db
    if user_db.count_admins() <= 1:
        target = user_db.get_by_id(uid)
        if target and target.get("role") == "admin":
            return error_response("Cannot delete the last admin account", 400)

    deleted = user_db.delete_user(uid)
    if not deleted:
        return error_response("User not found", 404)
    return json_response({"deleted": uid})


# ── Backup data handlers ──────────────────────────────────────────────


def handle_stats(req, db):
    from pybackup.server.httpserver import json_response, error_response

    _, err = _require_auth(req)
    if err:
        return err
    try:
        return json_response(db.stats())
    except Exception as exc:
        logger.exception("stats error")
        return error_response(str(exc), 500)


def handle_list_runs(req, db):
    from pybackup.server.httpserver import json_response

    _, err = _require_auth(req)
    if err:
        return err
    limit = min(req.query_int("limit", 50), 500)
    offset = req.query_int("offset", 0)
    job = req.query_str("job") or None
    status = req.query_str("status") or None
    runs = db.list_runs(limit=limit, offset=offset, job_name=job, status=status)
    total = db.count_runs(job_name=job, status=status)
    return json_response({"runs": runs, "total": total, "limit": limit, "offset": offset})


def handle_get_run(req, db):
    from pybackup.server.httpserver import json_response, error_response

    _, err = _require_auth(req)
    if err:
        return err
    run_id = _parse_id(req)
    if run_id is None:
        return error_response("Invalid run id", 400)
    run = db.get_run(run_id)
    if run is None:
        return error_response("Run not found", 404)
    run["files"] = db.list_files(run_id)
    return json_response(run)


def handle_delete_run(req, db):
    from pybackup.server.httpserver import json_response, error_response

    _, err = _require_admin(req)
    if err:
        return err
    run_id = _parse_id(req)
    if run_id is None:
        return error_response("Invalid run id", 400)
    if not db.delete_run(run_id):
        return error_response("Run not found", 404)
    return json_response({"deleted": run_id})


def handle_create_run(req, db):
    from pybackup.server.httpserver import json_response, error_response

    _, err = _require_auth(req)
    if err:
        return err
    try:
        body = req.json()
    except Exception:
        return error_response("Invalid JSON body")
    job_name = body.get("job_name", "manual")
    engine = body.get("engine", "manual")
    status = body.get("status", "success")
    run_id = db.create_run(job_name, engine)
    db.finish_run(
        run_id, status=status, output_path=body.get("output_path"), error=body.get("error")
    )
    return json_response(db.get_run(run_id), 201)


def handle_get_settings(req, db):
    from pybackup.server.httpserver import json_response

    _, err = _require_auth(req)
    if err:
        return err
    keys = ["theme", "log_level", "retention_days"]
    return json_response({k: db.get_setting(k) for k in keys})


def handle_update_settings(req, db):
    from pybackup.server.httpserver import json_response, error_response

    _, err = _require_auth(req)
    if err:
        return err
    try:
        body = req.json()
    except Exception:
        return error_response("Invalid JSON body")
    allowed = {"theme", "log_level", "retention_days"}
    updated = {}
    for key, val in body.items():
        if key in allowed:
            db.set_setting(key, str(val))
            updated[key] = val
    return json_response({"updated": updated})


# ── Route registration ────────────────────────────────────────────────


def register_routes(router) -> None:
    # Auth
    router.add("GET", "/api/auth/setup-needed", handle_setup_needed)
    router.add("POST", "/api/auth/login", handle_login)
    router.add("POST", "/api/auth/logout", handle_logout)
    router.add("GET", "/api/auth/me", handle_me)
    router.add("POST", "/api/auth/change-password", handle_change_password)
    # User management
    router.add("GET", "/api/users", handle_list_users)
    router.add("POST", "/api/users", handle_create_user)
    router.add("DELETE", "/api/users/:id", handle_delete_user)
    # Backup data
    router.add("GET", "/api/stats", handle_stats)
    router.add("GET", "/api/runs", handle_list_runs)
    router.add("POST", "/api/runs", handle_create_run)
    router.add("GET", "/api/runs/:id", handle_get_run)
    router.add("DELETE", "/api/runs/:id", handle_delete_run)
    router.add("GET", "/api/settings", handle_get_settings)
    router.add("POST", "/api/settings", handle_update_settings)


def _parse_id(req) -> int | None:
    raw = req.path_params.get("id") or req.query_str("id")
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None

"""
REST API route handlers for the pybackup web dashboard.

Routes registered with :id pattern syntax:
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
from typing import Any

logger = logging.getLogger(__name__)


def handle_stats(req, db):
    from pybackup.server.httpserver import json_response, error_response
    try:
        return json_response(db.stats())
    except Exception as exc:
        logger.exception("stats error")
        return error_response(str(exc), 500)


def handle_list_runs(req, db):
    from pybackup.server.httpserver import json_response
    limit  = min(req.query_int("limit", 50), 500)
    offset = req.query_int("offset", 0)
    job    = req.query_str("job")    or None
    status = req.query_str("status") or None
    runs   = db.list_runs(limit=limit, offset=offset, job_name=job, status=status)
    total  = db.count_runs(job_name=job, status=status)
    return json_response({"runs": runs, "total": total, "limit": limit, "offset": offset})


def handle_get_run(req, db):
    from pybackup.server.httpserver import json_response, error_response
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
    run_id = _parse_id(req)
    if run_id is None:
        return error_response("Invalid run id", 400)
    deleted = db.delete_run(run_id)
    if not deleted:
        return error_response("Run not found", 404)
    return json_response({"deleted": run_id})


def handle_create_run(req, db):
    from pybackup.server.httpserver import json_response, error_response
    try:
        body = req.json()
    except Exception:
        return error_response("Invalid JSON body")
    job_name = body.get("job_name", "manual")
    engine   = body.get("engine",   "manual")
    status   = body.get("status",   "success")
    run_id = db.create_run(job_name, engine)
    db.finish_run(run_id, status=status,
                  output_path=body.get("output_path"), error=body.get("error"))
    return json_response(db.get_run(run_id), 201)


def handle_get_settings(req, db):
    from pybackup.server.httpserver import json_response
    keys = ["theme", "log_level", "retention_days"]
    return json_response({k: db.get_setting(k) for k in keys})


def handle_update_settings(req, db):
    from pybackup.server.httpserver import json_response, error_response
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


def register_routes(router) -> None:
    router.add("GET",    "/api/stats",         handle_stats)
    router.add("GET",    "/api/runs",           handle_list_runs)
    router.add("POST",   "/api/runs",           handle_create_run)
    router.add("GET",    "/api/runs/:id",       handle_get_run)
    router.add("DELETE", "/api/runs/:id",       handle_delete_run)
    router.add("GET",    "/api/settings",       handle_get_settings)
    router.add("POST",   "/api/settings",       handle_update_settings)


def _parse_id(req) -> int | None:
    raw = req.path_params.get("id") or req.query_str("id")
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None

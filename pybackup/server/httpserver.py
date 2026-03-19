"""
Pure-Python HTTP server for the pybackup web dashboard.
No Flask. No FastAPI. No Django. Just stdlib http.server.

Router supports:
  - Exact routes:   /api/runs
  - Param routes:   /api/runs/:id
"""
from __future__ import annotations

import json
import logging
import mimetypes
import re
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from pybackup.utils.exceptions import ServerError

logger = logging.getLogger(__name__)
_STATIC_DIR = Path(__file__).parent.parent / "static"


# ── Router ────────────────────────────────────────────────────────────

class Router:
    """
    Pattern-based HTTP router.

    Supports:
        /api/runs          — exact match
        /api/runs/:id      — named param capture
    """

    def __init__(self) -> None:
        # Each entry: (method, compiled_regex, param_names, handler)
        self._routes: list[tuple[str, re.Pattern, list[str], Callable]] = []

    def add(self, method: str, path: str, fn: Callable) -> None:
        """Register a route. Use :name for path parameters (e.g. /api/runs/:id)."""
        param_names: list[str] = []
        # Convert /api/runs/:id → /api/runs/(?P<id>[^/]+)
        pattern = re.sub(
            r":([a-zA-Z_][a-zA-Z0-9_]*)",
            lambda m: f"(?P<{m.group(1)}>[^/]+)",
            re.escape(path).replace(r"\:",":")   # re.escape damages :name
        )
        # Re-do properly: escape the non-param parts only
        parts = path.split("/")
        regex_parts = []
        for part in parts:
            if part.startswith(":"):
                name = part[1:]
                param_names.append(name)
                regex_parts.append(f"(?P<{name}>[^/]+)")
            else:
                regex_parts.append(re.escape(part))
        pattern = "^" + "/".join(regex_parts) + "$"
        self._routes.append((method.upper(), re.compile(pattern), param_names, fn))

    def match(self, method: str, path: str) -> tuple[Callable | None, dict[str, str]]:
        m = method.upper()
        clean = path.rstrip("/") or "/"
        for route_method, regex, _names, fn in self._routes:
            if route_method != m:
                continue
            hit = regex.match(clean)
            if hit:
                return fn, hit.groupdict()
        return None, {}


# ── Request ───────────────────────────────────────────────────────────

class Request:
    def __init__(self, method: str, path: str, query: dict,
                 headers: Any, body: bytes) -> None:
        self.method = method
        self.path = path
        self.query = query
        self.headers = headers
        self.body = body
        self.path_params: dict[str, str] = {}

    def json(self) -> Any:
        try:
            return json.loads(self.body.decode("utf-8"))
        except Exception as exc:
            raise ServerError("Invalid JSON body", details=str(exc)) from exc

    def query_str(self, key: str, default: str = "") -> str:
        return (self.query.get(key) or [default])[0]

    def query_int(self, key: str, default: int = 0) -> int:
        try:
            return int(self.query_str(key, str(default)))
        except ValueError:
            return default


# ── Responses ─────────────────────────────────────────────────────────

def json_response(data: Any, status: int = 200) -> tuple[int, dict, bytes]:
    body = json.dumps(data, default=str).encode("utf-8")
    return status, {
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
        "Cache-Control": "no-store",
    }, body


def error_response(message: str, status: int = 400) -> tuple[int, dict, bytes]:
    return json_response({"error": message}, status)


# ── Handler ───────────────────────────────────────────────────────────

class PyBackupHandler(BaseHTTPRequestHandler):
    router: Router
    db: Any

    def log_message(self, fmt: str, *args: Any) -> None:  # type: ignore[override]
        logger.debug("HTTP %s — %s", self.address_string(), fmt % args)

    def log_error(self, fmt: str, *args: Any) -> None:  # type: ignore[override]
        logger.warning("HTTP error: %s", fmt % args)

    def _cors(self) -> dict[str, str]:
        return {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }

    def _send(self, status: int, headers: dict, body: bytes) -> None:
        self.send_response(status)
        for k, v in {**self._cors(), **headers}.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _dispatch(self, method: str) -> None:
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"
        query  = parse_qs(parsed.query)
        clen   = int(self.headers.get("Content-Length", 0) or 0)
        body   = self.rfile.read(clen) if clen else b""

        req = Request(method, path, query, self.headers, body)

        if path.startswith("/api"):
            fn, params = self.router.match(method, path)
            req.path_params = params
            if fn is None:
                return self._send(*error_response("Route not found", 404))
            try:
                self._send(*fn(req, self.db))
            except ServerError as exc:
                self._send(*error_response(str(exc), 400))
            except Exception as exc:
                logger.exception("Unhandled API error")
                self._send(*error_response(f"Internal error: {exc}", 500))
            return

        self._serve_static(path)

    def _serve_static(self, rel_path: str) -> None:
        safe = Path(rel_path.lstrip("/"))
        if ".." in safe.parts:
            return self._send(*error_response("Forbidden", 403))
        fp = _STATIC_DIR / (safe if str(safe) not in ("", ".") else Path("index.html"))
        if not fp.exists() or not fp.is_file():
            fp = _STATIC_DIR / "index.html"   # SPA fallback
        if not fp.exists():
            return self._send(*error_response("Not found", 404))
        mime, _ = mimetypes.guess_type(str(fp))
        data = fp.read_bytes()
        self._send(200, {
            "Content-Type": mime or "application/octet-stream",
            "Content-Length": str(len(data)),
            "Cache-Control": "public, max-age=3600",
        }, data)

    def do_GET(self)     -> None: self._dispatch("GET")
    def do_POST(self)    -> None: self._dispatch("POST")
    def do_DELETE(self)  -> None: self._dispatch("DELETE")
    def do_OPTIONS(self) -> None: self._send(204, self._cors(), b"")


# ── Server ────────────────────────────────────────────────────────────

class PyBackupServer:
    """
    Threaded HTTP server for the pybackup web dashboard.

    Usage::

        server = PyBackupServer(db=db, host="0.0.0.0", port=8741)
        server.start()   # blocks until SIGINT / SIGTERM
    """

    def __init__(self, db: Any, host: str = "0.0.0.0", port: int = 8741) -> None:
        from pybackup.server.handlers import register_routes
        router = Router()
        register_routes(router)
        PyBackupHandler.router = router
        PyBackupHandler.db = db
        self._httpd = ThreadingHTTPServer((host, port), PyBackupHandler)
        self.host = host
        self.port = port

    def start(self) -> None:
        logger.info("PyBackup dashboard → http://%s:%d", self.host, self.port)

        def _shutdown(sig: int, _frame: Any) -> None:
            logger.info("Signal %d — shutting down…", sig)
            threading.Thread(target=self._httpd.shutdown, daemon=True).start()

        # signal handlers can only be registered from the main thread
        import threading as _t
        if _t.current_thread() is _t.main_thread():
            signal.signal(signal.SIGINT,  _shutdown)
            signal.signal(signal.SIGTERM, _shutdown)

        try:
            self._httpd.serve_forever()
        finally:
            self._httpd.server_close()
            logger.info("Server stopped.")

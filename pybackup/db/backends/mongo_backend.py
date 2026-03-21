"""MongoDB backend for pybackup. Requires: pip install pymongo"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pybackup.utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

try:
    from pymongo import MongoClient, DESCENDING

    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False


class MongoDatabase:
    def __init__(self, cfg: dict[str, Any]) -> None:
        if not _AVAILABLE:
            raise DatabaseError("pymongo not installed. Run: pip install pymongo")
        host = cfg.get("host", "localhost")
        port = int(cfg.get("port", 27017))
        name = cfg.get("name", "pybackup")
        uri = "mongodb://"
        if cfg.get("user") and cfg.get("password"):
            uri += f"{cfg['user']}:{cfg['password']}@"
        uri += f"{host}:{port}/{name}"
        try:
            self._client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            self._db = self._client[name]
            self._runs = self._db.backup_runs
            self._files = self._db.backup_files
            self._settings = self._db.settings
            self._client.admin.command("ping")
        except Exception as exc:
            raise DatabaseError("MongoDB connect failed", details=str(exc)) from exc

    def create_run(
        self,
        job_name: str,
        engine: str,
        details: dict | None = None,
    ) -> str:
        now = datetime.now(tz=timezone.utc).isoformat()
        doc = dict(
            job_name=job_name,
            engine=engine,
            status="running",
            started_at=now,
            finished_at=None,
            output_path=None,
            error=None,
            details=details or {},
        )
        result = self._runs.insert_one(doc)
        return str(result.inserted_id)

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        output_path: str | None = None,
        error: str | None = None,
    ) -> None:
        from bson import ObjectId

        now = datetime.now(tz=timezone.utc).isoformat()
        self._runs.update_one(
            {"_id": ObjectId(run_id)},
            {
                "$set": {
                    "status": status,
                    "finished_at": now,
                    "output_path": output_path,
                    "error": error,
                }
            },
        )

    def get_run(self, run_id: str) -> dict | None:
        from bson import ObjectId

        doc = self._runs.find_one({"_id": ObjectId(run_id)})
        if doc:
            doc["id"] = str(doc.pop("_id"))
        return doc

    def list_runs(
        self,
        limit: int = 100,
        offset: int = 0,
        job_name: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        filt: dict = {}
        if job_name:
            filt["job_name"] = job_name
        if status:
            filt["status"] = status
        docs = list(self._runs.find(filt).sort("started_at", DESCENDING).skip(offset).limit(limit))
        for d in docs:
            d["id"] = str(d.pop("_id"))
        return docs

    def count_runs(
        self,
        job_name: str | None = None,
        status: str | None = None,
    ) -> int:
        filt: dict = {}
        if job_name:
            filt["job_name"] = job_name
        if status:
            filt["status"] = status
        return self._runs.count_documents(filt)

    def delete_run(self, run_id: str) -> bool:
        from bson import ObjectId

        r = self._runs.delete_one({"_id": ObjectId(run_id)})
        return r.deleted_count > 0

    def add_file(
        self,
        run_id: str,
        file_path: str,
        file_size: int | None = None,
        checksum: str | None = None,
    ) -> str:
        now = datetime.now(tz=timezone.utc).isoformat()
        r = self._files.insert_one(
            dict(
                run_id=run_id,
                file_path=file_path,
                file_size=file_size,
                checksum=checksum,
                created_at=now,
            )
        )
        return str(r.inserted_id)

    def list_files(self, run_id: str) -> list[dict]:
        docs = list(self._files.find({"run_id": run_id}))
        for d in docs:
            d["id"] = str(d.pop("_id"))
        return docs

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        doc = self._settings.find_one({"key": key})
        return doc["value"] if doc else default

    def set_setting(self, key: str, value: str) -> None:
        self._settings.update_one(
            {"key": key},
            {"$set": {"key": key, "value": value}},
            upsert=True,
        )

    def stats(self) -> dict:
        total = self._runs.count_documents({})
        success = self._runs.count_documents({"status": "success"})
        failed = self._runs.count_documents({"status": {"$in": ["failed", "crashed"]}})
        running = self._runs.count_documents({"status": "running"})
        recent = self.list_runs(limit=10)
        by_engine = list(
            self._runs.aggregate(
                [
                    {
                        "$group": {
                            "_id": "$engine",
                            "count": {"$sum": 1},
                            "successes": {
                                "$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}
                            },
                        }
                    },
                ]
            )
        )
        for e in by_engine:
            e["engine"] = e.pop("_id")
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "running": running,
            "success_rate": round((success / total * 100) if total else 0, 1),
            "recent": recent,
            "by_engine": by_engine,
            "daily": [],
        }

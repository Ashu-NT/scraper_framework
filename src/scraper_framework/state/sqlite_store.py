from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

from scraper_framework.state.base import IncrementalDecision, RunCheckpoint
from scraper_framework.utils.time import utc_now_iso


class SQLiteIncrementalStateStore:
    """SQLite-backed state store for incremental runs and checkpoints."""

    def __init__(self, path: str):
        self.path = path
        self._ensure_parent_dir(path)
        self._ensure_schema()

    def mark_run_started(self, job_id: str) -> int:
        now = utc_now_iso()
        with self._session() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO job_runs (job_id, run_count, last_started_utc, last_completed_utc)
                VALUES (?, 0, NULL, NULL)
                """,
                (job_id,),
            )
            conn.execute(
                """
                UPDATE job_runs
                SET run_count = run_count + 1,
                    last_started_utc = ?
                WHERE job_id = ?
                """,
                (now, job_id),
            )
            row = conn.execute(
                "SELECT run_count FROM job_runs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            return int(row["run_count"]) if row else 1

    def mark_run_completed(self, job_id: str) -> None:
        now = utc_now_iso()
        with self._session() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO job_runs (job_id, run_count, last_started_utc, last_completed_utc)
                VALUES (?, 0, NULL, NULL)
                """,
                (job_id,),
            )
            conn.execute(
                """
                UPDATE job_runs
                SET last_completed_utc = ?
                WHERE job_id = ?
                """,
                (now, job_id),
            )

    def load_checkpoint(self, job_id: str) -> Optional[RunCheckpoint]:
        with self._session() as conn:
            row = conn.execute(
                """
                SELECT request_json, page_index, status, updated_at_utc
                FROM run_checkpoint
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()

        if not row:
            return None

        request_payload = None
        if row["request_json"]:
            try:
                request_payload = json.loads(row["request_json"])
            except Exception:
                request_payload = None

        return RunCheckpoint(
            request_payload=request_payload,
            page_index=int(row["page_index"] or 0),
            status=str(row["status"] or ""),
            updated_at_utc=str(row["updated_at_utc"] or ""),
        )

    def save_checkpoint(
        self,
        job_id: str,
        request_payload: Optional[Dict[str, Any]],
        page_index: int,
        status: str = "in_progress",
    ) -> None:
        now = utc_now_iso()
        request_json = json.dumps(request_payload, ensure_ascii=False, sort_keys=True) if request_payload else None
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO run_checkpoint (job_id, request_json, page_index, status, updated_at_utc)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    request_json = excluded.request_json,
                    page_index = excluded.page_index,
                    status = excluded.status,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (job_id, request_json, int(page_index), status, now),
            )

    def clear_checkpoint(self, job_id: str) -> None:
        with self._session() as conn:
            conn.execute("DELETE FROM run_checkpoint WHERE job_id = ?", (job_id,))

    def decide_and_touch(
        self,
        job_id: str,
        dedupe_key: str,
        content_hash: str,
        mode: str,
    ) -> IncrementalDecision:
        key = str(dedupe_key or "").strip()
        if not key:
            raise ValueError("dedupe_key cannot be empty for incremental state")

        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in {"all", "new_only", "changed_only"}:
            raise ValueError(f"Unsupported incremental mode: {mode}")

        now = utc_now_iso()
        with self._session() as conn:
            row = conn.execute(
                """
                SELECT content_hash, first_seen_utc, last_changed_utc, seen_count
                FROM record_state
                WHERE job_id = ? AND dedupe_key = ?
                """,
                (job_id, key),
            ).fetchone()

            if not row:
                conn.execute(
                    """
                    INSERT INTO record_state
                    (job_id, dedupe_key, content_hash, first_seen_utc, last_seen_utc, last_changed_utc, seen_count)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                    """,
                    (job_id, key, content_hash, now, now, now),
                )
                return IncrementalDecision(emit=True, is_new=True, changed=True)

            previous_hash = str(row["content_hash"] or "")
            changed = previous_hash != content_hash

            if normalized_mode == "all":
                emit = True
            elif normalized_mode == "new_only":
                emit = False
            else:
                emit = changed

            last_changed = now if changed else str(row["last_changed_utc"] or now)
            conn.execute(
                """
                UPDATE record_state
                SET content_hash = ?,
                    last_seen_utc = ?,
                    last_changed_utc = ?,
                    seen_count = seen_count + 1
                WHERE job_id = ? AND dedupe_key = ?
                """,
                (content_hash, now, last_changed, job_id, key),
            )

            return IncrementalDecision(emit=emit, is_new=False, changed=changed)

    def _ensure_schema(self) -> None:
        with self._session() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_runs (
                    job_id TEXT PRIMARY KEY,
                    run_count INTEGER NOT NULL DEFAULT 0,
                    last_started_utc TEXT,
                    last_completed_utc TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_checkpoint (
                    job_id TEXT PRIMARY KEY,
                    request_json TEXT,
                    page_index INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS record_state (
                    job_id TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    first_seen_utc TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL,
                    last_changed_utc TEXT NOT NULL,
                    seen_count INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY (job_id, dedupe_key)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_record_state_job_last_seen
                ON record_state (job_id, last_seen_utc)
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _session(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _ensure_parent_dir(self, path: str) -> None:
        parent = Path(path).parent
        if str(parent) not in {"", "."}:
            parent.mkdir(parents=True, exist_ok=True)

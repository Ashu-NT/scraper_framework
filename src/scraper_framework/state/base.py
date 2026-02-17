from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol


@dataclass(frozen=True)
class IncrementalDecision:
    """Decision for whether a record should be emitted this run."""

    emit: bool
    is_new: bool
    changed: bool


@dataclass(frozen=True)
class RunCheckpoint:
    """Persisted checkpoint for resuming a job."""

    request_payload: Optional[Dict[str, Any]]
    page_index: int
    status: str
    updated_at_utc: str


class IncrementalStateStore(Protocol):
    """Protocol for incremental state backends."""

    def mark_run_started(self, job_id: str) -> int: ...

    def mark_run_completed(self, job_id: str) -> None: ...

    def load_checkpoint(self, job_id: str) -> Optional[RunCheckpoint]: ...

    def save_checkpoint(
        self,
        job_id: str,
        request_payload: Optional[Dict[str, Any]],
        page_index: int,
        status: str = "in_progress",
    ) -> None: ...

    def clear_checkpoint(self, job_id: str) -> None: ...

    def decide_and_touch(
        self,
        job_id: str,
        dedupe_key: str,
        content_hash: str,
        mode: str,
    ) -> IncrementalDecision: ...

from __future__ import annotations
from typing import List, Protocol
from scraper_framework.core.models import Record, ScrapeJob

class Sink(Protocol):
    """Protocol for output sinks."""

    def write(self, job: ScrapeJob, records: List[Record]) -> None: ...

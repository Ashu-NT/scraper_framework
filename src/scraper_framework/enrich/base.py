from __future__ import annotations

from typing import Protocol

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.models import Record


class Enricher(Protocol):
    """Protocol for record enrichers."""

    def should_enrich(self, record: Record) -> bool: ...

    def enrich(self, record: Record, adapter: SiteAdapter) -> Record: ...

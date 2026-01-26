from __future__ import annotations
from typing import Protocol
from scraper_framework.core.models import Record
from scraper_framework.adapters.base import SiteAdapter


class Enricher(Protocol):
    """Protocol for record enrichers."""

    def should_enrich(self, record: Record) -> bool: ...
    def enrich(self, record: Record, adapter: SiteAdapter) -> Record: ...

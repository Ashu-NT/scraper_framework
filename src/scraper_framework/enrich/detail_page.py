from __future__ import annotations
from typing import Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scraper_framework.enrich.base import Enricher
from scraper_framework.core.models import Record, RequestSpec
from scraper_framework.fetch.strategies import FetchStrategy
from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.utils.logging import get_logger


class DetailPageEnricher(Enricher):
    """Enricher that fetches additional data from detail pages."""

    def __init__(self, fetcher: FetchStrategy, fields: set[str]):
        self.fetcher = fetcher
        self.fields = fields
        self.log = get_logger("scraper_framework.enrich")

    def should_enrich(self, record: Record) -> bool:
        """Check if the record needs enrichment."""
        for f in self.fields:
            if not record.fields.get(f):
                return True
        return False

    def enrich(self, record: Record, adapter: SiteAdapter) -> Record:
        """Enrich the record with data from its detail page."""
        try:
            page = self.fetcher.fetch(RequestSpec(url=record.source_url))
            soup = BeautifulSoup(page.raw, "html.parser")

            for field in self.fields:
                if record.fields.get(field):
                    continue

                loc = adapter.field_locator(f"detail:{field}")
                if not loc:
                    continue

                el = soup.select_one(loc)
                if el:
                    record.fields[field] = el.get_text(" ", strip=True)

        except Exception as e:
            self.log.warning("Enrichment failed for %s (%s)", record.source_url, type(e).__name__)

        return record

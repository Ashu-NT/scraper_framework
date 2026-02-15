from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.engine import ScrapeEngine
from scraper_framework.core.models import DedupeMode, ScrapeJob
from scraper_framework.enrich.detail_page import DetailPageEnricher
from scraper_framework.fetch.strategies import (
    FetchStrategy,
    JsonApiFetchStrategy,
    StaticHtmlFetchStrategy,
)
from scraper_framework.http.client import RequestsHttpClient
from scraper_framework.parse.parsers import HtmlPageParser, JsonPageParser, PageParser
from scraper_framework.process.registry import create_default_registry
from scraper_framework.process.runner import ProcessingRunner
from scraper_framework.sinks.base import Sink
from scraper_framework.sinks.csv_sink import CsvSink
from scraper_framework.sinks.gsheet_sink import GoogleSheetsSink
from scraper_framework.transform.dedupe import DedupeStrategy, HashDedupeStrategy, UrlDedupeStrategy
from scraper_framework.transform.normalizers import DefaultNormalizer, Normalizer
from scraper_framework.transform.validators import RequiredFieldsValidator, Validator


@dataclass(frozen=True)
class BuiltComponents:
    engine: ScrapeEngine
    fetcher: FetchStrategy
    parser: PageParser
    sink: Sink
    deduper: DedupeStrategy
    normalizer: Normalizer
    validator: Validator
    enricher: Optional[DetailPageEnricher]
    processor_runner: ProcessingRunner


class ComponentFactory:
    """
    Factory responsible for wiring dependencies.
    Keeps main.py clean and makes your system easy to extend.
    """

    def __init__(self, http_timeout_s: int = 30):
        self.http_timeout_s = http_timeout_s

    def build(self, job: ScrapeJob, adapter: SiteAdapter) -> BuiltComponents:
        """
        Build all components needed for scraping.

        Args:
            job: The scrape job configuration.
            adapter: The site adapter.

        Returns:
            A container with all built components.
        """
        client = self._http_client()
        fetcher = self._fetcher(client, adapter)
        parser = self._parser(adapter)
        deduper = self._deduper(job)
        normalizer = self._normalizer()
        validator = self._validator()
        sink = self._sink(job)
        enricher = self._enricher(job, fetcher)
        processor_runner = self._processor_runner()

        engine = ScrapeEngine(
            fetcher=fetcher,
            parser=parser,
            adapter=adapter,
            normalizer=normalizer,
            validator=validator,
            deduper=deduper,
            sink=sink,
            enricher=enricher,
            processor_runner=processor_runner,
        )

        return BuiltComponents(
            engine=engine,
            fetcher=fetcher,
            parser=parser,
            sink=sink,
            deduper=deduper,
            normalizer=normalizer,
            validator=validator,
            enricher=enricher,
            processor_runner=processor_runner,
        )

    # ---------- Builders (private) ----------

    def _http_client(self) -> RequestsHttpClient:
        """Create the HTTP client."""
        return RequestsHttpClient(timeout_s=self.http_timeout_s)

    def _fetcher(self, client: RequestsHttpClient, adapter: SiteAdapter) -> FetchStrategy:
        """Create the fetch strategy based on adapter mode."""
        mode = (adapter.mode() or "").upper()
        if mode == "JSON_API":
            return JsonApiFetchStrategy(client)

        # For dynamic pages rendered by JavaScript, use a Selenium-backed HTTP client.
        if mode == "DYNAMIC":
            # Import locally to avoid requiring selenium unless used.
            from scraper_framework.http.selenium_client import SeleniumHttpClient

            selenium_client = SeleniumHttpClient(timeout_s=self.http_timeout_s)
            return StaticHtmlFetchStrategy(selenium_client)

        return StaticHtmlFetchStrategy(client)

    def _parser(self, adapter: SiteAdapter) -> PageParser:
        """Create the page parser based on adapter mode."""
        mode = (adapter.mode() or "").upper()
        if mode == "JSON_API":
            return JsonPageParser()
        return HtmlPageParser()

    def _deduper(self, job: ScrapeJob) -> DedupeStrategy:
        """Create the deduplication strategy."""
        if job.dedupe_mode == DedupeMode.BY_HASH:
            return HashDedupeStrategy()
        return UrlDedupeStrategy()

    def _normalizer(self) -> Normalizer:
        """Create the field normalizer."""
        return DefaultNormalizer()

    def _validator(self) -> Validator:
        """Create the record validator."""
        return RequiredFieldsValidator()

    def _sink(self, job: ScrapeJob) -> Sink:
        """Create the output sink."""
        sink_type = str(job.sink_config.get("type", "csv")).lower()
        if sink_type in ("google_sheets", "gsheet", "sheets"):
            return GoogleSheetsSink()

        if sink_type == str("jsonl").lower():
            from scraper_framework.sinks.jsonl_sink import JsonlSink

            return JsonlSink()

        return CsvSink()

    def _enricher(self, job: ScrapeJob, fetcher: FetchStrategy) -> Optional[DetailPageEnricher]:
        """Create the enricher if enabled."""
        if not getattr(job, "enrich", None):
            return None
        if not job.enrich.enabled:
            return None

        fields = job.enrich.fields or {"phone", "website", "address"}
        return DetailPageEnricher(fetcher=fetcher, fields=set(fields))

    def _processor_runner(self) -> ProcessingRunner:
        """Create the processing pipeline runner with built-in plugins."""
        return ProcessingRunner(registry=create_default_registry())

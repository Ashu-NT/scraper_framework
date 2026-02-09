from __future__ import annotations
from typing import List, Optional, Any

from scraper_framework.core.models import Page, Record, RequestSpec, ScrapeJob, ScrapeReport
from scraper_framework.fetch.strategies import FetchStrategy
from scraper_framework.parse.parsers import PageParser
from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.transform.normalizers import Normalizer
from scraper_framework.transform.validators import Validator
from scraper_framework.transform.dedupe import DedupeStrategy
from scraper_framework.sinks.base import Sink
from scraper_framework.utils.time import utc_now_iso
from scraper_framework.utils.hashing import stable_hash
from scraper_framework.http.policies import RateLimiter
from scraper_framework.utils.logging import get_logger

class ScrapeEngine:
    """
    Orchestrates the scraping process by coordinating fetchers, parsers, adapters, and other components.
    """

    def __init__(
        self,
        fetcher: FetchStrategy,
        parser: PageParser,
        adapter: SiteAdapter,
        normalizer: Normalizer,
        validator: Validator,
        deduper: DedupeStrategy,
        sink: Sink,
        enricher=None,
    ):
        """
        Initialize the scrape engine with all necessary components.

        Args:
            fetcher: Strategy for fetching web pages.
            parser: Parser for extracting cards from pages.
            adapter: Site-specific adapter for field extraction.
            normalizer: Component for normalizing record fields.
            validator: Validator for ensuring record completeness.
            deduper: Strategy for deduplicating records.
            sink: Output sink for writing records.
            enricher: Optional enricher for additional data fetching.
        """
        self.fetcher = fetcher
        self.parser = parser
        self.adapter = adapter
        self.normalizer = normalizer
        self.validator = validator
        self.deduper = deduper
        self.sink = sink
        self.enricher = enricher
        self.log = get_logger("scraper_framework.engine")

    def run(self, job: ScrapeJob) -> ScrapeReport:
        """
        Execute the scraping job by fetching pages, extracting records, and writing output.

        Args:
            job: The scrape job configuration.

        Returns:
            A report summarizing the scraping results.
        """
        report = ScrapeReport()
        records: List[Record] = []

        try:
            limiter = RateLimiter(job.delay_ms)

            current: Optional[RequestSpec] = job.start
            pages = 0

            self.log.info("Job started: %s (%s)", job.name, job.id)

            while current and pages < job.max_pages:
                self.log.info("Fetching page %s: %s", pages + 1, current.url)

                page = self.fetcher.fetch(current)
                report.pages_fetched += 1

                cards = self.parser.parse_cards(page, self.adapter)
                report.cards_found += len(cards)
                self.log.info("Cards found: %s", len(cards))

                for card in cards:
                    rec = self.extract(card, page, job)
                    if rec is None:
                        report.records_skipped += 1
                        report.bump_failure("extract_failed")
                        continue

                    # Optional enrichment step
                    if self.enricher and self.enricher.should_enrich(rec):
                        rec = self.enricher.enrich(rec, self.adapter)

                    rec = self.normalizer.normalize(rec)

                    vr = self.validator.validate(rec, job.required_fields)
                    if not vr.ok:
                        report.records_skipped += 1
                        report.bump_failure(vr.reason)
                        continue

                    records.append(rec)
                    report.records_emitted += 1

                current = self.parser.next_request(page, self.adapter, current)
                pages += 1

                limiter.sleep()

            records = self.deduper.dedupe(records)
            self.sink.write(job, records)

            self.log.info(
                "Job done: pages=%s cards=%s emitted=%s skipped=%s",
                report.pages_fetched, report.cards_found, report.records_emitted, report.records_skipped
            )
        finally:
            # Clean up any resources (e.g., Selenium driver for DYNAMIC mode)
            self._cleanup()

        return report

    def extract(self, card, page: Page, job: ScrapeJob) -> Optional[Record]:
        """
        Extract a record from a card element.

        Args:
            card: The parsed card element.
            page: The page containing the card.
            job: The scrape job configuration.

        Returns:
            A Record if extraction succeeds, None otherwise.
        """
        source_url = self.adapter.extract_source_url(card, page)
        if not source_url:
            return None

        fields = {}
        for field in job.field_schema:
            val = self.adapter.extract_field(card, field, page)
            fields[field] = val

        rid = stable_hash(source_url)
        return Record(
            id=rid,
            source_url=source_url,
            scraped_at_utc=utc_now_iso(),
            fields=fields,
        )
    
    def _cleanup(self) -> None:
        """
        Clean up resources used by the fetcher (e.g., Selenium driver for DYNAMIC mode).
        Called automatically after scraping completes.
        """
        try:
            # Check if the fetcher's client has a close method (for SeleniumHttpClient)
            if hasattr(self.fetcher, 'client') and hasattr(self.fetcher.client, 'close'):
                self.fetcher.client.close()
                self.log.info("Closed HTTP client resources (e.g., Selenium driver)")
        except Exception as e:
            self.log.warning("Error closing HTTP client: %s", type(e).__name__)
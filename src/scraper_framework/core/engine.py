from __future__ import annotations
from typing import Any, Dict, List, Optional, Set

from scraper_framework.core.models import Page, Record, RequestSpec, ScrapeJob, ScrapeReport
from scraper_framework.fetch.strategies import FetchStrategy
from scraper_framework.parse.parsers import PageParser
from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.transform.normalizers import Normalizer
from scraper_framework.transform.validators import Validator
from scraper_framework.transform.dedupe import DedupeStrategy
from scraper_framework.sinks.base import Sink
from scraper_framework.process.runner import ProcessingRunner
from scraper_framework.utils.time import utc_now_iso
from scraper_framework.utils.hashing import normalize_text, stable_hash
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
        processor_runner: Optional[ProcessingRunner] = None,
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
            processor_runner: Optional processing pipeline runner.
        """
        self.fetcher = fetcher
        self.parser = parser
        self.adapter = adapter
        self.normalizer = normalizer
        self.validator = validator
        self.deduper = deduper
        self.sink = sink
        self.enricher = enricher
        self.processor_runner = processor_runner
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
        stream_buffer: List[Record] = []
        seen_dedupe_keys: Set[str] = set()
        chunks_flushed = 0
        execution_mode = str(getattr(job, "execution_mode", "memory")).lower()
        if execution_mode not in {"memory", "stream"}:
            raise ValueError("job.execution_mode must be 'memory' or 'stream'")
        stream_mode = execution_mode == "stream"
        batch_size = max(1, int(getattr(job, "batch_size", 500)))

        try:
            limiter = RateLimiter(job.delay_ms)

            current: Optional[RequestSpec] = job.start
            pages = 0

            self.log.info(
                "Job started: %s (%s) mode=%s batch_size=%s",
                job.name,
                job.id,
                execution_mode,
                batch_size,
            )

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

                    if stream_mode:
                        stream_buffer.append(rec)
                        if len(stream_buffer) >= batch_size:
                            chunks_flushed += 1
                            self._flush_stream_chunk(
                                job=job,
                                chunk_records=stream_buffer,
                                seen_dedupe_keys=seen_dedupe_keys,
                                report=report,
                                chunk_index=chunks_flushed,
                            )
                            stream_buffer = []
                    else:
                        records.append(rec)

                current = self.parser.next_request(page, self.adapter, current)
                pages += 1

                limiter.sleep()

            if stream_mode:
                if stream_buffer:
                    chunks_flushed += 1
                    self._flush_stream_chunk(
                        job=job,
                        chunk_records=stream_buffer,
                        seen_dedupe_keys=seen_dedupe_keys,
                        report=report,
                        chunk_index=chunks_flushed,
                    )

                # Keep legacy behavior for empty runs (create empty output/header where applicable).
                if report.records_emitted == 0:
                    self.sink.write(job, [])
            else:
                records = self.deduper.dedupe(records)
                records = self._apply_processing(job, records, report)
                report.records_emitted = len(records)
                self.sink.write(job, records)

            self.log.info(
                "Job done: mode=%s pages=%s cards=%s emitted=%s skipped=%s quarantined=%s chunks=%s",
                execution_mode,
                report.pages_fetched,
                report.cards_found,
                report.records_emitted,
                report.records_skipped,
                report.records_quarantined,
                chunks_flushed,
            )
        finally:
            # Clean up any resources (e.g., Selenium driver for DYNAMIC mode)
            self._cleanup()

        return report

    def _flush_stream_chunk(
        self,
        job: ScrapeJob,
        chunk_records: List[Record],
        seen_dedupe_keys: Set[str],
        report: ScrapeReport,
        chunk_index: int,
    ) -> None:
        """Flush one stream chunk through dedupe -> processing -> sink."""
        if not chunk_records:
            return

        deduped_records, local_unique_count, cross_chunk_duplicates = self._dedupe_stream_chunk(
            chunk_records, seen_dedupe_keys
        )
        processed_records = self._apply_processing(job, deduped_records, report)

        written = len(processed_records)
        if written:
            self.sink.write(job, processed_records)
            report.records_emitted += written

        self.log.info(
            "Chunk flushed: index=%s input=%s local_unique=%s cross_chunk_duplicates=%s written=%s",
            chunk_index,
            len(chunk_records),
            local_unique_count,
            cross_chunk_duplicates,
            written,
        )

    def _dedupe_stream_chunk(
        self,
        records: List[Record],
        seen_dedupe_keys: Set[str],
    ) -> tuple[List[Record], int, int]:
        """Deduplicate records within chunk and across previous chunks."""
        local_unique = self.deduper.dedupe(records)

        unique_records: List[Record] = []
        cross_chunk_duplicates = 0

        for record in local_unique:
            key = str(self.deduper.key(record) or "").strip()
            if not key:
                continue
            if key in seen_dedupe_keys:
                cross_chunk_duplicates += 1
                continue
            seen_dedupe_keys.add(key)
            unique_records.append(record)

        return unique_records, len(local_unique), cross_chunk_duplicates

    def _apply_processing(self, job: ScrapeJob, records: List[Record], report: ScrapeReport) -> List[Record]:
        """Apply processing pipeline when enabled, while accumulating report metrics."""
        if not records:
            return records

        if not self.processor_runner or not getattr(job, "processing", None) or not job.processing.enabled:
            return records

        processing_result = self.processor_runner.run(job, records)
        report.records_quarantined += processing_result.records_quarantined
        self._merge_stage_metrics(report, processing_result.stage_metrics)
        self._merge_processing_artifacts(report, processing_result.artifacts)
        return processing_result.records

    def _merge_stage_metrics(self, report: ScrapeReport, stage_metrics: Dict[str, Dict[str, Any]]) -> None:
        for stage_name, metric in stage_metrics.items():
            existing = report.processing_stage_metrics.get(stage_name)
            if not existing:
                report.processing_stage_metrics[stage_name] = dict(metric)
                continue

            existing["records_in"] = existing.get("records_in", 0) + int(metric.get("records_in", 0))
            existing["records_out"] = existing.get("records_out", 0) + int(metric.get("records_out", 0))
            existing["dropped"] = existing.get("dropped", 0) + int(metric.get("dropped", 0))
            existing["errors"] = existing.get("errors", 0) + int(metric.get("errors", 0))
            existing["latency_ms"] = round(
                float(existing.get("latency_ms", 0.0)) + float(metric.get("latency_ms", 0.0)),
                3,
            )
            report.processing_stage_metrics[stage_name] = existing

    def _merge_processing_artifacts(self, report: ScrapeReport, artifacts: Dict[str, Any]) -> None:
        for stage_name, artifact in artifacts.items():
            if stage_name not in report.processing_artifacts:
                report.processing_artifacts[stage_name] = artifact
                continue

            existing = report.processing_artifacts[stage_name]
            if isinstance(existing, list):
                existing.append(artifact)
                report.processing_artifacts[stage_name] = existing
            else:
                report.processing_artifacts[stage_name] = [existing, artifact]

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

        rid = stable_hash(normalize_text(source_url))
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

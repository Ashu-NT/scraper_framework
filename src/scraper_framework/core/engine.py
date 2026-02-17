from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.models import Page, Record, RequestSpec, ScrapeJob, ScrapeReport
from scraper_framework.fetch.strategies import FetchStrategy
from scraper_framework.http.policies import RateLimiter
from scraper_framework.parse.parsers import PageParser
from scraper_framework.process.runner import ProcessingRunner
from scraper_framework.sinks.base import Sink
from scraper_framework.state.base import IncrementalStateStore
from scraper_framework.transform.dedupe import DedupeStrategy
from scraper_framework.transform.normalizers import Normalizer
from scraper_framework.transform.validators import Validator
from scraper_framework.utils.hashing import normalize_text, stable_hash
from scraper_framework.utils.logging import get_logger
from scraper_framework.utils.time import utc_now_iso


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
        state_store: Optional[IncrementalStateStore] = None,
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
        self.state_store = state_store
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
        execution_mode, stream_mode, batch_size = self._resolve_execution(job)
        incremental_cfg = getattr(job, "incremental", None)
        incremental_enabled = bool(self.state_store and incremental_cfg and incremental_cfg.enabled)
        run_count = 0
        force_full_refresh = False
        successful = False
        current: Optional[RequestSpec] = job.start
        pages = 0

        try:
            limiter = RateLimiter(job.delay_ms)

            if incremental_enabled:
                run_count = self.state_store.mark_run_started(job.id)
                force_full_refresh = self._should_force_full_refresh(incremental_cfg, run_count)
                if incremental_cfg.resume:
                    resumed_request, resumed_pages = self._load_resume_state(job)
                    if resumed_request is not None:
                        current = resumed_request
                        pages = resumed_pages
                        self.log.info("Resuming from checkpoint: pages=%s url=%s", pages, resumed_request.url)

            self.log.info(
                "Job started: %s (%s) mode=%s batch_size=%s incremental=%s run_count=%s full_refresh=%s",
                job.name,
                job.id,
                execution_mode,
                batch_size,
                incremental_enabled,
                run_count,
                force_full_refresh,
            )

            while current and pages < job.max_pages:
                page, cards = self._fetch_and_parse_page(current, report, pages + 1)
                chunks_flushed = self._collect_page_records(
                    cards=cards,
                    page=page,
                    job=job,
                    report=report,
                    stream_mode=stream_mode,
                    stream_buffer=stream_buffer,
                    batch_size=batch_size,
                    seen_dedupe_keys=seen_dedupe_keys,
                    records=records,
                    chunks_flushed=chunks_flushed,
                    force_full_refresh=force_full_refresh,
                )

                current = self.parser.next_request(page, self.adapter, current)
                pages += 1

                if incremental_enabled and incremental_cfg.resume:
                    checkpoint_every = max(1, int(getattr(incremental_cfg, "checkpoint_every_pages", 1) or 1))
                    if pages % checkpoint_every == 0:
                        self.state_store.save_checkpoint(
                            job.id,
                            self._request_to_payload(current),
                            pages,
                            status="in_progress",
                        )

                limiter.sleep()

            chunks_flushed = self._finalize_run(
                job=job,
                stream_mode=stream_mode,
                stream_buffer=stream_buffer,
                seen_dedupe_keys=seen_dedupe_keys,
                report=report,
                records=records,
                chunks_flushed=chunks_flushed,
                force_full_refresh=force_full_refresh,
            )

            self.log.info(
                "Job done: mode=%s pages=%s cards=%s emitted=%s skipped=%s incremental_skipped=%s quarantined=%s chunks=%s",
                execution_mode,
                report.pages_fetched,
                report.cards_found,
                report.records_emitted,
                report.records_skipped,
                report.records_skipped_incremental,
                report.records_quarantined,
                chunks_flushed,
            )
            successful = True
        finally:
            if incremental_enabled:
                try:
                    if successful:
                        if incremental_cfg.resume:
                            self.state_store.clear_checkpoint(job.id)
                        self.state_store.mark_run_completed(job.id)
                    elif incremental_cfg.resume:
                        self.state_store.save_checkpoint(
                            job.id,
                            self._request_to_payload(current),
                            pages,
                            status="in_progress",
                        )
                except Exception as e:
                    self.log.warning("Incremental state sync failed: %s", type(e).__name__)

            # Clean up any resources (e.g., Selenium driver for DYNAMIC mode)
            self._cleanup()

        return report

    def _resolve_execution(self, job: ScrapeJob) -> tuple[str, bool, int]:
        execution_mode = str(getattr(job, "execution_mode", "memory")).lower()
        if execution_mode not in {"memory", "stream"}:
            raise ValueError("job.execution_mode must be 'memory' or 'stream'")
        stream_mode = execution_mode == "stream"
        batch_size = max(1, int(getattr(job, "batch_size", 500)))
        return execution_mode, stream_mode, batch_size

    def _fetch_and_parse_page(self, current: RequestSpec, report: ScrapeReport, page_index: int) -> tuple[Page, List[Any]]:
        self.log.info("Fetching page %s: %s", page_index, current.url)
        page = self.fetcher.fetch(current)
        report.pages_fetched += 1
        cards = self.parser.parse_cards(page, self.adapter)
        report.cards_found += len(cards)
        self.log.info("Cards found: %s", len(cards))
        return page, cards

    def _collect_page_records(
        self,
        cards: List[Any],
        page: Page,
        job: ScrapeJob,
        report: ScrapeReport,
        stream_mode: bool,
        stream_buffer: List[Record],
        batch_size: int,
        seen_dedupe_keys: Set[str],
        records: List[Record],
        chunks_flushed: int,
        force_full_refresh: bool,
    ) -> int:
        for card in cards:
            record = self._build_valid_record(card, page, job, report)
            if record is None:
                continue
            chunks_flushed = self._append_record(
                job=job,
                record=record,
                stream_mode=stream_mode,
                stream_buffer=stream_buffer,
                batch_size=batch_size,
                seen_dedupe_keys=seen_dedupe_keys,
                report=report,
                records=records,
                chunks_flushed=chunks_flushed,
                force_full_refresh=force_full_refresh,
            )
        return chunks_flushed

    def _build_valid_record(self, card: Any, page: Page, job: ScrapeJob, report: ScrapeReport) -> Optional[Record]:
        record = self.extract(card, page, job)
        if record is None:
            report.records_skipped += 1
            report.bump_failure("extract_failed")
            return None

        if self.enricher and self.enricher.should_enrich(record):
            record = self.enricher.enrich(record, self.adapter)

        record = self.normalizer.normalize(record)
        validation = self.validator.validate(record, job.required_fields)
        if not validation.ok:
            report.records_skipped += 1
            report.bump_failure(validation.reason)
            return None
        return record

    def _append_record(
        self,
        job: ScrapeJob,
        record: Record,
        stream_mode: bool,
        stream_buffer: List[Record],
        batch_size: int,
        seen_dedupe_keys: Set[str],
        report: ScrapeReport,
        records: List[Record],
        chunks_flushed: int,
        force_full_refresh: bool,
    ) -> int:
        if not stream_mode:
            records.append(record)
            return chunks_flushed

        stream_buffer.append(record)
        if len(stream_buffer) < batch_size:
            return chunks_flushed

        next_chunk = chunks_flushed + 1
        self._flush_stream_chunk(
            job=job,
            chunk_records=stream_buffer,
            seen_dedupe_keys=seen_dedupe_keys,
            report=report,
            chunk_index=next_chunk,
            force_full_refresh=force_full_refresh,
        )
        stream_buffer.clear()
        return next_chunk

    def _finalize_run(
        self,
        job: ScrapeJob,
        stream_mode: bool,
        stream_buffer: List[Record],
        seen_dedupe_keys: Set[str],
        report: ScrapeReport,
        records: List[Record],
        chunks_flushed: int,
        force_full_refresh: bool,
    ) -> int:
        if stream_mode:
            return self._finalize_stream_run(
                job,
                stream_buffer,
                seen_dedupe_keys,
                report,
                chunks_flushed,
                force_full_refresh=force_full_refresh,
            )

        deduped_records = self.deduper.dedupe(records)
        processed_records = self._apply_processing(job, deduped_records, report)
        emittable_records = self._apply_incremental(job, processed_records, report, force_full_refresh=force_full_refresh)
        report.records_emitted = len(emittable_records)
        self.sink.write(job, emittable_records)
        return chunks_flushed

    def _finalize_stream_run(
        self,
        job: ScrapeJob,
        stream_buffer: List[Record],
        seen_dedupe_keys: Set[str],
        report: ScrapeReport,
        chunks_flushed: int,
        force_full_refresh: bool,
    ) -> int:
        if stream_buffer:
            chunks_flushed += 1
            self._flush_stream_chunk(
                job=job,
                chunk_records=stream_buffer,
                seen_dedupe_keys=seen_dedupe_keys,
                report=report,
                chunk_index=chunks_flushed,
                force_full_refresh=force_full_refresh,
            )

        # Keep legacy behavior for empty runs (create empty output/header where applicable).
        if report.records_emitted == 0:
            self.sink.write(job, [])
        return chunks_flushed

    def _flush_stream_chunk(
        self,
        job: ScrapeJob,
        chunk_records: List[Record],
        seen_dedupe_keys: Set[str],
        report: ScrapeReport,
        chunk_index: int,
        force_full_refresh: bool,
    ) -> None:
        """Flush one stream chunk through dedupe -> processing -> sink."""
        if not chunk_records:
            return

        deduped_records, local_unique_count, cross_chunk_duplicates = self._dedupe_stream_chunk(
            chunk_records, seen_dedupe_keys
        )
        processed_records = self._apply_processing(job, deduped_records, report)
        emittable_records = self._apply_incremental(job, processed_records, report, force_full_refresh=force_full_refresh)

        written = len(emittable_records)
        if written:
            self.sink.write(job, emittable_records)
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

    def _load_resume_state(self, job: ScrapeJob) -> tuple[Optional[RequestSpec], int]:
        if not self.state_store:
            return None, 0

        checkpoint = self.state_store.load_checkpoint(job.id)
        if not checkpoint or checkpoint.status != "in_progress":
            return None, 0
        if not checkpoint.request_payload:
            return None, int(checkpoint.page_index or 0)

        request = self._request_from_payload(checkpoint.request_payload)
        return request, int(checkpoint.page_index or 0)

    def _request_to_payload(self, req: Optional[RequestSpec]) -> Optional[Dict[str, Any]]:
        if req is None:
            return None
        return {
            "url": req.url,
            "method": req.method,
            "headers": dict(req.headers or {}),
            "params": dict(req.params or {}),
            "body": req.body,
        }

    def _request_from_payload(self, payload: Dict[str, Any]) -> RequestSpec:
        return RequestSpec(
            url=str(payload.get("url") or ""),
            method=str(payload.get("method") or "GET"),
            headers=dict(payload.get("headers") or {}),
            params=dict(payload.get("params") or {}),
            body=payload.get("body"),
        )

    def _should_force_full_refresh(self, incremental_cfg: Any, run_count: int) -> bool:
        every = getattr(incremental_cfg, "full_refresh_every_runs", None)
        if every is None:
            return False
        try:
            n = int(every)
        except Exception:
            return False
        return n > 0 and run_count > 0 and (run_count % n == 0)

    def _apply_incremental(
        self,
        job: ScrapeJob,
        records: List[Record],
        report: ScrapeReport,
        force_full_refresh: bool,
    ) -> List[Record]:
        if not records or not self.state_store:
            return records

        incremental_cfg = getattr(job, "incremental", None)
        if not incremental_cfg or not incremental_cfg.enabled:
            return records

        mode = "all" if force_full_refresh else str(getattr(incremental_cfg, "mode", "changed_only") or "changed_only")
        mode = mode.strip().lower()

        emitted: List[Record] = []
        skipped = 0
        for record in records:
            dedupe_key = str(self.deduper.key(record) or record.source_url or "").strip()
            if not dedupe_key:
                emitted.append(record)
                continue

            content_hash = self._record_content_hash(record)
            decision = self.state_store.decide_and_touch(
                job_id=job.id,
                dedupe_key=dedupe_key,
                content_hash=content_hash,
                mode=mode,
            )
            if decision.emit:
                emitted.append(record)
            else:
                skipped += 1

        if skipped:
            report.records_skipped_incremental += skipped
        return emitted

    def _record_content_hash(self, record: Record) -> str:
        payload = {
            "source_url": record.source_url,
            "fields": record.fields,
        }
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return stable_hash(normalize_text(serialized))

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
            if hasattr(self.fetcher, "client") and hasattr(self.fetcher.client, "close"):
                self.fetcher.client.close()
                self.log.info("Closed HTTP client resources (e.g., Selenium driver)")
        except Exception as e:
            self.log.warning("Error closing HTTP client: %s", type(e).__name__)

"""
Tests for chunked streaming execution mode and sink write semantics.
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock

from src.scraper_framework.core.engine import ScrapeEngine
from src.scraper_framework.core.models import (
    Page,
    ProcessingConfig,
    ProcessingStage,
    Record,
    RequestSpec,
    ScrapeJob,
    ValidationResult,
)
from src.scraper_framework.process.registry import create_default_registry
from src.scraper_framework.process.runner import ProcessingRunner
from src.scraper_framework.sinks.csv_sink import CsvSink
from src.scraper_framework.sinks.jsonl_sink import JsonlSink
from src.scraper_framework.transform.dedupe import UrlDedupeStrategy


def _record(idx: int, source_url: str, fields: dict) -> Record:
    return Record(
        id=f"id-{idx}",
        source_url=source_url,
        scraped_at_utc="2026-01-01T00:00:00Z",
        fields=fields,
    )


class TestStreamingEngine(unittest.TestCase):
    def test_stream_mode_flushes_chunks_and_global_dedupes(self):
        fetcher = Mock()
        parser = Mock()
        adapter = Mock()
        normalizer = Mock()
        validator = Mock()
        sink = Mock()

        fetcher.fetch.return_value = Page(
            url="https://example.com",
            status_code=200,
            content_type="text/html",
            raw="<html></html>",
        )
        parser.parse_cards.return_value = [object(), object(), object(), object()]
        parser.next_request.return_value = None
        adapter.extract_source_url.side_effect = [
            "https://example.com/1",
            "https://example.com/2",
            "https://example.com/1",  # duplicate across chunk boundary
            "https://example.com/3",
        ]
        adapter.extract_field.return_value = "x"
        normalizer.normalize.side_effect = lambda rec: rec
        validator.validate.return_value = ValidationResult(ok=True, reason="")

        engine = ScrapeEngine(
            fetcher=fetcher,
            parser=parser,
            adapter=adapter,
            normalizer=normalizer,
            validator=validator,
            deduper=UrlDedupeStrategy(),
            sink=sink,
        )

        job = ScrapeJob(
            id="stream-job",
            name="stream-job",
            start=RequestSpec(url="https://example.com"),
            execution_mode="stream",
            batch_size=2,
            delay_ms=0,
            max_pages=1,
            required_fields={"source_url"},
            field_schema=["name"],
            processing=ProcessingConfig(enabled=False),
            sink_config={"type": "jsonl", "path": "unused.jsonl", "write_mode": "overwrite"},
        )

        report = engine.run(job)
        self.assertEqual(report.records_emitted, 3)

        self.assertEqual(sink.write.call_count, 2)
        written_counts = [len(call.args[1]) for call in sink.write.call_args_list]
        self.assertEqual(written_counts, [2, 1])

    def test_stream_mode_aggregates_processing_stage_metrics(self):
        fetcher = Mock()
        parser = Mock()
        adapter = Mock()
        normalizer = Mock()
        validator = Mock()
        sink = Mock()

        fetcher.fetch.return_value = Page(
            url="https://example.com",
            status_code=200,
            content_type="text/html",
            raw="<html></html>",
        )
        parser.parse_cards.return_value = [object(), object(), object()]
        parser.next_request.return_value = None
        adapter.extract_source_url.side_effect = [
            "https://example.com/1",
            "https://example.com/2",
            "https://example.com/3",
        ]
        adapter.extract_field.side_effect = ["", "keep", "keep2"]
        normalizer.normalize.side_effect = lambda rec: rec
        validator.validate.return_value = ValidationResult(ok=True, reason="")

        engine = ScrapeEngine(
            fetcher=fetcher,
            parser=parser,
            adapter=adapter,
            normalizer=normalizer,
            validator=validator,
            deduper=UrlDedupeStrategy(),
            sink=sink,
            processor_runner=ProcessingRunner(create_default_registry()),
        )

        job = ScrapeJob(
            id="stream-metrics",
            name="stream-metrics",
            start=RequestSpec(url="https://example.com"),
            execution_mode="stream",
            batch_size=2,
            delay_ms=0,
            max_pages=1,
            required_fields={"source_url"},
            field_schema=["name"],
            processing=ProcessingConfig(
                enabled=True,
                schema_version="1.0",
                stages=[
                    ProcessingStage(
                        plugin="drop_if_field_empty",
                        stage_type="record",
                        on_error="fail",
                        config={"field": "name"},
                    )
                ],
            ),
            sink_config={"type": "jsonl", "path": "unused.jsonl", "write_mode": "overwrite"},
        )

        report = engine.run(job)
        self.assertEqual(report.records_emitted, 2)
        self.assertIn("1:drop_if_field_empty", report.processing_stage_metrics)
        stage = report.processing_stage_metrics["1:drop_if_field_empty"]
        self.assertEqual(stage["records_in"], 3)
        self.assertEqual(stage["records_out"], 2)
        self.assertEqual(stage["dropped"], 1)


class TestStreamingSinkWrites(unittest.TestCase):
    def test_csv_stream_overwrite_truncates_once_then_appends(self):
        sink = CsvSink()
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        tmp.close()
        path = tmp.name

        try:
            job = ScrapeJob(
                id="csv-stream",
                name="csv-stream",
                start=RequestSpec(url="https://example.com"),
                execution_mode="stream",
                batch_size=2,
                required_fields={"source_url"},
                field_schema=["name"],
                sink_config={"type": "csv", "path": path, "write_mode": "overwrite"},
            )

            sink.write(job, [_record(1, "https://example.com/1", {"name": "a"})])
            sink.write(job, [_record(2, "https://example.com/2", {"name": "b"})])

            with open(path, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
            self.assertEqual(len(lines), 3)  # header + 2 rows
        finally:
            os.unlink(path)

    def test_jsonl_stream_overwrite_truncates_once_then_appends(self):
        sink = JsonlSink()
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl")
        tmp.close()
        path = tmp.name

        try:
            job = ScrapeJob(
                id="jsonl-stream",
                name="jsonl-stream",
                start=RequestSpec(url="https://example.com"),
                execution_mode="stream",
                batch_size=2,
                required_fields={"source_url"},
                field_schema=["name"],
                sink_config={"type": "jsonl", "path": path, "write_mode": "overwrite"},
            )

            sink.write(job, [_record(1, "https://example.com/1", {"name": "a"})])
            sink.write(job, [_record(2, "https://example.com/2", {"name": "b"})])

            with open(path, "r", encoding="utf-8") as f:
                lines = [json.loads(line) for line in f if line.strip()]
            self.assertEqual(len(lines), 2)
            self.assertEqual(lines[0]["source_url"], "https://example.com/1")
            self.assertEqual(lines[1]["source_url"], "https://example.com/2")
        finally:
            os.unlink(path)

    def test_csv_sink_creates_parent_directory(self):
        sink = CsvSink()
        base_dir = tempfile.mkdtemp()
        path = os.path.join(base_dir, "nested", "output_books.csv")

        try:
            job = ScrapeJob(
                id="csv-parent-dir",
                name="csv-parent-dir",
                start=RequestSpec(url="https://example.com"),
                execution_mode="memory",
                batch_size=2,
                required_fields={"source_url"},
                field_schema=["name"],
                sink_config={"type": "csv", "path": path, "write_mode": "overwrite"},
            )

            sink.write(job, [_record(1, "https://example.com/1", {"name": "a"})])
            self.assertTrue(os.path.exists(path))
        finally:
            shutil.rmtree(base_dir, ignore_errors=True)

    def test_jsonl_sink_creates_parent_directory(self):
        sink = JsonlSink()
        base_dir = tempfile.mkdtemp()
        path = os.path.join(base_dir, "nested", "output_books.jsonl")

        try:
            job = ScrapeJob(
                id="jsonl-parent-dir",
                name="jsonl-parent-dir",
                start=RequestSpec(url="https://example.com"),
                execution_mode="memory",
                batch_size=2,
                required_fields={"source_url"},
                field_schema=["name"],
                sink_config={"type": "jsonl", "path": path, "write_mode": "overwrite"},
            )

            sink.write(job, [_record(1, "https://example.com/1", {"name": "a"})])
            self.assertTrue(os.path.exists(path))
        finally:
            shutil.rmtree(base_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()

"""
Tests for post-scrape processing pipeline contracts and execution behavior.
"""

import unittest
from unittest.mock import Mock

from src.scraper_framework.config_models import ScraperConfig, config_to_job_objects
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
from src.scraper_framework.transform.dedupe import UrlDedupeStrategy


def _record(idx: int, source_url: str, fields: dict) -> Record:
    return Record(
        id=f"id-{idx}",
        source_url=source_url,
        scraped_at_utc="2026-01-01T00:00:00Z",
        fields=fields,
    )


class TestProcessingConfig(unittest.TestCase):
    def test_processing_enabled_requires_stages(self):
        config_data = {
            "job": {
                "id": "job",
                "name": "job",
                "adapter": "books_toscrape",
                "start_url": "https://example.com",
            },
            "sink": {"type": "csv", "path": "out.csv"},
            "processing": {
                "enabled": True,
                "schema_version": "1.0",
                "stages": [],
            },
        }

        with self.assertRaises(ValueError) as cm:
            ScraperConfig(**config_data)
        self.assertIn("processing.stages cannot be empty", str(cm.exception))

    def test_processing_converts_to_core_job_objects(self):
        config_data = {
            "job": {
                "id": "job",
                "name": "job",
                "adapter": "books_toscrape",
                "start_url": "https://example.com",
            },
            "sink": {"type": "csv", "path": "out.csv"},
            "processing": {
                "enabled": True,
                "schema_version": "1.0",
                "stages": [
                    {
                        "plugin": "drop_if_field_empty",
                        "type": "record",
                        "on_error": "quarantine",
                        "config": {"field": "name"},
                    }
                ],
            },
        }

        config = ScraperConfig(**config_data)
        job, _, _ = config_to_job_objects(config)

        self.assertTrue(job.processing.enabled)
        self.assertEqual(job.processing.schema_version, "1.0")
        self.assertEqual(len(job.processing.stages), 1)
        stage = job.processing.stages[0]
        self.assertEqual(stage.plugin, "drop_if_field_empty")
        self.assertEqual(stage.stage_type, "record")
        self.assertEqual(stage.on_error, "quarantine")
        self.assertEqual(stage.config["field"], "name")

    def test_stream_execution_fields_convert_to_core_job(self):
        config_data = {
            "job": {
                "id": "job",
                "name": "job",
                "adapter": "books_toscrape",
                "start_url": "https://example.com",
                "execution_mode": "stream",
                "batch_size": 250,
            },
            "sink": {"type": "jsonl", "path": "out.jsonl", "write_mode": "overwrite"},
        }

        config = ScraperConfig(**config_data)
        job, _, _ = config_to_job_objects(config)
        self.assertEqual(job.execution_mode, "stream")
        self.assertEqual(job.batch_size, 250)


class TestProcessingRunner(unittest.TestCase):
    def setUp(self):
        self.runner = ProcessingRunner(registry=create_default_registry())

    def test_record_stage_drop_and_metrics(self):
        job = ScrapeJob(
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
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
        )
        records = [
            _record(1, "https://example.com/1", {"name": ""}),
            _record(2, "https://example.com/2", {"name": "ok"}),
        ]

        result = self.runner.run(job, records)
        self.assertEqual(len(result.records), 1)
        self.assertEqual(result.records[0].source_url, "https://example.com/2")
        self.assertEqual(result.records_quarantined, 0)

        metrics = result.stage_metrics["1:drop_if_field_empty"]
        self.assertEqual(metrics["records_in"], 2)
        self.assertEqual(metrics["records_out"], 1)
        self.assertEqual(metrics["dropped"], 1)
        self.assertEqual(metrics["errors"], 0)

    def test_record_stage_quarantine_policy(self):
        job = ScrapeJob(
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
            processing=ProcessingConfig(
                enabled=True,
                schema_version="1.0",
                stages=[
                    ProcessingStage(
                        plugin="drop_if_field_empty",
                        stage_type="record",
                        on_error="quarantine",
                        config={},  # missing required field config to trigger plugin error
                    )
                ],
            ),
        )
        records = [
            _record(1, "https://example.com/1", {"name": "one"}),
            _record(2, "https://example.com/2", {"name": "two"}),
        ]

        result = self.runner.run(job, records)
        self.assertEqual(len(result.records), 0)
        self.assertEqual(result.records_quarantined, 2)
        metrics = result.stage_metrics["1:drop_if_field_empty"]
        self.assertEqual(metrics["errors"], 2)

    def test_analytics_stage_keeps_records_and_emits_artifacts(self):
        job = ScrapeJob(
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
            processing=ProcessingConfig(
                enabled=True,
                schema_version="1.0",
                stages=[
                    ProcessingStage(
                        plugin="field_coverage_analytics",
                        stage_type="analytics",
                        on_error="fail",
                        config={"fields": ["name"]},
                    )
                ],
            ),
        )
        records = [
            _record(1, "https://example.com/1", {"name": "one"}),
            _record(2, "https://example.com/2", {"name": ""}),
        ]

        result = self.runner.run(job, records)
        self.assertEqual(len(result.records), 2)
        artifacts = result.artifacts["1:field_coverage_analytics"]
        self.assertEqual(artifacts["total_records"], 2)
        self.assertEqual(artifacts["field_coverage"]["name"]["present"], 1)


class TestEngineWithProcessing(unittest.TestCase):
    def test_engine_runs_processing_when_enabled(self):
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
        parser.parse_cards.return_value = [object(), object()]
        parser.next_request.return_value = None
        adapter.extract_source_url.side_effect = [
            "https://example.com/item/1",
            "https://example.com/item/2",
        ]
        adapter.extract_field.side_effect = ["", "keep-me"]
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
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
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
        )

        report = engine.run(job)
        self.assertEqual(report.records_emitted, 1)
        self.assertEqual(report.records_quarantined, 0)
        self.assertIn("1:drop_if_field_empty", report.processing_stage_metrics)

        sink.write.assert_called_once()
        _, written_records = sink.write.call_args.args
        self.assertEqual(len(written_records), 1)
        self.assertEqual(written_records[0].fields["name"], "keep-me")

    def test_engine_skips_processing_when_disabled(self):
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
        parser.parse_cards.return_value = [object()]
        parser.next_request.return_value = None
        adapter.extract_source_url.return_value = "https://example.com/item/1"
        adapter.extract_field.return_value = ""
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
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
            delay_ms=0,
            max_pages=1,
            required_fields={"source_url"},
            field_schema=["name"],
            processing=ProcessingConfig(enabled=False),
        )

        report = engine.run(job)
        self.assertEqual(report.records_emitted, 1)
        self.assertEqual(report.records_quarantined, 0)
        self.assertEqual(report.processing_stage_metrics, {})

        sink.write.assert_called_once()
        _, written_records = sink.write.call_args.args
        self.assertEqual(len(written_records), 1)
        self.assertEqual(written_records[0].fields["name"], "")


if __name__ == "__main__":
    unittest.main()

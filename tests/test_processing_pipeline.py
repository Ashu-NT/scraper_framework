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

    def test_score_lead_fit_plugin_adds_weighted_score(self):
        job = ScrapeJob(
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
            processing=ProcessingConfig(
                enabled=True,
                schema_version="1.0",
                stages=[
                    ProcessingStage(
                        plugin="score_lead_fit",
                        stage_type="record",
                        on_error="fail",
                        config={
                            "weights": {"budget": 0.01},
                            "presence_weights": {"phone": 5},
                            "output_field": "fit_score",
                            "round_digits": 2,
                        },
                    )
                ],
            ),
        )
        records = [
            _record(1, "https://example.com/1", {"budget": "1000", "phone": "123"}),
            _record(2, "https://example.com/2", {"budget": "500", "phone": ""}),
        ]

        result = self.runner.run(job, records)
        self.assertEqual(len(result.records), 2)
        self.assertEqual(result.records[0].fields["fit_score"], 15.0)
        self.assertEqual(result.records[1].fields["fit_score"], 5.0)

    def test_top_n_per_segment_keeps_best_scored_per_group(self):
        job = ScrapeJob(
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
            processing=ProcessingConfig(
                enabled=True,
                schema_version="1.0",
                stages=[
                    ProcessingStage(
                        plugin="top_n_per_segment",
                        stage_type="batch",
                        on_error="fail",
                        config={
                            "segment_field": "category",
                            "score_field": "lead_score",
                            "top_n": 1,
                        },
                    )
                ],
            ),
        )
        records = [
            _record(1, "https://example.com/1", {"category": "A", "lead_score": 10}),
            _record(2, "https://example.com/2", {"category": "A", "lead_score": 7}),
            _record(3, "https://example.com/3", {"category": "B", "lead_score": 3}),
            _record(4, "https://example.com/4", {"category": "B", "lead_score": 9}),
        ]

        result = self.runner.run(job, records)
        self.assertEqual(len(result.records), 2)
        self.assertEqual(result.records[0].source_url, "https://example.com/1")
        self.assertEqual(result.records[1].source_url, "https://example.com/4")

        artifacts = result.artifacts["1:top_n_per_segment"]
        self.assertEqual(artifacts["total_input"], 4)
        self.assertEqual(artifacts["total_output"], 2)
        self.assertEqual(artifacts["selected_per_segment"]["A"], 1)
        self.assertEqual(artifacts["selected_per_segment"]["B"], 1)

    def test_normalize_upwork_budget_parses_hourly_range(self):
        job = ScrapeJob(
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
            processing=ProcessingConfig(
                enabled=True,
                schema_version="1.0",
                stages=[
                    ProcessingStage(
                        plugin="normalize_upwork_budget",
                        stage_type="record",
                        on_error="fail",
                        config={"input_field": "budget", "hourly_to_usd_hours": 160},
                    )
                ],
            ),
        )
        records = [
            _record(1, "https://example.com/1", {"budget": "$20 - $40/hr"}),
        ]

        result = self.runner.run(job, records)
        fields = result.records[0].fields
        self.assertEqual(fields["budget_type"], "hourly")
        self.assertEqual(fields["budget_min"], 20.0)
        self.assertEqual(fields["budget_max"], 40.0)
        self.assertEqual(fields["budget_usd_est"], 4800.0)

    def test_normalize_upwork_age_parses_relative_age(self):
        job = ScrapeJob(
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
            processing=ProcessingConfig(
                enabled=True,
                schema_version="1.0",
                stages=[
                    ProcessingStage(
                        plugin="normalize_upwork_age",
                        stage_type="record",
                        on_error="fail",
                        config={"input_field": "posted_ago"},
                    )
                ],
            ),
        )
        records = [
            _record(1, "https://example.com/1", {"posted_ago": "2 hours ago"}),
        ]

        result = self.runner.run(job, records)
        fields = result.records[0].fields
        self.assertEqual(fields["post_age_hours"], 2.0)
        self.assertEqual(fields["post_age_bucket"], "recent")

    def test_client_quality_score_generates_score_and_tier(self):
        job = ScrapeJob(
            id="job",
            name="job",
            start=RequestSpec(url="https://example.com"),
            processing=ProcessingConfig(
                enabled=True,
                schema_version="1.0",
                stages=[
                    ProcessingStage(
                        plugin="client_quality_score",
                        stage_type="record",
                        on_error="fail",
                        config={},
                    )
                ],
            ),
        )
        records = [
            _record(
                1,
                "https://example.com/1",
                {
                    "payment_verified": True,
                    "hire_rate": "80%",
                    "total_spent": "$50000",
                    "avg_hourly_rate": "40",
                    "reviews": "20",
                    "jobs_posted": "50",
                },
            ),
        ]

        result = self.runner.run(job, records)
        fields = result.records[0].fields
        self.assertIn("client_quality_score", fields)
        self.assertIn("client_quality_tier", fields)
        self.assertGreater(fields["client_quality_score"], 50.0)


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

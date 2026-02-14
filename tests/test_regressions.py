"""
Regression tests for previously identified behavior issues.
"""

import unittest
from unittest.mock import Mock

from src.scraper_framework.core.engine import ScrapeEngine
from src.scraper_framework.core.models import Page, RequestSpec, ScrapeJob, ValidationResult
from src.scraper_framework.sinks.gsheet_sink import GoogleSheetsSink
from src.scraper_framework.transform.dedupe import UrlDedupeStrategy


class TestGoogleSheetsHeaderSafety(unittest.TestCase):
    """Ensure Google Sheets header handling is non-destructive."""

    def test_header_mismatch_raises_without_clearing(self):
        sink = GoogleSheetsSink()
        ws = Mock()
        ws.row_values.return_value = ["id", "source_url", "old_col"]

        expected_header = ["id", "source_url", "scraped_at_utc", "name"]

        with self.assertRaisesRegex(ValueError, "header mismatch"):
            sink._ensure_header(ws, expected_header)

        ws.clear.assert_not_called()
        ws.append_row.assert_not_called()
        ws.update.assert_not_called()

    def test_empty_header_row_writes_header_to_a1(self):
        sink = GoogleSheetsSink()
        ws = Mock()
        ws.row_values.return_value = []

        expected_header = ["id", "source_url", "scraped_at_utc", "name"]
        sink._ensure_header(ws, expected_header)

        ws.update.assert_called_once_with("A1", [expected_header], value_input_option="USER_ENTERED")


class TestEngineMetricsAfterDedupe(unittest.TestCase):
    """Ensure records_emitted reflects rows written after dedupe."""

    def test_records_emitted_tracks_post_dedupe_count(self):
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

        card_1 = object()
        card_2 = object()
        parser.parse_cards.return_value = [card_1, card_2]
        parser.next_request.return_value = None

        adapter.extract_source_url.side_effect = [
            "https://example.com/item/1",
            "https://example.com/item/1",
        ]
        adapter.extract_field.return_value = "Example item"

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
            id="job-1",
            name="metrics-regression",
            start=RequestSpec(url="https://example.com"),
            max_pages=1,
            delay_ms=0,
            field_schema=["name"],
            required_fields={"name", "source_url"},
        )

        report = engine.run(job)

        self.assertEqual(report.records_emitted, 1)
        sink.write.assert_called_once()

        _, written_records = sink.write.call_args.args
        self.assertEqual(len(written_records), 1)


if __name__ == "__main__":
    unittest.main()

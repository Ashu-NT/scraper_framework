import os
import tempfile
import unittest
from unittest.mock import Mock

from src.scraper_framework.core.engine import ScrapeEngine
from src.scraper_framework.core.models import (
    IncrementalConfig,
    Page,
    RequestSpec,
    ScrapeJob,
    ValidationResult,
)
from src.scraper_framework.state.sqlite_store import SQLiteIncrementalStateStore
from src.scraper_framework.transform.dedupe import UrlDedupeStrategy


class TestIncrementalEngine(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmp_dir, "state.db")

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_changed_only_skips_unchanged_across_runs(self):
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
        adapter.extract_field.return_value = "same-name"
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
            state_store=SQLiteIncrementalStateStore(self.state_path),
        )

        job = ScrapeJob(
            id="incremental-job",
            name="incremental-job",
            start=RequestSpec(url="https://example.com"),
            execution_mode="memory",
            delay_ms=0,
            max_pages=1,
            required_fields={"source_url"},
            field_schema=["name"],
            incremental=IncrementalConfig(
                enabled=True,
                backend="sqlite",
                state_path=self.state_path,
                mode="changed_only",
                resume=True,
                checkpoint_every_pages=1,
            ),
            sink_config={"type": "jsonl", "path": "unused.jsonl", "write_mode": "overwrite"},
        )

        report_1 = engine.run(job)
        report_2 = engine.run(job)

        adapter.extract_field.return_value = "changed-name"
        report_3 = engine.run(job)

        self.assertEqual(report_1.records_emitted, 1)
        self.assertEqual(report_2.records_emitted, 0)
        self.assertEqual(report_2.records_skipped_incremental, 1)
        self.assertEqual(report_3.records_emitted, 1)

        written_counts = [len(call.args[1]) for call in sink.write.call_args_list]
        self.assertEqual(written_counts, [1, 0, 1])


if __name__ == "__main__":
    unittest.main()

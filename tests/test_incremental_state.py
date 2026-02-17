import os
import tempfile
import unittest

from src.scraper_framework.state.sqlite_store import SQLiteIncrementalStateStore


class TestSQLiteIncrementalStateStore(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, "state.db")
        self.store = SQLiteIncrementalStateStore(self.db_path)

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_decide_and_touch_changed_only(self):
        decision_1 = self.store.decide_and_touch(
            job_id="job-a",
            dedupe_key="https://example.com/1",
            content_hash="h1",
            mode="changed_only",
        )
        self.assertTrue(decision_1.emit)
        self.assertTrue(decision_1.is_new)
        self.assertTrue(decision_1.changed)

        decision_2 = self.store.decide_and_touch(
            job_id="job-a",
            dedupe_key="https://example.com/1",
            content_hash="h1",
            mode="changed_only",
        )
        self.assertFalse(decision_2.emit)
        self.assertFalse(decision_2.is_new)
        self.assertFalse(decision_2.changed)

        decision_3 = self.store.decide_and_touch(
            job_id="job-a",
            dedupe_key="https://example.com/1",
            content_hash="h2",
            mode="changed_only",
        )
        self.assertTrue(decision_3.emit)
        self.assertFalse(decision_3.is_new)
        self.assertTrue(decision_3.changed)

    def test_checkpoint_roundtrip(self):
        payload = {
            "url": "https://example.com/page-2",
            "method": "GET",
            "headers": {"X-Test": "1"},
            "params": {"page": 2},
            "body": None,
        }
        self.store.save_checkpoint("job-b", payload, page_index=2, status="in_progress")
        checkpoint = self.store.load_checkpoint("job-b")
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint.page_index, 2)
        self.assertEqual(checkpoint.status, "in_progress")
        self.assertEqual(checkpoint.request_payload["url"], "https://example.com/page-2")

        self.store.clear_checkpoint("job-b")
        self.assertIsNone(self.store.load_checkpoint("job-b"))

    def test_run_counter(self):
        first = self.store.mark_run_started("job-c")
        second = self.store.mark_run_started("job-c")
        self.assertEqual(first, 1)
        self.assertEqual(second, 2)
        self.store.mark_run_completed("job-c")


if __name__ == "__main__":
    unittest.main()

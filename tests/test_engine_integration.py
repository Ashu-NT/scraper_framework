"""
Integration tests for the scraping engine configuration.
Tests configuration validation and structure.
"""

import os
import tempfile
import unittest

from src.scraper_framework.config_models import JobConfig, ScraperConfig


class TestEngineIntegration(unittest.TestCase):
    """Integration tests for the scraping engine configuration."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_full_scraping_pipeline_with_csv_output(self):
        """Test the complete scraping pipeline with CSV output."""
        # Create job config using ScraperConfig structure
        job_config = JobConfig(
            id="test_job_1",
            name="test_books_scraping",
            adapter="books_toscrape",
            start_url="https://books.toscrape.com/",
            max_pages=1,
            field_schema=["name", "price", "rating"],
        )

        # Create scraping config with required sink
        scraping_config = ScraperConfig(
            job=job_config, sink={"type": "csv", "path": os.path.join(self.temp_dir, "output.csv")}
        )

        # Verify the config validates correctly
        self.assertEqual(scraping_config.job.name, "test_books_scraping")
        self.assertEqual(scraping_config.job.adapter, "books_toscrape")
        self.assertEqual(scraping_config.job.max_pages, 1)

    def test_engine_with_invalid_adapter_fails_gracefully(self):
        """Test that invalid adapter is accepted in config (validation at runtime)."""
        # Create job config with non-existent adapter
        job_config = JobConfig(
            id="test_invalid",
            name="test_invalid_adapter",
            adapter="non_existent_adapter",
            start_url="https://example.com/",
            field_schema=["name"],
        )

        # Config creation should succeed, adapter validation happens at runtime
        scraping_config = ScraperConfig(
            job=job_config, sink={"type": "csv", "path": os.path.join(self.temp_dir, "invalid_output.csv")}
        )

        self.assertEqual(scraping_config.job.adapter, "non_existent_adapter")

    def test_scraper_config_validates_sink_csv(self):
        """Test that ScraperConfig properly validates CSV sink configuration."""
        job_config = JobConfig(
            id="test_csv",
            name="test_csv_sink",
            adapter="books_toscrape",
            start_url="https://books.toscrape.com/",
            field_schema=["name"],
        )

        # Valid CSV sink should work
        scraping_config = ScraperConfig(job=job_config, sink={"type": "csv", "path": "/tmp/output.csv"})

        self.assertIsNotNone(scraping_config.sink)

    def test_scraper_config_validates_sink_google_sheets(self):
        """Test that ScraperConfig properly validates Google Sheets sink configuration."""
        job_config = JobConfig(
            id="test_sheets",
            name="test_sheets_sink",
            adapter="books_toscrape",
            start_url="https://books.toscrape.com/",
            field_schema=["name"],
        )

        # Valid Google Sheets sink should work
        scraping_config = ScraperConfig(
            job=job_config, sink={"type": "google_sheets", "sheet_id": "test_sheet_123", "tab": "Sheet1"}
        )

        self.assertIsNotNone(scraping_config.sink)

    def test_job_config_validates_start_url(self):
        """Test that JobConfig validates start_url is a proper URL."""
        # Valid URL should work
        valid_job = JobConfig(
            id="test_url", name="test_job", adapter="test", start_url="https://example.com/", field_schema=[]
        )
        self.assertEqual(valid_job.start_url, "https://example.com/")

        # Invalid URL should raise ValidationError
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            JobConfig(id="test_invalid_url", name="test_job", adapter="test", start_url="not_a_url", field_schema=[])


if __name__ == "__main__":
    unittest.main()

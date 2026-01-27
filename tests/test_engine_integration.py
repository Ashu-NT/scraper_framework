"""
Integration tests for the scraping engine.
Tests the full pipeline with mocked HTTP client.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
from pathlib import Path

from src.scraper_framework.core.engine import ScrapingEngine
from src.scraper_framework.core.models import ScrapingConfig, JobConfig
from src.scraper_framework.http.client import HttpClient
from src.scraper_framework.http.response import HttpResponse
from src.scraper_framework.sinks.csv_sink import CsvSink
from src.scraper_framework.adapters.sites.books_toscrape import BooksToScrapeAdapter


class TestEngineIntegration(unittest.TestCase):
    """Integration tests for the scraping engine."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test outputs
        self.temp_dir = tempfile.mkdtemp()

        # Sample HTML for books.toscrape.com
        self.sample_html = """
        <html>
        <body>
            <div class="product_pod">
                <h3><a href="/book1.html" title="Book One">Book One</a></h3>
                <div class="product_price">
                    <p class="price_color">£10.99</p>
                </div>
                <p class="star-rating Three">Three stars</p>
            </div>
            <div class="product_pod">
                <h3><a href="/book2.html" title="Book Two">Book Two</a></h3>
                <div class="product_price">
                    <p class="price_color">£15.50</p>
                </div>
                <p class="star-rating Four">Four stars</p>
            </div>
        </body>
        </html>
        """

        # Create mock response
        self.mock_response = HttpResponse(
            url="https://books.toscrape.com/",
            status_code=200,
            content=self.sample_html,
            headers={"content-type": "text/html"}
        )

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch('src.scraper_framework.http.client.HttpClient.get')
    def test_full_scraping_pipeline_with_csv_output(self, mock_get):
        """Test the complete scraping pipeline with CSV output."""
        # Setup mock HTTP client
        mock_get.return_value = self.mock_response

        # Create job config
        job_config = JobConfig(
            name="test_books_scraping",
            adapter="books_toscrape",
            source_url="https://books.toscrape.com/",
            output_path=os.path.join(self.temp_dir, "output.csv"),
            output_format="csv",
            fields=["name", "price", "rating"]
        )

        # Create scraping config
        scraping_config = ScrapingConfig(
            jobs=[job_config]
        )

        # Create engine
        engine = ScrapingEngine(scraping_config)

        # Run the scraping
        results = engine.run()

        # Verify results
        self.assertEqual(len(results), 1)
        self.assertIn("test_books_scraping", results)

        job_result = results["test_books_scraping"]
        self.assertTrue(job_result.success)
        self.assertEqual(job_result.records_processed, 2)

        # Verify CSV file was created and has correct content
        csv_path = Path(self.temp_dir) / "output.csv"
        self.assertTrue(csv_path.exists())

        with open(csv_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Should have header + 2 data rows
        self.assertEqual(len(lines), 3)

        # Check header
        self.assertIn("name", lines[0])
        self.assertIn("price", lines[0])
        self.assertIn("rating", lines[0])

        # Check data rows contain expected content
        data_content = "".join(lines[1:])
        self.assertIn("Book One", data_content)
        self.assertIn("£10.99", data_content)
        self.assertIn("Three", data_content)
        self.assertIn("Book Two", data_content)
        self.assertIn("£15.50", data_content)
        self.assertIn("Four", data_content)

    @patch('src.scraper_framework.http.client.HttpClient.get')
    def test_engine_handles_http_errors_gracefully(self, mock_get):
        """Test that engine handles HTTP errors gracefully."""
        # Setup mock to raise an exception
        mock_get.side_effect = Exception("Connection failed")

        # Create job config
        job_config = JobConfig(
            name="test_error_handling",
            adapter="books_toscrape",
            source_url="https://books.toscrape.com/",
            output_path=os.path.join(self.temp_dir, "error_output.csv"),
            output_format="csv",
            fields=["name"]
        )

        scraping_config = ScrapingConfig(jobs=[job_config])
        engine = ScrapingEngine(scraping_config)

        # Run the scraping
        results = engine.run()

        # Verify error was handled
        self.assertEqual(len(results), 1)
        self.assertIn("test_error_handling", results)

        job_result = results["test_error_handling"]
        self.assertFalse(job_result.success)
        self.assertIn("Connection failed", job_result.error_message)

    @patch('src.scraper_framework.http.client.HttpClient.get')
    def test_engine_with_invalid_adapter_fails_gracefully(self, mock_get):
        """Test that engine fails gracefully with invalid adapter."""
        # Create job config with non-existent adapter
        job_config = JobConfig(
            name="test_invalid_adapter",
            adapter="non_existent_adapter",
            source_url="https://example.com/",
            output_path=os.path.join(self.temp_dir, "invalid_output.csv"),
            output_format="csv",
            fields=["name"]
        )

        scraping_config = ScrapingConfig(jobs=[job_config])
        engine = ScrapingEngine(scraping_config)

        # Run the scraping
        results = engine.run()

        # Verify error was handled
        self.assertEqual(len(results), 1)
        self.assertIn("test_invalid_adapter", results)

        job_result = results["test_invalid_adapter"]
        self.assertFalse(job_result.success)
        self.assertIn("Adapter 'non_existent_adapter' not found", job_result.error_message)

    @patch('src.scraper_framework.http.client.HttpClient.get')
    def test_engine_processes_multiple_jobs(self, mock_get):
        """Test that engine can process multiple jobs."""
        # Setup different responses for different URLs
        def mock_get_side_effect(url):
            if "books.toscrape.com" in url:
                return self.mock_response
            elif "example.com" in url:
                return HttpResponse(
                    url="https://example.com/",
                    status_code=200,
                    content="<html><body><h1>Example</h1></body></html>",
                    headers={"content-type": "text/html"}
                )
            else:
                raise Exception("Unexpected URL")

        mock_get.side_effect = mock_get_side_effect

        # Create multiple job configs
        job1_config = JobConfig(
            name="job1_books",
            adapter="books_toscrape",
            source_url="https://books.toscrape.com/",
            output_path=os.path.join(self.temp_dir, "job1_output.csv"),
            output_format="csv",
            fields=["name", "price"]
        )

        job2_config = JobConfig(
            name="job2_example",
            adapter="books_toscrape",  # Using same adapter for simplicity
            source_url="https://example.com/",
            output_path=os.path.join(self.temp_dir, "job2_output.csv"),
            output_format="csv",
            fields=["name"]
        )

        scraping_config = ScrapingConfig(jobs=[job1_config, job2_config])
        engine = ScrapingEngine(scraping_config)

        # Run the scraping
        results = engine.run()

        # Verify both jobs ran
        self.assertEqual(len(results), 2)
        self.assertIn("job1_books", results)
        self.assertIn("job2_example", results)

        # Both should succeed (job2 will have 0 records since adapter doesn't match content)
        self.assertTrue(results["job1_books"].success)
        self.assertTrue(results["job2_example"].success)

    def test_engine_validates_config_before_running(self):
        """Test that engine validates config before attempting to run jobs."""
        # Create invalid config (missing required fields)
        invalid_config = ScrapingConfig(jobs=[])

        with self.assertRaises(ValueError):
            ScrapingEngine(invalid_config)


if __name__ == '__main__':
    unittest.main()
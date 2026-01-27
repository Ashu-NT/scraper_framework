"""
Unit tests for core scraper components.
Fast, focused tests for individual components.
"""

import unittest
from unittest.mock import Mock, patch
from src.scraper_framework.core.models import Record, ValidationResult
from src.scraper_framework.transform.validators import RequiredFieldsValidator
from src.scraper_framework.transform.dedupe import DedupeBySourceUrl, DedupeByHash
from src.scraper_framework.config_models import ScraperConfig, CsvSinkConfig, GoogleSheetsSinkConfig
from src.scraper_framework.adapters.registry import register, get, get_registered_adapters


class TestValidators(unittest.TestCase):
    """Test record validation logic."""

    def setUp(self):
        self.validator = RequiredFieldsValidator()

    def test_validate_complete_record(self):
        """Test validation of a complete record."""
        record = Record(
            id="1",
            source_url="https://example.com/1",
            scraped_at_utc="2024-01-01T00:00:00Z",
            fields={"name": "Test", "price": "$10.99"}
        )
        required_fields = {"name", "source_url"}
        result = self.validator.validate(record, required_fields)
        self.assertTrue(result.ok)

    def test_validate_missing_required_field(self):
        """Test validation fails when required field is missing."""
        record = Record(
            id="1",
            source_url="https://example.com/1",
            scraped_at_utc="2024-01-01T00:00:00Z",
            fields={"name": "Test"}  # missing price
        )
        required_fields = {"name", "price"}
        result = self.validator.validate(record, required_fields)
        self.assertFalse(result.ok)
        self.assertIn("price", result.reason)

    def test_validate_empty_required_field(self):
        """Test validation fails when required field is empty."""
        record = Record(
            id="1",
            source_url="https://example.com/1",
            scraped_at_utc="2024-01-01T00:00:00Z",
            fields={"name": "", "price": "$10.99"}
        )
        required_fields = {"name", "price"}
        result = self.validator.validate(record, required_fields)
        self.assertFalse(result.ok)
        self.assertIn("name", result.reason)


class TestDedupeStrategies(unittest.TestCase):
    """Test deduplication strategies."""

    def setUp(self):
        self.url_deduper = DedupeBySourceUrl()
        self.hash_deduper = DedupeByHash()

    def test_dedupe_by_url_unique(self):
        """Test URL deduplication with unique URLs."""
        record1 = Record(id="1", source_url="https://example.com/1", scraped_at_utc="t", fields={})
        record2 = Record(id="2", source_url="https://example.com/2", scraped_at_utc="t", fields={})

        self.assertTrue(self.url_deduper.should_keep(record1))
        self.assertTrue(self.url_deduper.should_keep(record2))

    def test_dedupe_by_url_duplicate(self):
        """Test URL deduplication with duplicate URLs."""
        record1 = Record(id="1", source_url="https://example.com/1", scraped_at_utc="t", fields={})
        record2 = Record(id="2", source_url="https://example.com/1", scraped_at_utc="t", fields={})  # duplicate URL

        self.assertTrue(self.url_deduper.should_keep(record1))
        self.assertFalse(self.url_deduper.should_keep(record2))

    def test_dedupe_by_hash_unique(self):
        """Test hash deduplication with unique content."""
        record1 = Record(id="1", source_url="u", scraped_at_utc="t", fields={"name": "A", "price": "10"})
        record2 = Record(id="2", source_url="u", scraped_at_utc="t", fields={"name": "B", "price": "20"})

        self.assertTrue(self.hash_deduper.should_keep(record1))
        self.assertTrue(self.hash_deduper.should_keep(record2))

    def test_dedupe_by_hash_duplicate(self):
        """Test hash deduplication with duplicate content."""
        record1 = Record(id="1", source_url="u", scraped_at_utc="t", fields={"name": "A", "price": "10"})
        record2 = Record(id="2", source_url="u", scraped_at_utc="t", fields={"name": "A", "price": "10"})  # duplicate content

        self.assertTrue(self.hash_deduper.should_keep(record1))
        self.assertFalse(self.hash_deduper.should_keep(record2))


class TestConfigValidation(unittest.TestCase):
    """Test Pydantic configuration validation."""

    def test_valid_csv_config(self):
        """Test valid CSV configuration."""
        config_data = {
            "job": {
                "id": "test",
                "name": "Test Job",
                "adapter": "test_adapter",
                "start_url": "https://example.com"
            },
            "sink": {
                "type": "csv",
                "path": "test.csv"
            }
        }
        config = ScraperConfig(**config_data)
        self.assertEqual(config.sink.type, "csv")
        self.assertEqual(config.sink.path, "test.csv")

    def test_valid_google_sheets_config(self):
        """Test valid Google Sheets configuration."""
        config_data = {
            "job": {
                "id": "test",
                "name": "Test Job",
                "adapter": "test_adapter",
                "start_url": "https://example.com"
            },
            "sink": {
                "type": "google_sheets",
                "sheet_id": "123",
                "tab": "Sheet1"
            }
        }
        config = ScraperConfig(**config_data)
        self.assertEqual(config.sink.type, "google_sheets")
        self.assertEqual(config.sink.sheet_id, "123")

    def test_invalid_sink_type(self):
        """Test invalid sink type."""
        config_data = {
            "job": {
                "id": "test",
                "name": "Test Job",
                "adapter": "test_adapter",
                "start_url": "https://example.com"
            },
            "sink": {
                "type": "invalid_type",
                "path": "test.csv"
            }
        }
        with self.assertRaises(ValueError) as cm:
            ScraperConfig(**config_data)
        self.assertIn("Unknown sink type", str(cm.exception))

    def test_enrich_fields_not_in_schema(self):
        """Test enrich fields validation against field schema."""
        config_data = {
            "job": {
                "id": "test",
                "name": "Test Job",
                "adapter": "test_adapter",
                "start_url": "https://example.com",
                "field_schema": ["name", "price"]
            },
            "sink": {
                "type": "csv",
                "path": "test.csv"
            },
            "enrich": {
                "enabled": True,
                "fields": ["phone", "website"]  # Not in field_schema
            }
        }
        with self.assertRaises(ValueError) as cm:
            ScraperConfig(**config_data)
        self.assertIn("must be declared in job.field_schema", str(cm.exception))


class TestAdapterRegistry(unittest.TestCase):
    """Test adapter registry functionality."""

    def setUp(self):
        # Clear any existing registrations for testing
        from src.scraper_framework.adapters import registry
        registry._ADAPTERS.clear()

    def test_register_and_get_adapter(self):
        """Test registering and retrieving an adapter."""
        mock_adapter = Mock()
        mock_adapter.key.return_value = "test_adapter"

        register(mock_adapter)
        retrieved = get("test_adapter")
        self.assertEqual(retrieved, mock_adapter)

    def test_register_duplicate_adapter(self):
        """Test registering adapters with duplicate keys."""
        mock_adapter1 = Mock()
        mock_adapter1.key.return_value = "duplicate_key"

        mock_adapter2 = Mock()
        mock_adapter2.key.return_value = "duplicate_key"

        register(mock_adapter1)
        # Should overwrite without error (current implementation)
        register(mock_adapter2)
        retrieved = get("duplicate_key")
        self.assertEqual(retrieved, mock_adapter2)

    def test_get_nonexistent_adapter(self):
        """Test getting a non-existent adapter."""
        with self.assertRaises(KeyError):
            get("nonexistent")

    def test_get_registered_adapters(self):
        """Test getting all registered adapters."""
        mock_adapter1 = Mock()
        mock_adapter1.key.return_value = "adapter1"
        mock_adapter2 = Mock()
        mock_adapter2.key.return_value = "adapter2"

        register(mock_adapter1)
        register(mock_adapter2)

        adapters = get_registered_adapters()
        self.assertEqual(len(adapters), 2)
        self.assertIn(mock_adapter1, adapters)
        self.assertIn(mock_adapter2, adapters)


if __name__ == '__main__':
    unittest.main()
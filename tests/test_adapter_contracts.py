"""
Contract tests for site adapters.
Ensures all adapters satisfy basic expectations.
"""

import unittest
from src.scraper_framework.adapters.registry import get_registered_adapters
from src.scraper_framework.adapters.sites import register_all


class TestAdapterContracts(unittest.TestCase):
    """Test that all adapters satisfy their contracts."""

    def setUp(self):
        """Set up test fixtures."""
        register_all()  # Register all adapters before testing

    def test_all_adapters_have_unique_keys(self):
        """Test that all registered adapters have unique, non-empty keys."""
        adapters = get_registered_adapters()
        keys = []

        for adapter in adapters:
            # Key must be non-empty
            key = adapter.key()
            self.assertIsInstance(key, str)
            self.assertTrue(len(key.strip()) > 0, f"Adapter {adapter.__class__.__name__} has empty key")

            # Key must be unique
            self.assertNotIn(key, keys, f"Duplicate adapter key: {key}")
            keys.append(key)

    def test_all_adapters_have_valid_modes(self):
        """Test that all adapters have valid scraping modes."""
        adapters = get_registered_adapters()
        valid_modes = {"STATIC_HTML", "JSON_API"}

        for adapter in adapters:
            mode = adapter.mode()
            self.assertIn(mode, valid_modes,
                         f"Adapter {adapter.key()} has invalid mode: {mode}")

    def test_static_adapters_have_card_locator(self):
        """Test that STATIC_HTML adapters have non-empty card locators."""
        adapters = get_registered_adapters()

        for adapter in adapters:
            if adapter.mode() == "STATIC_HTML":
                card_locator = adapter.card_locator()
                self.assertIsInstance(card_locator, str)
                self.assertTrue(len(card_locator.strip()) > 0,
                               f"Adapter {adapter.key()} has empty card_locator")

    def test_adapters_have_field_locators_for_known_fields(self):
        """Test that adapters return field locators for their known fields."""
        adapters = get_registered_adapters()
        test_fields = ["name", "price", "rating", "phone", "website", "address"]

        for adapter in adapters:
            adapter_key = adapter.key()

            for field in test_fields:
                locator = adapter.field_locator(field)
                # Field locator can be None (field not supported) or a string
                if locator is not None:
                    self.assertIsInstance(locator, str,
                                         f"Adapter {adapter_key} field_locator({field}) returned non-string: {type(locator)}")

    def test_adapters_have_extract_source_url_method(self):
        """Test that adapters have the extract_source_url method."""
        adapters = get_registered_adapters()

        for adapter in adapters:
            # Method should exist
            self.assertTrue(hasattr(adapter, 'extract_source_url'),
                           f"Adapter {adapter.key()} missing extract_source_url method")

            # Method should be callable
            method = getattr(adapter, 'extract_source_url')
            self.assertTrue(callable(method),
                           f"Adapter {adapter.key()} extract_source_url is not callable")

    def test_adapter_keys_are_reasonable_length(self):
        """Test that adapter keys are reasonable length (not too long)."""
        adapters = get_registered_adapters()

        for adapter in adapters:
            key = adapter.key()
            self.assertLess(len(key), 50,
                           f"Adapter key too long: {key} ({len(key)} chars)")

    def test_adapter_keys_use_valid_characters(self):
        """Test that adapter keys use valid characters (alphanumeric, underscore, dash)."""
        adapters = get_registered_adapters()
        import re

        valid_pattern = re.compile(r'^[a-zA-Z0-9_-]+$')

        for adapter in adapters:
            key = adapter.key()
            self.assertRegex(key, valid_pattern,
                           f"Adapter key contains invalid characters: {key}")


if __name__ == '__main__':
    unittest.main()
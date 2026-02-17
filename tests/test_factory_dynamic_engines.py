import types
import unittest
from unittest.mock import Mock, patch

from src.scraper_framework.core.factory import ComponentFactory
from src.scraper_framework.core.models import RequestSpec, ScrapeJob


class _DynamicAdapter:
    def mode(self):
        return "DYNAMIC"


class TestFactoryDynamicEngines(unittest.TestCase):
    def test_fetcher_uses_playwright_client_when_configured(self):
        factory = ComponentFactory(http_timeout_s=10)
        adapter = _DynamicAdapter()
        job = ScrapeJob(
            id="job-playwright",
            name="job-playwright",
            start=RequestSpec(url="https://example.com"),
            dynamic_engine="playwright",
        )

        dummy_module = types.ModuleType("scraper_framework.http.playwright_client")

        class DummyPlaywrightClient:
            def __init__(self, timeout_s=30):
                self.timeout_s = timeout_s
                self.engine = "playwright"

        dummy_module.PlaywrightHttpClient = DummyPlaywrightClient

        with patch.dict("sys.modules", {"scraper_framework.http.playwright_client": dummy_module}):
            fetcher = factory._fetcher(job, Mock(), adapter)

        self.assertEqual(getattr(fetcher.client, "engine", ""), "playwright")

    def test_fetcher_uses_selenium_client_by_default(self):
        factory = ComponentFactory(http_timeout_s=10)
        adapter = _DynamicAdapter()
        job = ScrapeJob(
            id="job-selenium",
            name="job-selenium",
            start=RequestSpec(url="https://example.com"),
            dynamic_engine="selenium",
        )

        dummy_module = types.ModuleType("scraper_framework.http.selenium_client")

        class DummySeleniumClient:
            def __init__(self, timeout_s=30):
                self.timeout_s = timeout_s
                self.engine = "selenium"

        dummy_module.SeleniumHttpClient = DummySeleniumClient

        with patch.dict("sys.modules", {"scraper_framework.http.selenium_client": dummy_module}):
            fetcher = factory._fetcher(job, Mock(), adapter)

        self.assertEqual(getattr(fetcher.client, "engine", ""), "selenium")


if __name__ == "__main__":
    unittest.main()

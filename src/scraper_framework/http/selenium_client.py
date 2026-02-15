from __future__ import annotations

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
except Exception:
    webdriver = None  # type: ignore
    Options = None  # type: ignore
    Service = None  # type: ignore
    ChromeDriverManager = None  # type: ignore

from scraper_framework.core.models import RequestSpec
from scraper_framework.http.response import HttpResponse
from scraper_framework.http.selenium_steps import (
    ClickSelectorsStep,
    CookieConsentStep,
    RevealAndClickStep,
    ScrollStep,
    WaitForSelectorStep,
    WindowStep,
)
from scraper_framework.utils.logging import get_logger


class SeleniumHttpClient:
    """
    Key behavior:
      - First fetch navigates to URL
      - Later fetches (same URL) do NOT re-navigate; they scroll/click/wait
    """

    def __init__(self, headless: bool = True, timeout_s: int = 30, driver_path: str | None = None):
        if webdriver is None:
            raise RuntimeError("selenium not installed")

        self.log = get_logger("scraper_framework.http.selenium")
        self.timeout_s = timeout_s

        opts = Options()
        if headless:
            opts.add_argument("--headless=new")

        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-dev-shm-usage")

        try:
            service = Service(driver_path) if driver_path else Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=opts)
        except Exception:
            self.driver = webdriver.Chrome(options=opts)

        # Track navigation state
        self._current_url: str | None = None

        # Pipeline (order matters)
        # We wait AFTER scroll/click so the new cards have time to appear.
        self.steps = [
            WindowStep(),
            CookieConsentStep(),
            # one action step per iteration
            ClickSelectorsStep(),
            ScrollStep(),
            RevealAndClickStep(),  # custom step to handle "reveal on scroll" + click patterns
            # wait after action so DOM can update
            WaitForSelectorStep(),
        ]

    def send(self, req: RequestSpec) -> HttpResponse:
        params = req.params or {}

        # Navigate only if URL changed or first time
        if self._current_url != req.url:
            self.log.info("Selenium: navigating %s", req.url)
            self.driver.get(req.url)
            self._current_url = req.url

            # reset one-time flags per navigation
            params.pop("_window_applied", None)
            params.pop("_cookies_handled", None)

        # Run pipeline steps for this "page state"
        for step in self.steps:
            step.apply(self.driver, params, self.log, self.timeout_s)

        html = self.driver.page_source or ""
        return HttpResponse(status_code=200, headers={}, text=html, json=None)

    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass

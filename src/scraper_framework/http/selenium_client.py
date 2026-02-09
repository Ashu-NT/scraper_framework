from __future__ import annotations

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
except Exception:  # pragma: no cover - import errors surfaced at runtime
    webdriver = None  # type: ignore
    Options = None
    Service = None
    By = None
    WebDriverWait = None
    EC = None
    ChromeDriverManager = None

from scraper_framework.core.models import RequestSpec
from scraper_framework.http.response import HttpResponse
from scraper_framework.utils.logging import get_logger


class SeleniumHttpClient:
    """A lightweight Selenium-backed HttpClient.

    Notes:
    - Returns `HttpResponse` with `text` set to `driver.page_source`.
    - `status_code` and `headers` are best-effort (Selenium does not expose them reliably).
    - Supports optional request params:
        - `wait_selector`: CSS selector to wait for presence before reading page.
        - `wait_time`: seconds to wait (overrides client timeout for this request).
        - `click_selectors`: iterable of CSS selectors to click after load.
    """

    def __init__(self, headless: bool = True, timeout_s: int = 30, driver_path: str | None = None):
        if webdriver is None:
            raise RuntimeError("selenium or webdriver-manager is not installed")

        self.log = get_logger("scraper_framework.http.selenium")
        self.timeout_s = timeout_s

        opts = Options()
        
        # Anti-detection: disable webdriver flag and set realistic user agent
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
        
        if headless:
            try:
                ##opts.add_argument("--headless=new")
                pass
            except Exception:
                ##opts.add_argument("--headless")
                pass
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")

        # Create Chrome service either from provided path or webdriver-manager
        try:
            if driver_path:
                service = Service(driver_path)
            else:
                service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=opts)
        except Exception:
            # Fallback: try default constructor (requires chromedriver in PATH)
            self.log.warning("Failed to use webdriver-manager; falling back to system chromedriver")
            self.driver = webdriver.Chrome(options=opts)

        # Inject JavaScript to mask webdriver detection
        self.driver.execute_cdp_cmd(
            'Page.addScriptToEvaluateOnNewDocument',
            {'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'}
        )

        self.wait = WebDriverWait(self.driver, timeout_s)

    def send(self, req: RequestSpec) -> HttpResponse:
        url = req.url
        params = req.params or {}

        try:
            self.driver.get(url)

            # Optional: close common overlays/ads (banners, popups, modals)
            close_selectors = params.get("close_selectors") or [
                # Common close button selectors
                "button.close",
                "[aria-label='Close']",
                "[aria-label='close']",
                ".modal-close",
                ".popup-close",
                ".banner-close",
                ".ad-close",
                ".cookie-banner .close",
                "button[aria-label*='dismiss']",
                "button[aria-label*='close']",
            ]
            
            for sel in close_selectors:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    el.click()
                    self.log.debug("Closed overlay: %s", sel)
                except Exception:
                    pass  # Silently ignore if selector doesn't match

            # Optional wait for a selector
            wait_selector = params.get("wait_selector")
            wait_time = params.get("wait_time", self.timeout_s)
            if wait_selector:
                try:
                    WebDriverWait(self.driver, wait_time).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
                    )
                except Exception:
                    self.log.debug("wait_selector not found before timeout: %s", wait_selector)

            # Optional clicks (e.g., load-more buttons)
            click_selectors = params.get("click_selectors") or []
            for sel in click_selectors:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    el.click()
                except Exception:
                    self.log.debug("click selector failed: %s", sel)

            html = self.driver.page_source or ""

            return HttpResponse(status_code=200, headers={}, text=html, json=None)

        except Exception as e:
            self.log.exception("Selenium fetch failed for %s: %s", url, type(e).__name__)
            raise

    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass

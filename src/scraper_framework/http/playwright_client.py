from __future__ import annotations

import time

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except Exception:
    sync_playwright = None  # type: ignore
    PlaywrightTimeoutError = Exception  # type: ignore

from scraper_framework.core.models import RequestSpec
from scraper_framework.http.response import HttpResponse
from scraper_framework.utils.logging import get_logger


class PlaywrightHttpClient:
    """HTTP-like client backed by Playwright Chromium for dynamic pages."""

    def __init__(self, headless: bool = True, timeout_s: int = 30):
        if sync_playwright is None:
            raise RuntimeError("playwright not installed")

        self.log = get_logger("scraper_framework.http.playwright")
        self.timeout_s = timeout_s
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=headless)
        self._context = self._browser.new_context(viewport={"width": 1366, "height": 768})
        self._page = self._context.new_page()
        self._current_url: str | None = None

    def send(self, req: RequestSpec) -> HttpResponse:
        params = req.params or {}

        if self._current_url != req.url:
            self.log.info("Playwright: navigating %s", req.url)
            self._page.goto(req.url, wait_until="domcontentloaded", timeout=self._timeout_ms(self.timeout_s))
            self._current_url = req.url
            params.pop("_window_applied", None)
            params.pop("_cookies_handled", None)

        self._apply_window(params)
        self._apply_cookie_consent(params)
        self._apply_click_action(params)
        self._apply_scroll_action(params)
        self._apply_reveal_click(params)
        self._apply_wait_selector(params)

        html = self._page.content()
        return HttpResponse(status_code=200, headers={}, text=html, json=None)

    def close(self) -> None:
        for closer in (self._context, self._browser, self._playwright):
            try:
                closer.close()  # type: ignore[attr-defined]
            except Exception:
                pass

    def _apply_window(self, params: dict) -> None:
        if params.get("window_enabled") is False or params.get("_window_applied"):
            return

        try:
            ws = params.get("window_size")
            if isinstance(ws, str) and "," in ws:
                width_str, height_str = ws.split(",", 1)
                width, height = int(width_str), int(height_str)
            else:
                width, height = 1366, 768
            self._page.set_viewport_size({"width": width, "height": height})
            params["_window_applied"] = True
        except Exception:
            self.log.debug("Playwright window setup failed")

    def _apply_cookie_consent(self, params: dict) -> None:
        if params.get("cookies_enabled") is False or params.get("_cookies_handled"):
            return

        action = str(params.get("cookie_action", "auto")).lower()
        prefer_reject = action in {"auto", "reject"}
        timeout_ms = self._timeout_ms(float(params.get("cookie_timeout", 4)))

        css_reject = ["#onetrust-reject-all-handler"]
        css_accept = ["#onetrust-accept-btn-handler"]
        xp_reject = [
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'reject')]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'decline')]",
        ]
        xp_accept = [
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'accept')]",
            "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'agree')]",
        ]

        css_selectors = (css_reject + css_accept) if prefer_reject else (css_accept + css_reject)
        xp_selectors = (xp_reject + xp_accept) if prefer_reject else (xp_accept + xp_reject)

        clicked = False
        for selector in css_selectors:
            if self._click_selector(selector, timeout_ms=timeout_ms):
                clicked = True
                break

        if not clicked:
            for xpath_selector in xp_selectors:
                if self._click_xpath(xpath_selector, timeout_ms=timeout_ms):
                    break

        params["_cookies_handled"] = True

    def _apply_click_action(self, params: dict) -> None:
        if params.get("click_enabled") is False:
            return
        if params.get("click_action") != "once":
            return
        selector = params.get("click_selector")
        if not selector:
            return

        timeout_ms = self._timeout_ms(float(params.get("click_timeout", 4)))
        pause_s = float(params.get("click_pause", 0.8))
        if self._click_selector(selector, timeout_ms=timeout_ms):
            self.log.info("Playwright click action: clicked %s", selector)
            time.sleep(max(0.0, pause_s))
        else:
            self.log.info("Playwright click action: click failed %s", selector)

    def _apply_scroll_action(self, params: dict) -> None:
        if params.get("scroll_enabled") is False:
            return
        if params.get("scroll_action") != "down":
            return

        px = int(params.get("scroll_px", 450))
        pause_s = float(params.get("scroll_pause", 0.8))
        wait_selector = params.get("scroll_wait_increase_selector")
        prev_count = params.get("scroll_prev_count")
        wait_time_s = float(params.get("scroll_wait_time", 6))

        self._page.evaluate("window.scrollBy(0, arguments[0])", px)
        time.sleep(max(0.0, pause_s))

        if wait_selector and isinstance(prev_count, int):
            deadline = time.time() + max(0.0, wait_time_s)
            while time.time() < deadline:
                try:
                    now_count = self._page.locator(wait_selector).count()
                except Exception:
                    break
                if now_count > prev_count:
                    break
                time.sleep(max(0.05, pause_s))

    def _apply_reveal_click(self, params: dict) -> None:
        if params.get("reveal_enabled") is False:
            return

        reveal_selector = params.get("reveal_selector")
        click_selector = params.get("reveal_click_selector")
        if not reveal_selector:
            return

        max_scrolls = int(params.get("reveal_max_scrolls", 10))
        scroll_pause = float(params.get("reveal_scroll_pause", 0.6))
        click_pause = float(params.get("reveal_click_pause", 0.8))
        for _ in range(max_scrolls):
            try:
                target = self._page.locator(reveal_selector).first
                if target.count() > 0 and target.is_visible():
                    target.scroll_into_view_if_needed(timeout=self._timeout_ms(self.timeout_s))
                    if click_selector:
                        self._click_selector(click_selector, timeout_ms=self._timeout_ms(4))
                        time.sleep(max(0.0, click_pause))
                    return
            except Exception:
                pass

            self._page.evaluate("window.scrollBy(0, window.innerHeight * 0.7)")
            time.sleep(max(0.0, scroll_pause))

        self.log.info("Playwright reveal click: selector not revealed after %d scrolls", max_scrolls)

    def _apply_wait_selector(self, params: dict) -> None:
        if params.get("wait_enabled") is False:
            return
        selector = params.get("wait_selector")
        if not selector:
            return

        wait_s = float(params.get("wait_time", self.timeout_s))
        try:
            self._page.wait_for_selector(selector, timeout=self._timeout_ms(wait_s))
        except PlaywrightTimeoutError:
            self.log.debug("Playwright wait selector not found: %s", selector)

    def _click_selector(self, selector: str, timeout_ms: int) -> bool:
        try:
            loc = self._page.locator(selector).first
            loc.wait_for(state="visible", timeout=timeout_ms)
            loc.click(timeout=timeout_ms)
            return True
        except Exception:
            return False

    def _click_xpath(self, xpath_selector: str, timeout_ms: int) -> bool:
        try:
            loc = self._page.locator(f"xpath={xpath_selector}").first
            loc.wait_for(state="visible", timeout=timeout_ms)
            loc.click(timeout=timeout_ms)
            return True
        except Exception:
            return False

    def _timeout_ms(self, timeout_s: float) -> int:
        return int(max(0.0, float(timeout_s)) * 1000)

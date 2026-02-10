from __future__ import annotations

import time
from typing import Protocol, Any

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class SeleniumStep(Protocol):
    """Single Selenium behavior in the request pipeline."""
    name: str

    def apply(self, driver: Any, params: dict, log: Any, timeout_s: int) -> None: ...

# WINDOW / VIEWPORT

class WindowStep:
    name = "window"

    def __init__(self, default_size=(1366, 768), start_maximized=False):
        self.default_size = default_size
        self.start_maximized = start_maximized

    def apply(self, driver, params, log, timeout_s):
        if params.get("window_enabled") is False:
            return

        # Only apply once per navigation to avoid flicker
        if params.get("_window_applied"):
            return

        try:
            if params.get("start_maximized", self.start_maximized):
                driver.maximize_window()
            else:
                ws = params.get("window_size")
                if isinstance(ws, str) and "," in ws:
                    w, h = ws.split(",", 1)
                    driver.set_window_size(int(w), int(h))
                else:
                    w, h = self.default_size
                    driver.set_window_size(w, h)
            params["_window_applied"] = True
        except Exception:
            log.debug("WindowStep failed")

# COOKIE CONSENT
class CookieConsentStep:
    name = "cookies"

    def apply(self, driver, params, log, timeout_s):
        if params.get("cookies_enabled") is False:
            return

        # Only try once per navigation to avoid re-clicking
        if params.get("_cookies_handled"):
            return

        action = (params.get("cookie_action") or "auto").lower()
        prefer_reject = action in ("auto", "reject")
        timeout = float(params.get("cookie_timeout", 4))

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

        def click(by, sel):
            try:
                el = WebDriverWait(driver, timeout / 2).until(
                    EC.element_to_be_clickable((by, sel))
                )
                try:
                    el.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", el)
                return True
            except Exception:
                return False

        order = (
            (css_reject, css_accept, xp_reject, xp_accept)
            if prefer_reject
            else (css_accept, css_reject, xp_accept, xp_reject)
        )

        # main doc
        for sel in order[0] + order[1]:
            if click(By.CSS_SELECTOR, sel):
                params["_cookies_handled"] = True
                return

        for xp in order[2] + order[3]:
            if click(By.XPATH, xp):
                params["_cookies_handled"] = True
                return

        # iframe fallback
        try:
            frames = driver.find_elements(By.CSS_SELECTOR, "iframe")
        except Exception:
            frames = []

        for fr in frames[:10]:
            try:
                driver.switch_to.frame(fr)
                for sel in order[0] + order[1]:
                    if click(By.CSS_SELECTOR, sel):
                        driver.switch_to.default_content()
                        params["_cookies_handled"] = True
                        return
                for xp in order[2] + order[3]:
                    if click(By.XPATH, xp):
                        driver.switch_to.default_content()
                        params["_cookies_handled"] = True
                        return
                driver.switch_to.default_content()
            except Exception:
                try:
                    driver.switch_to.default_content()
                except Exception:
                    pass

        params["_cookies_handled"] = True

# WAIT FOR CONTENT
class WaitForSelectorStep:
    name = "wait"

    def apply(self, driver, params, log, timeout_s):
        if params.get("wait_enabled") is False:
            return

        selector = params.get("wait_selector")
        if not selector:
            return

        wait_time = float(params.get("wait_time", timeout_s))
        try:
            WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
        except Exception:
            log.debug("Wait selector not found: %s", selector)

# CLICK ELEMENTS
class ClickSelectorsStep:
    """
    One click action per request (pagination-style).
    Adapter sets params["click_action"]="once" and params["click_selector"]=...

    Params:
      - click_enabled: bool (default True)
      - click_action: "once" -> click once, otherwise no-op
      - click_selector: CSS selector to click
      - click_timeout: seconds (default 4)
      - click_pause: seconds after clicking (default 0.8)
      - click_use_js: bool (default True)
    """
    name = "click_action"

    def apply(self, driver, params, log, timeout_s):
        if params.get("click_enabled") is False:
            return
        if params.get("click_action") != "once":
            return

        sel = params.get("click_selector")
        if not sel:
            return

        click_timeout = float(params.get("click_timeout", 4))
        click_pause = float(params.get("click_pause", 0.8))
        use_js = bool(params.get("click_use_js", True))

        try:
            el = WebDriverWait(driver, click_timeout).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
            )
            try:
                el.click()
            except Exception:
                if use_js:
                    driver.execute_script("arguments[0].click();", el)
                else:
                    raise

            log.info("ClickActionStep: clicked %s", sel)
            time.sleep(click_pause)
        except Exception:
            log.info("ClickActionStep: click failed or not clickable: %s", sel)

# Scroll + reveal step for virtualized pages with "reveal on scroll" patterns.
class RevealAndClickStep:
    """
    Scrolls until a selector becomes visible, then optionally clicks it ONCE.
    Designed for virtualized pages.
    """
    name = "reveal_click"

    def apply(self, driver, params, log, timeout_s):
        if params.get("reveal_enabled") is False:
            return

        reveal_selector = params.get("reveal_selector")
        click_selector = params.get("reveal_click_selector")

        if not reveal_selector:
            return

        max_scrolls = int(params.get("reveal_max_scrolls", 10))
        scroll_pause = float(params.get("reveal_scroll_pause", 0.6))
        click_pause = float(params.get("reveal_click_pause", 0.8))

        for i in range(max_scrolls):
            try:
                el = driver.find_element(By.CSS_SELECTOR, reveal_selector)
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)

                if click_selector:
                    try:
                        btn = driver.find_element(By.CSS_SELECTOR, click_selector)
                        driver.execute_script("arguments[0].click();", btn)
                        log.info("RevealAndClick: clicked %s", click_selector)
                        time.sleep(click_pause)
                    except Exception:
                        log.info("RevealAndClick: element visible but click failed")

                return
            except Exception:
                driver.execute_script("window.scrollBy(0, window.innerHeight * 0.7);")
                time.sleep(scroll_pause)

        log.info("RevealAndClick: selector not revealed after %d scrolls", max_scrolls)

# SCROLL STEP — ONE SCROLL ACTION PER REQUEST (PAGINATION STYLE)
class ScrollStep:
    """
    Adapter-driven scrolling:
      - Adapter sets params["scroll_action"] = "down" in next_request()
      - Engine calls fetch() again (like page-2/page-3)
      - This step performs ONE scroll and optionally waits for more cards

    Params supported:
      - scroll_enabled: bool (default True)
      - scroll_action: "down" (anything else => no-op)
      - scroll_px: int (default 450)
      - scroll_pause: float seconds (default 0.8)
      - scroll_wait_increase_selector: CSS selector to watch
      - scroll_prev_count: previous count (adapter can store it)
      - scroll_wait_time: seconds to wait for count to increase (default 6)
    """
    name = "scroll"

    def apply(self, driver, params, log, timeout_s):
        if params.get("scroll_enabled") is False:
            return

        if params.get("scroll_action") != "down":
            return

        px = int(params.get("scroll_px", 450))
        pause = float(params.get("scroll_pause", 0.8))
        wait_sel = params.get("scroll_wait_increase_selector")
        prev = params.get("scroll_prev_count")
        wait_time = float(params.get("scroll_wait_time", 6))

        # Do one smooth scroll
        try:
            driver.execute_script(
                "window.scrollBy({ top: arguments[0], behavior: 'smooth' });",
                px,
            )
        except Exception:
            driver.execute_script("window.scrollBy(0, arguments[0]);", px)

        time.sleep(pause)

        # Optional: wait until more cards appear than last time
        if wait_sel and isinstance(prev, int):
            t0 = time.time()
            while (time.time() - t0) < wait_time:
                try:
                    now = len(driver.find_elements(By.CSS_SELECTOR, wait_sel))
                except Exception:
                    break
                if now > prev:
                    break
                time.sleep(pause)

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


# ============================================================
# WINDOW / VIEWPORT
# ============================================================

class WindowStep:
    name = "window"

    def __init__(self, default_size=(1366, 768), start_maximized=False):
        self.default_size = default_size
        self.start_maximized = start_maximized

    def apply(self, driver, params, log, timeout_s):
        if params.get("window_enabled") is False:
            return

        try:
            if params.get("start_maximized", self.start_maximized):
                driver.maximize_window()
                return

            ws = params.get("window_size")
            if isinstance(ws, str) and "," in ws:
                w, h = ws.split(",", 1)
                driver.set_window_size(int(w), int(h))
            else:
                w, h = self.default_size
                driver.set_window_size(w, h)
        except Exception:
            log.debug("WindowStep failed")


# ============================================================
# COOKIE CONSENT
# ============================================================

class CookieConsentStep:
    name = "cookies"

    def apply(self, driver, params, log, timeout_s):
        if params.get("cookies_enabled") is False:
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

        for sel in order[0] + order[1]:
            if click(By.CSS_SELECTOR, sel):
                return

        for xp in order[2] + order[3]:
            if click(By.XPATH, xp):
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
                        return
                for xp in order[2] + order[3]:
                    if click(By.XPATH, xp):
                        driver.switch_to.default_content()
                        return
                driver.switch_to.default_content()
            except Exception:
                driver.switch_to.default_content()


# ============================================================
# WAIT FOR CONTENT
# ============================================================

class WaitForSelectorStep:
    name = "wait"

    def apply(self, driver, params, log, timeout_s):
        if params.get("wait_enabled") is False:
            return

        selector = params.get("wait_selector")
        if not selector:
            return

        try:
            WebDriverWait(driver, float(params.get("wait_time", timeout_s))).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
        except Exception:
            log.debug("Wait selector not found: %s", selector)


# ============================================================
# CLICK ELEMENTS
# ============================================================

class ClickSelectorsStep:
    name = "clicks"

    def apply(self, driver, params, log, timeout_s):
        if params.get("clicks_enabled") is False:
            return

        for sel in params.get("click_selectors") or []:
            try:
                el = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
                )
                try:
                    el.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", el)
            except Exception:
                pass


# ============================================================
# SCROLL STEP — BATCH / LOAD-AWARE (BEST PRACTICE)
# ============================================================

class ScrollStep:
    name = "scroll"

    def apply(self, driver, params, log, timeout_s):
        if params.get("scroll_enabled") is False:
            return
        if not params.get("scroll"):
            return

        selector = params.get("scroll_until_selector")
        target = params.get("scroll_until_count")
        if not selector or not target:
            return

        target = int(target)

        max_rounds = int(params.get("scroll_steps", 30))
        pause = float(params.get("scroll_pause", 0.7))
        idle_wait = float(params.get("scroll_idle_wait", 1.0))
        end_wait = float(params.get("scroll_end_wait", 1.2))

        # NEW: hard cap in seconds (prevents long silent runs)
        max_seconds = float(params.get("scroll_max_seconds", 20))

        # NEW: control verbosity (log every N rounds)
        log_every = int(params.get("scroll_log_every", 1))

        start = time.time()
        last_count = -1
        no_progress_rounds = 0

        for i in range(max_rounds):
            # hard time cap
            if (time.time() - start) > max_seconds:
                log.info("ScrollStep: max_seconds reached (%.1fs), stopping", max_seconds)
                break

            try:
                current = len(driver.find_elements(By.CSS_SELECTOR, selector))
            except Exception:
                break

            if i % log_every == 0:
                log.info("ScrollStep: round=%d cards=%d target=%d", i + 1, current, target)

            if current >= target:
                log.info("ScrollStep: target reached (%d >= %d), stopping", current, target)
                break

            # if progress happened, let rendering settle
            if current > last_count:
                last_count = current
                no_progress_rounds = 0
                time.sleep(idle_wait)
                continue

            # otherwise scroll a bit (near-bottom trigger)
            try:
                doc_h = driver.execute_script("return document.body.scrollHeight") or 0
                y = driver.execute_script("return window.pageYOffset") or 0
                view_h = driver.execute_script("return window.innerHeight") or 0

                # move down ~80% of a viewport each time (human-ish)
                next_y = min(int(y + view_h * 0.8), int(doc_h))
                driver.execute_script(f"window.scrollTo({{ top: {next_y}, behavior: 'smooth' }});")
            except Exception:
                break

            time.sleep(pause)

            no_progress_rounds += 1
            if no_progress_rounds >= int(params.get("scroll_no_progress_limit", 8)):
                log.info("ScrollStep: no progress after %d scrolls, stopping", no_progress_rounds)
                break

        time.sleep(end_wait)



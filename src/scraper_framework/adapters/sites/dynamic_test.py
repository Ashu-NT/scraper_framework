from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urljoin

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.models import Page, RequestSpec
from scraper_framework.parse.cards import Card
from scraper_framework.utils.logging import get_logger

class DynamicTestAdapter(SiteAdapter):
    """
    Example adapter for sites that render content dynamically via JavaScript.
    Uses Selenium to wait for content to load before parsing.

    This adapter demonstrates mode='DYNAMIC' with wait_selector and wait_time.
    """
    def __init__(self):
        self.log = get_logger("scraper_framework.adapters.dynamic_test")

    def key(self) -> str:
        """Return the adapter key."""
        return "dynamic_test"

    def mode(self) -> str:
        """Return the scraping mode."""
        return "DYNAMIC"

    def card_locator(self) -> str:
        """Return CSS selector for card elements."""
        # Example: products loaded by JavaScript
        return "a[href^='/en/football/'][href*='-vs-']"

    def field_locator(self, field: str) -> Optional[str]:
        """Return CSS selector for a field."""
        mapping = {
            "Home_team": "div.ss div.ts div.vs",
            "Away_team": "div.ss div.us div.vs",
            "Home_score": "div.As",
            "Away_score": "div.Bs",
        }
        return mapping.get(field)

    def extract_source_url(self, card: Card, page: Page) -> Optional[str]:
        """Extract the source URL from a card."""
       # Because the card element IS the <a>, read href from the element itself
        raw = card.raw()            # BeautifulSoup element
        href = raw.get("href")      # <-- key change
        return urljoin(page.url, href) if href else None

    def extract_field(self, card: Card, field: str, page: Page) -> Any:
        """Extract a field value from a card."""
        if field == "image":
            # Prefer data-src (lazy-load), fallback to src
            src = card.get_attr("img", "data-src") or card.get_attr("img", "src")
            return urljoin(page.url, src) if src else None

        if field == "price":
            # Extract and clean price
            price_text = card.get_text(self.field_locator(field) or "")
            return price_text.strip() if price_text else None

        loc = self.field_locator(field)
        return card.get_text(loc) if loc else None

    def next_request(self, page: Page, current: RequestSpec) -> Optional[RequestSpec]:
        """
        Adapter-driven infinite scroll pagination using UNIQUE href progress.

        Why: virtualized DOM is not monotonic (counts go up/down), but unique hrefs
        seen over time is monotonic if you accumulate it in params.

        Stop conditions:
        - cursor >= scroll_max_pages
        - unique_seen_total did not increase for scroll_stall_limit cycles
        """
        params = dict(current.params or {})

        max_scroll_pages = int(params.get("scroll_max_pages", 25))
        cursor = int(params.get("scroll_cursor", 0))

        stall_limit = int(params.get("scroll_stall_limit", 3))
        stall = int(params.get("scroll_stall_count", 0))

        # Safety: stop after N scroll cycles
        if cursor >= max_scroll_pages:
            self.log.info("Scroll stop: reached scroll_max_pages=%d", max_scroll_pages)
            return None

        # Extract unique hrefs matching our card pattern.
        # We count unique per DOM snapshot, and also accumulate across cycles.
        cards = getattr(page, "_cards_cache", [])
        hrefs = set()

        for c in cards:
            u = self.extract_source_url(c, page)
            if u:
                hrefs.add(u)

        unique_in_dom = len(hrefs)

        # Accumulate total uniques across cycles in params
        seen_total = set(params.get("scroll_seen_hrefs", []))
        before_total = len(seen_total)
        seen_total.update(hrefs)
        after_total = len(seen_total)

        # Did we discover NEW unique cards this cycle?
        grew = after_total > int(params.get("scroll_unique_total", 0))

        if not grew:
            stall += 1
        else:
            stall = 0

        # Stop if stalled
        if stall >= stall_limit:
            self.log.info(
                "Scroll stop: cursor=%d unique_total=%d (+%d) dom_unique=%d stall=%d/%d",
                cursor,
                after_total,
                after_total - before_total,
                unique_in_dom,
                stall,
                stall_limit,
            )
            return None

        # Log progress (nice for debugging)
        self.log.info(
            "Scroll progress: cursor=%d unique_total=%d (+%d) dom_unique=%d stall=%d/%d",
            cursor,
            after_total,
            after_total - before_total,
            unique_in_dom,
            stall,
            stall_limit,
        )

        # Build next RequestSpec: ONE scroll action
        params.update({
            "scroll_action": "down",
            "scroll_cursor": cursor + 1,

            # store accumulated progress
            "scroll_stall_count": stall,
            "scroll_unique_total": after_total,
            "scroll_seen_hrefs": list(seen_total),

            # optional: ScrollStep can wait for DOM to show more items (best-effort)
            "scroll_wait_increase_selector": self.card_locator(),
            "scroll_prev_count": unique_in_dom,  # DOM count (not the progress metric)
        })

        return RequestSpec(
            url=current.url,
            headers=current.headers,
            params=params,
            method="GET",
            body=None,
        )



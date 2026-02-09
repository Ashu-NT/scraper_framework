from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urljoin

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.models import Page, RequestSpec
from scraper_framework.parse.cards import Card


class DynamicTestAdapter(SiteAdapter):
    """
    Example adapter for sites that render content dynamically via JavaScript.
    Uses Selenium to wait for content to load before parsing.

    This adapter demonstrates mode='DYNAMIC' with wait_selector and wait_time.
    """

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
        """Extract the next page request from the current page."""
        return None

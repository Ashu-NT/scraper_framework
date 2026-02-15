from __future__ import annotations

from typing import Any, Optional

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.models import Page, RequestSpec
from scraper_framework.parse.cards import Card


class ScrapeStatic(SiteAdapter):
    """Adapter for scraping for testing."""

    def key(self) -> str:
        """Return the adapter key."""
        return "test_static"

    def mode(self) -> str:
        """Return the scraping mode."""
        return "STATIC_HTML"

    def card_locator(self) -> str:
        """Return CSS selector for card elements."""
        return "div.row_arc"

    def field_locator(self, field: str) -> Optional[str]:
        """Return CSS selector for a field."""
        mapping = {
            "name": "p.p_class",
            "address": "p.address",
            "phone": "p.phone",
        }
        return mapping.get(field)

    def extract_source_url(self, card: Card, page: Page) -> Optional[str]:
        """Extract the source URL from a card."""
        href = card.get_attr("p a", "href")
        return href if href else None

    def extract_field(self, card: Card, field: str, page: Page) -> Any:
        """Extract a field value from a card."""
        loc = self.field_locator(field)
        return card.get_text(loc) if loc else None

    def next_request(self, page: Page, current: RequestSpec) -> Optional[RequestSpec]:
        """Extract the next page request from the current page."""
        # We'll parse via naive string search to keep adapter independent of BeautifulSoup.
        # (Later we can add a helper for next link extraction.)
        html = page.raw
        marker = '<a class="next page-numbers" href="'
        i = html.find(marker)
        if i == -1:
            return None
        j = html.find('"', i + len(marker))
        href = html[i + len(marker) : j]
        next_url = href
        return RequestSpec(url=next_url, headers=current.headers, params=current.params, method="GET", body=None)

from __future__ import annotations
from typing import Any, Optional
from urllib.parse import urljoin

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.models import Page, RequestSpec
from scraper_framework.parse.cards import Card

class BooksToScrapeAdapter(SiteAdapter):
    """Adapter for scraping books.toscrape.com."""

    def key(self) -> str:
        """Return the adapter key."""
        return "books_toscrape"

    def mode(self) -> str:
        """Return the scraping mode."""
        return "STATIC_HTML"

    def card_locator(self) -> str:
        """Return CSS selector for card elements."""
        return "article.product_pod"

    def field_locator(self, field: str) -> Optional[str]:
        """Return CSS selector for a field."""
        mapping = {
            "name": "h3 a",
            "price": ".price_color",
            "rating": "p.star-rating",
        }
        return mapping.get(field)

    def extract_source_url(self, card: Card, page: Page) -> Optional[str]:
        """Extract the source URL from a card."""
        href = card.get_attr("h3 a", "href")
        return urljoin(page.url, href) if href else None

    def extract_field(self, card: Card, field: str, page: Page) -> Any:
        """Extract a field value from a card."""
        if field == "rating":
            # rating is encoded as class: "star-rating Three"
            raw = card.raw()
            cls = raw.select_one("p.star-rating")
            if not cls:
                return None
            classes = cls.get("class", [])
            # e.g. ["star-rating", "Three"]
            for c in classes:
                if c.lower() != "star-rating":
                    return c
            return None

        loc = self.field_locator(field)
        return card.get_text(loc) if loc else None

    def next_request(self, page: Page, current: RequestSpec) -> Optional[RequestSpec]:
        """Extract the next page request from the current page."""
        # BooksToScrape uses <li class="next"><a href="...">
        # We'll parse via naive string search to keep adapter independent of BeautifulSoup.
        # (Later we can add a helper for next link extraction.)
        html = page.raw
        marker = '<li class="next"><a href="'
        i = html.find(marker)
        if i == -1:
            return None
        j = html.find('"', i + len(marker))
        href = html[i + len(marker): j]
        next_url = urljoin(page.url, href)
        return RequestSpec(url=next_url, headers=current.headers, params=current.params, method="GET", body=None)

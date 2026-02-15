# parse/parsers.py
from __future__ import annotations

from typing import List, Optional, Protocol

from bs4 import BeautifulSoup

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.models import Page, RequestSpec
from scraper_framework.parse.cards import Card, HtmlCard, JsonCard


class PageParser(Protocol):
    """Protocol for parsing pages into cards."""

    def parse_cards(self, page: Page, adapter: SiteAdapter) -> List[Card]: ...

    def next_request(self, page: Page, adapter: SiteAdapter, current: RequestSpec) -> Optional[RequestSpec]: ...


class HtmlPageParser:
    """Parser for HTML pages."""

    def parse_cards(self, page: Page, adapter: SiteAdapter) -> List[Card]:
        """Parse cards from an HTML page."""
        soup = BeautifulSoup(page.raw, "html.parser")
        locator = adapter.card_locator()
        cards = [HtmlCard(el) for el in soup.select(locator)]

        # cache for next_request()
        setattr(page, "_cards_cache", cards)
        return cards

    def next_request(self, page: Page, adapter: SiteAdapter, current: RequestSpec) -> Optional[RequestSpec]:
        """Extract next request from an HTML page."""
        return adapter.next_request(page, current)


class JsonPageParser:
    """Parser for JSON API responses."""

    def parse_cards(self, page: Page, adapter: SiteAdapter) -> List[Card]:
        """Parse cards from a JSON response."""
        # Expect adapter.card_locator() to be like "items" or "data.items"
        locator = adapter.card_locator()
        cur = page.raw
        for part in locator.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                cur = None
        items = cur if isinstance(cur, list) else []
        cards = [JsonCard(obj) for obj in items]
        setattr(page, "_cards_cache", cards)
        return cards

    def next_request(self, page: Page, adapter: SiteAdapter, current: RequestSpec) -> Optional[RequestSpec]:
        """Extract next request from a JSON response."""
        return adapter.next_request(page, current)

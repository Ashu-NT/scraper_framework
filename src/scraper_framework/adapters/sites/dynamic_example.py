from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urljoin

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.models import Page, RequestSpec
from scraper_framework.parse.cards import Card


class DynamicExampleAdapter(SiteAdapter):
    """
    Example adapter for sites that render content dynamically via JavaScript.
    Uses Selenium to wait for content to load before parsing.

    This adapter demonstrates mode='DYNAMIC' with wait_selector and wait_time.
    """

    def key(self) -> str:
        """Return the adapter key."""
        return "dynamic_example"

    def mode(self) -> str:
        """Return the scraping mode."""
        return "DYNAMIC"

    def card_locator(self) -> str:
        """Return CSS selector for card elements."""
        # Example: products loaded by JavaScript
        return ".product-item, [data-product]"

    def field_locator(self, field: str) -> Optional[str]:
        """Return CSS selector for a field."""
        mapping = {
            "name": ".product-title, .product-name, [data-name]",
            "price": ".product-price, .price, [data-price]",
            "description": ".product-desc, .description, [data-description]",
            "url": "a.product-link[href], a[data-product-url]",
            "image": "img.product-image, img[data-src]",
        }
        return mapping.get(field)

    def extract_source_url(self, card: Card, page: Page) -> Optional[str]:
        """Extract the source URL from a card."""
        # Try data attribute first, then href
        href = card.get_attr("[data-product-url]", "data-product-url")
        if href:
            return urljoin(page.url, href)

        href = card.get_attr("a[href]", "href")
        if href:
            return urljoin(page.url, href)

        return None

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

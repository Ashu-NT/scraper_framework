from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urljoin

from scraper_framework.adapters.base import SiteAdapter
from scraper_framework.core.models import Page, RequestSpec
from scraper_framework.parse.cards import Card
from scraper_framework.parse.html_utils import find_next_url


class GenericDirectoryAdapter(SiteAdapter):
    """
    A reusable adapter you can tune via selectors inside this file (or later via config).
    Goal: scrape repeated business cards from a directory-like listing page.
    """

    def key(self) -> str:
        """Return the adapter key."""
        return "directory_generic"

    def mode(self) -> str:
        """Return the scraping mode."""
        return "STATIC_HTML"

    # --- Card locator (main container) ---
    def card_locator(self) -> str:
        """Return CSS selector for card elements."""
        # You will change this per directory site.
        # Keep multiple fallbacks to handle template variations.
        return ".listing, .result, .card, article"

    # --- Field locators (sub elements inside each card) ---
    def field_locator(self, field: str) -> Optional[str]:
        """Return CSS selector for a field."""
        mapping = {
            "name": "h2, h3, .name, .title",
            "category": ".category, .type, .tags",
            "address": ".address, .location, address",
            "phone": ".phone, .tel, a[href^='tel:']",
            "website": "a.website, a[href^='http']",
            "rating": ".rating, .stars, [data-rating]",
            "reviews": ".reviews, [data-reviews]",
            "detail:availability": ".availability",  # Fields prefixed with detail: are only used during enrichment
        }
        return mapping.get(field)

    def extract_source_url(self, card: Card, page: Page) -> Optional[str]:
        """Extract the source URL from a card."""

        # best: link to the detail page
        href = card.get_attr("a[href]", "href")
        if href:
            return urljoin(page.url, href)
        return None

    def extract_field(self, card: Card, field: str, page: Page) -> Any:
        """Extract a field value from a card."""
        if field == "website":

            # Prefer explicit website link, then any absolute http link.
            href = card.get_attr("a.website[href]", "href") or card.get_attr("a[href^='http']", "href")
            return href

        if field == "phone":
            tel = card.get_attr("a[href^='tel:']", "href")
            if tel:
                return tel.replace("tel:", "").strip()
            return card.get_text(self.field_locator(field) or "")

        if field == "rating":

            # rating can be in text or data attribute
            data_rating = card.get_attr("[data-rating]", "data-rating")
            if data_rating:
                return data_rating
            return card.get_text(self.field_locator(field) or "")

        if field == "reviews":
            data_reviews = card.get_attr("[data-reviews]", "data-reviews")
            if data_reviews:
                return data_reviews
            return card.get_text(self.field_locator(field) or "")

        loc = self.field_locator(field)
        return card.get_text(loc) if loc else None

    def next_request(self, page: Page, current: RequestSpec) -> Optional[RequestSpec]:
        """Extract the next page request from the current page."""
        next_url = find_next_url(page.raw, base_url=page.url)
        if not next_url:
            return None
        return RequestSpec(
            url=next_url,
            method="GET",
            headers=current.headers,
            params=current.params,
            body=None,
        )

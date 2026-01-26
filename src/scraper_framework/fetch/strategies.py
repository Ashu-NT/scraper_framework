from __future__ import annotations
from typing import Protocol
from scraper_framework.core.models import Page, RequestSpec
from scraper_framework.http.client import HttpClient

class FetchStrategy(Protocol):
    """Protocol for fetching web pages."""

    def fetch(self, req: RequestSpec) -> Page: ...


class StaticHtmlFetchStrategy:
    """Fetch strategy for static HTML pages."""

    def __init__(self, client: HttpClient):
        self.client = client

    def fetch(self, req: RequestSpec) -> Page:
        """Fetch a page and return as HTML."""
        resp = self.client.send(req)
        return Page(
            url=req.url,
            status_code=resp.status_code,
            content_type=resp.headers.get("Content-Type", ""),
            raw=resp.text,
        )


class JsonApiFetchStrategy:
    """Fetch strategy for JSON API responses."""

    def __init__(self, client: HttpClient):
        self.client = client

    def fetch(self, req: RequestSpec) -> Page:
        """Fetch a page and return as JSON."""
        resp = self.client.send(req)
        return Page(
            url=req.url,
            status_code=resp.status_code,
            content_type=resp.headers.get("Content-Type", ""),
            raw=resp.json if resp.json is not None else {},
        )

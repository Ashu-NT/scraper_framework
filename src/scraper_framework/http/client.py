from __future__ import annotations

from typing import Protocol

import requests

from scraper_framework.core.models import RequestSpec
from scraper_framework.http.policies import RetryPolicy, backoff_sleep
from scraper_framework.http.response import HttpResponse
from scraper_framework.utils.logging import get_logger


class HttpClient(Protocol):
    """Protocol for HTTP clients."""

    def send(self, req: RequestSpec) -> HttpResponse: ...


class RequestsHttpClient:
    """HTTP client using the requests library."""

    def __init__(self, timeout_s: int = 30, retry: RetryPolicy | None = None):
        self.session = requests.Session()
        self.timeout_s = timeout_s
        self.retry = retry or RetryPolicy()
        self.log = get_logger("scraper_framework.http")

    def send(self, req: RequestSpec) -> HttpResponse:
        """Send an HTTP request with retry logic."""
        last_exc: Exception | None = None

        for attempt in range(self.retry.max_attempts):
            try:
                r = self.session.request(
                    method=req.method,
                    url=req.url,
                    headers=req.headers,
                    params=req.params,
                    json=req.body if isinstance(req.body, (dict, list)) else None,
                    data=None if isinstance(req.body, (dict, list)) else req.body,
                    timeout=self.timeout_s,
                )
                ct = r.headers.get("Content-Type", "")

                # If charset not specified, force utf-8 for HTML-ish content
                if "charset=" not in ct.lower() and ("text/html" in ct.lower() or "text/plain" in ct.lower()):
                    r.encoding = "utf-8"

                js = None
                if "application/json" in ct:
                    try:
                        js = r.json()
                    except Exception:
                        js = None

                resp = HttpResponse(status_code=r.status_code, headers=dict(r.headers), text=r.text, json=js)

                if resp.status_code in self.retry.retry_statuses and attempt < self.retry.max_attempts - 1:
                    self.log.warning("Retrying %s (status=%s, attempt=%s)", req.url, resp.status_code, attempt + 1)
                    backoff_sleep(self.retry, attempt)
                    continue

                return resp

            except Exception as e:
                last_exc = e
                if attempt < self.retry.max_attempts - 1:
                    self.log.warning("Retrying %s (exception=%s, attempt=%s)", req.url, type(e).__name__, attempt + 1)
                    backoff_sleep(self.retry, attempt)
                    continue
                raise

        # Should never hit
        raise last_exc if last_exc else RuntimeError("HTTP send failed unexpectedly")

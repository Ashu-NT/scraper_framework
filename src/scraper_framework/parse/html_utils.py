from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def find_next_url(html: str, base_url: str) -> Optional[str]:
    """
    Finds a "next page" link in common patterns.
    Works across many directory sites.
    """
    soup = BeautifulSoup(html, "html.parser")

    candidates = [
        'a[rel="next"]',
        "a.next",
        ".pagination a[aria-label*=Next]",
        ".pagination a[rel=next]",
        "li.next a",
    ]

    for css in candidates:
        a = soup.select_one(css)
        if a and a.has_attr("href"):
            href_raw: Any = a.get("href")
            href = href_raw[0] if isinstance(href_raw, list) else href_raw
            if href is not None:
                return urljoin(base_url, str(href))

    return None

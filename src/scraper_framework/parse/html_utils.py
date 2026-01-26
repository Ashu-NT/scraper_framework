from __future__ import annotations

from typing import Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin


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
            return urljoin(base_url, a["href"])

    return None

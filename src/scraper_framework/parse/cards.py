# parse/cards.py
from __future__ import annotations
from typing import Any, Optional, Protocol
from bs4 import BeautifulSoup

class Card(Protocol):
    """Protocol for card elements."""

    def raw(self) -> Any: ...
    def get_text(self, locator: str) -> Optional[str]: ...
    def get_attr(self, locator: str, attr: str) -> Optional[str]: ...
    def get_value(self, locator: str) -> Any: ...


class HtmlCard:
    """Card implementation for HTML elements."""

    def __init__(self, root: Any):
        self._root = root

    def raw(self) -> Any:
        """Return the raw element."""
        return self._root

    def get_text(self, locator: str) -> Optional[str]:
        """Get text from a CSS selector."""
        el = self._root.select_one(locator)
        return el.get_text(" ", strip=True) if el else None

    def get_attr(self, locator: str, attr: str) -> Optional[str]:
        """Get attribute value from a CSS selector."""
        el = self._root.select_one(locator)
        return el.get(attr) if el and el.has_attr(attr) else None

    def get_value(self, locator: str) -> Any:
        """Get value from a CSS selector."""
        return self.get_text(locator)


class JsonCard:
    """Card implementation for JSON objects."""

    def __init__(self, obj: Any):
        self._obj = obj

    def raw(self) -> Any:
        """Return the raw object."""
        return self._obj

    def get_text(self, locator: str) -> Optional[str]:
        """Get text from a dot-path locator."""
        v = self.get_value(locator)
        return str(v) if v is not None else None

    def get_attr(self, locator: str, attr: str) -> Optional[str]:
        """Get attribute from a dot-path locator."""
        v = self.get_value(locator)
        if isinstance(v, dict):
            return v.get(attr)
        return None

    def get_value(self, locator: str) -> Any:
        """Get value from a dot-path locator."""
        # Minimal dot-path: "a.b.c"
        cur = self._obj
        for part in locator.split("."):
            if cur is None:
                return None
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None
        return cur

from __future__ import annotations

from typing import Dict

from scraper_framework.adapters.base import SiteAdapter

_ADAPTERS: Dict[str, SiteAdapter] = {}


def register(adapter: SiteAdapter) -> None:
    """Register a site adapter."""
    _ADAPTERS[adapter.key()] = adapter


def get(key: str) -> SiteAdapter:
    """Retrieve a registered adapter by key."""
    if key not in _ADAPTERS:
        raise KeyError(f"Adapter not registered: {key}")
    return _ADAPTERS[key]


def get_registered_adapters():
    """Get all registered adapters."""
    return list(_ADAPTERS.values())

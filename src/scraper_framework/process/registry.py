from __future__ import annotations

from typing import Callable, Dict, Iterable

from scraper_framework.process.base import ProcessorPlugin
from scraper_framework.process.plugins import built_in_plugin_factories


class ProcessingRegistry:
    """Registry mapping plugin names to plugin factories."""

    def __init__(self):
        self._factories: Dict[str, Callable[[], ProcessorPlugin]] = {}

    def register(self, name: str, factory: Callable[[], ProcessorPlugin]) -> None:
        key = str(name or "").strip()
        if not key:
            raise ValueError("Processor plugin name cannot be empty")
        self._factories[key] = factory

    def create(self, name: str) -> ProcessorPlugin:
        key = str(name or "").strip()
        if key not in self._factories:
            known = ", ".join(sorted(self._factories.keys()))
            raise KeyError(f"Unknown processor plugin '{key}'. Known plugins: {known}")
        return self._factories[key]()

    def keys(self) -> Iterable[str]:
        return self._factories.keys()


def create_default_registry() -> ProcessingRegistry:
    """Create a registry preloaded with built-in plugins."""
    registry = ProcessingRegistry()
    for name, cls in built_in_plugin_factories().items():
        registry.register(name, cls)
    return registry

from __future__ import annotations

from scraper_framework.process.plugins.basic import (
    DropIfFieldEmptyProcessor,
    FieldCoverageAnalyticsProcessor,
    PassThroughProcessor,
)


def built_in_plugin_factories() -> dict[str, type]:
    """Return built-in processing plugin factories keyed by plugin name."""
    return {
        PassThroughProcessor.name: PassThroughProcessor,
        DropIfFieldEmptyProcessor.name: DropIfFieldEmptyProcessor,
        FieldCoverageAnalyticsProcessor.name: FieldCoverageAnalyticsProcessor,
    }

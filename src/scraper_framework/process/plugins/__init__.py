from __future__ import annotations

from scraper_framework.process.plugins.basic import (
    ClientQualityScoreProcessor,
    DropIfFieldEmptyProcessor,
    FieldCoverageAnalyticsProcessor,
    NormalizeUpworkAgeProcessor,
    NormalizeUpworkBudgetProcessor,
    PassThroughProcessor,
    ScoreLeadFitProcessor,
    TopNPerSegmentProcessor,
)


def built_in_plugin_factories() -> dict[str, type]:
    """Return built-in processing plugin factories keyed by plugin name."""
    return {
        PassThroughProcessor.name: PassThroughProcessor,
        DropIfFieldEmptyProcessor.name: DropIfFieldEmptyProcessor,
        FieldCoverageAnalyticsProcessor.name: FieldCoverageAnalyticsProcessor,
        ScoreLeadFitProcessor.name: ScoreLeadFitProcessor,
        TopNPerSegmentProcessor.name: TopNPerSegmentProcessor,
        NormalizeUpworkBudgetProcessor.name: NormalizeUpworkBudgetProcessor,
        NormalizeUpworkAgeProcessor.name: NormalizeUpworkAgeProcessor,
        ClientQualityScoreProcessor.name: ClientQualityScoreProcessor,
    }

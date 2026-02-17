from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class DedupeMode(str, Enum):
    """Enumeration for deduplication modes."""

    BY_SOURCE_URL = "BY_SOURCE_URL"
    BY_HASH = "BY_HASH"


@dataclass(frozen=True)
class RequestSpec:
    """Specification for an HTTP request."""

    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    body: Any = None


@dataclass(frozen=True)
class EnrichConfig:
    """Configuration for enrichment features."""

    enabled: bool = False
    fields: Set[str] = field(default_factory=set)


@dataclass(frozen=True)
class ProcessingStage:
    """Configuration for a single processing stage."""

    plugin: str
    stage_type: str = "record"  # record | batch | analytics
    on_error: str = "fail"  # fail | skip | quarantine
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProcessingConfig:
    """Configuration for post-scrape processing pipeline."""

    enabled: bool = False
    schema_version: str = "1.0"
    stages: List[ProcessingStage] = field(default_factory=list)


@dataclass(frozen=True)
class IncrementalConfig:
    """Configuration for incremental caching/checkpoint behavior."""

    enabled: bool = False
    backend: str = "sqlite"  # sqlite
    state_path: str = "output/state.db"
    mode: str = "changed_only"  # all | new_only | changed_only
    resume: bool = True
    checkpoint_every_pages: int = 1
    full_refresh_every_runs: Optional[int] = None


@dataclass(frozen=True)
class ScrapeJob:
    """Configuration for a scraping job."""

    id: str
    name: str
    start: RequestSpec
    execution_mode: str = "memory"  # memory | stream
    batch_size: int = 500
    max_pages: int = 5
    delay_ms: int = 800
    required_fields: Set[str] = field(default_factory=lambda: {"name", "source_url"})
    dedupe_mode: DedupeMode = DedupeMode.BY_SOURCE_URL
    field_schema: List[str] = field(default_factory=list)
    dynamic_engine: str = "selenium"  # selenium | playwright
    enrich: EnrichConfig = field(default_factory=EnrichConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    incremental: IncrementalConfig = field(default_factory=IncrementalConfig)
    sink_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Page:
    """Represents a fetched web page."""

    url: str
    status_code: int
    content_type: str
    raw: Any


@dataclass
class Record:
    """A scraped data record."""

    id: str
    source_url: str
    scraped_at_utc: str
    fields: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationResult:
    """Result of record validation."""

    ok: bool
    reason: str = ""


@dataclass
class ScrapeReport:
    """Summary report of a scraping job."""

    pages_fetched: int = 0
    cards_found: int = 0
    records_emitted: int = 0
    records_skipped: int = 0
    records_skipped_incremental: int = 0
    records_quarantined: int = 0
    failures: Dict[str, int] = field(default_factory=dict)
    processing_stage_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    processing_artifacts: Dict[str, Any] = field(default_factory=dict)

    def bump_failure(self, key: str) -> None:
        """Increment the count for a specific failure type."""
        self.failures[key] = self.failures.get(key, 0) + 1

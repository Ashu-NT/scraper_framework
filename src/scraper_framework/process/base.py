from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Protocol

from pydantic import BaseModel, Field, ValidationError

from scraper_framework.core.models import Record


class RecordSchemaV1(BaseModel):
    """Pydantic schema for pipeline record shape v1.0."""
    id: str
    source_url: str
    scraped_at_utc: str
    fields: Dict[str, Any] = Field(default_factory=dict)


class StagePayloadV1(BaseModel):
    """Container model for validating stage input/output schema."""
    schema_version: Literal["1.0"]
    records: List[RecordSchemaV1]


def validate_records_schema(records: List[Record], schema_version: str) -> None:
    """Validate records against the configured stage schema version."""
    if schema_version != "1.0":
        raise ValueError(f"Unsupported processing schema_version: {schema_version}")

    payload = {
        "schema_version": schema_version,
        "records": [
            {
                "id": r.id,
                "source_url": r.source_url,
                "scraped_at_utc": r.scraped_at_utc,
                "fields": r.fields,
            }
            for r in records
        ],
    }

    try:
        StagePayloadV1(**payload)
    except ValidationError as e:
        raise ValueError(f"Processing schema validation failed: {e}") from e


@dataclass(frozen=True)
class ProcessContext:
    """Shared runtime context provided to processing plugins."""
    job_id: str
    job_name: str
    stage_name: str
    stage_index: int
    run_started_utc: str


@dataclass
class StageRuntimeMetrics:
    """Runtime metrics captured for each processing stage."""
    records_in: int = 0
    records_out: int = 0
    dropped: int = 0
    errors: int = 0
    latency_ms: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "records_in": self.records_in,
            "records_out": self.records_out,
            "dropped": self.dropped,
            "errors": self.errors,
            "latency_ms": round(self.latency_ms, 3),
        }


@dataclass
class ProcessResult:
    """Result contract for batch and analytics processors."""
    records: List[Record]
    artifacts: Dict[str, Any] = field(default_factory=dict)


class ProcessorPlugin(Protocol):
    """Strategy contract implemented by each processing plugin."""
    name: str
    stage_type: str  # record | batch | analytics
    input_schema_version: str
    output_schema_version: str
    idempotent: bool

    def process_record(self, record: Record, config: Dict[str, Any], ctx: ProcessContext) -> Optional[Record]:
        ...

    def process_batch(self, records: List[Record], config: Dict[str, Any], ctx: ProcessContext) -> ProcessResult:
        ...

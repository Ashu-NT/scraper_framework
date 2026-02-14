from __future__ import annotations

from typing import Any, Dict, List, Optional

from scraper_framework.core.models import Record
from scraper_framework.process.base import ProcessContext, ProcessResult


class PassThroughProcessor:
    """No-op processor useful for smoke tests and baseline stages."""
    name = "pass_through"
    stage_type = "batch"
    input_schema_version = "1.0"
    output_schema_version = "1.0"
    idempotent = True

    def process_record(self, record: Record, config: Dict[str, Any], ctx: ProcessContext) -> Optional[Record]:
        return record

    def process_batch(self, records: List[Record], config: Dict[str, Any], ctx: ProcessContext) -> ProcessResult:
        return ProcessResult(records=list(records))


class DropIfFieldEmptyProcessor:
    """
    Drops records where configured field is missing/empty.
    Config:
      - field (str, required)
    """
    name = "drop_if_field_empty"
    stage_type = "record"
    input_schema_version = "1.0"
    output_schema_version = "1.0"
    idempotent = True

    def process_record(self, record: Record, config: Dict[str, Any], ctx: ProcessContext) -> Optional[Record]:
        field_name = str(config.get("field", "")).strip()
        if not field_name:
            raise ValueError("drop_if_field_empty requires config.field")

        if field_name == "source_url":
            value = record.source_url
        elif field_name == "id":
            value = record.id
        elif field_name == "scraped_at_utc":
            value = record.scraped_at_utc
        else:
            value = record.fields.get(field_name)

        if value is None or str(value).strip() == "":
            return None
        return record

    def process_batch(self, records: List[Record], config: Dict[str, Any], ctx: ProcessContext) -> ProcessResult:
        # Runner executes record stages per-record via process_record.
        return ProcessResult(records=list(records))


class FieldCoverageAnalyticsProcessor:
    """
    Computes field population coverage without mutating records.
    Config:
      - fields (list[str], optional): explicit field names to measure.
    """
    name = "field_coverage_analytics"
    stage_type = "analytics"
    input_schema_version = "1.0"
    output_schema_version = "1.0"
    idempotent = True

    def process_record(self, record: Record, config: Dict[str, Any], ctx: ProcessContext) -> Optional[Record]:
        return record

    def process_batch(self, records: List[Record], config: Dict[str, Any], ctx: ProcessContext) -> ProcessResult:
        configured_fields = config.get("fields") or []
        fields = [str(f) for f in configured_fields if str(f).strip()]

        if not fields:
            discovered = set()
            for rec in records:
                discovered.update(rec.fields.keys())
            fields = sorted(discovered)

        coverage = {}
        total = len(records)
        for field_name in fields:
            present = 0
            for rec in records:
                val = rec.fields.get(field_name)
                if val is not None and str(val).strip() != "":
                    present += 1
            coverage[field_name] = {
                "present": present,
                "missing": max(total - present, 0),
                "coverage_ratio": (present / total) if total > 0 else 0.0,
            }

        artifacts = {
            "total_records": total,
            "field_coverage": coverage,
        }
        return ProcessResult(records=list(records), artifacts=artifacts)

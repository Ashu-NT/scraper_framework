from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List

from scraper_framework.core.models import Record, ScrapeJob
from scraper_framework.process.base import (
    ProcessContext,
    ProcessResult,
    StageRuntimeMetrics,
    validate_records_schema,
)
from scraper_framework.process.registry import ProcessingRegistry
from scraper_framework.utils.logging import get_logger
from scraper_framework.utils.time import utc_now_iso


@dataclass
class ProcessingRunResult:
    """Output of a processing pipeline run."""

    records: List[Record]
    schema_version: str
    records_quarantined: int = 0
    stage_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    artifacts: Dict[str, Any] = field(default_factory=dict)


class ProcessingRunner:
    """Executes configured processing stages with policy-aware error handling."""

    def __init__(self, registry: ProcessingRegistry):
        self.registry = registry
        self.log = get_logger("scraper_framework.processing.runner")

    def run(self, job: ScrapeJob, records: List[Record]) -> ProcessingRunResult:
        cfg = getattr(job, "processing", None)
        if not cfg or not cfg.enabled or not cfg.stages:
            return ProcessingRunResult(records=list(records), schema_version="1.0")

        current_records = list(records)
        schema_version = cfg.schema_version
        quarantined: List[Record] = []
        stage_metrics: Dict[str, Dict[str, Any]] = {}
        artifacts: Dict[str, Any] = {}

        validate_records_schema(current_records, schema_version)

        for idx, stage in enumerate(cfg.stages, start=1):
            current_records, schema_version = self._run_stage(
                job=job,
                stage=stage,
                stage_index=idx,
                current_records=current_records,
                schema_version=schema_version,
                quarantined=quarantined,
                stage_metrics=stage_metrics,
                artifacts=artifacts,
            )

        return ProcessingRunResult(
            records=current_records,
            schema_version=schema_version,
            records_quarantined=len(quarantined),
            stage_metrics=stage_metrics,
            artifacts=artifacts,
        )

    def _run_stage(
        self,
        job: ScrapeJob,
        stage: Any,
        stage_index: int,
        current_records: List[Record],
        schema_version: str,
        quarantined: List[Record],
        stage_metrics: Dict[str, Dict[str, Any]],
        artifacts: Dict[str, Any],
    ) -> tuple[List[Record], str]:
        stage_name = f"{stage_index}:{stage.plugin}"
        started = time.perf_counter()
        metrics = StageRuntimeMetrics(records_in=len(current_records))
        input_snapshot = list(current_records)
        on_error = str(stage.on_error or "fail").lower()
        ctx = self._build_stage_context(job, stage_name, stage_index)

        try:
            plugin = self.registry.create(stage.plugin)
            self._validate_stage_contracts(stage_name, stage, plugin, schema_version)
            current_records, stage_artifacts = self._execute_stage(
                plugin,
                current_records,
                stage.config,
                ctx,
                on_error,
                metrics,
                quarantined,
            )
            if stage_artifacts:
                artifacts[stage_name] = stage_artifacts
            validate_records_schema(current_records, plugin.output_schema_version)
            schema_version = plugin.output_schema_version
        except Exception as exc:
            current_records = self._handle_stage_exception(
                exc=exc,
                stage_name=stage_name,
                on_error=on_error,
                input_snapshot=input_snapshot,
                current_records=current_records,
                metrics=metrics,
                quarantined=quarantined,
            )
        finally:
            self._finalize_stage_metrics(metrics, current_records, started, stage_metrics, stage_name)

        return current_records, schema_version

    def _build_stage_context(self, job: ScrapeJob, stage_name: str, stage_index: int) -> ProcessContext:
        return ProcessContext(
            job_id=job.id,
            job_name=job.name,
            stage_name=stage_name,
            stage_index=stage_index,
            run_started_utc=utc_now_iso(),
        )

    def _validate_stage_contracts(self, stage_name: str, stage: Any, plugin: Any, schema_version: str) -> None:
        if stage.stage_type != plugin.stage_type:
            raise ValueError(f"Stage '{stage_name}' type mismatch: config={stage.stage_type} plugin={plugin.stage_type}")
        if plugin.input_schema_version != schema_version:
            raise ValueError(f"Stage '{stage_name}' expected schema {plugin.input_schema_version}, got {schema_version}")
        if not plugin.idempotent:
            raise ValueError(f"Stage '{stage_name}' plugin must be idempotent")

    def _execute_stage(
        self,
        plugin: Any,
        current_records: List[Record],
        stage_config: Dict[str, Any],
        ctx: ProcessContext,
        on_error: str,
        metrics: StageRuntimeMetrics,
        quarantined: List[Record],
    ) -> tuple[List[Record], Dict[str, Any]]:
        if plugin.stage_type == "record":
            return (
                self._run_record_stage(
                    plugin=plugin,
                    records=current_records,
                    stage_config=stage_config,
                    ctx=ctx,
                    on_error=on_error,
                    metrics=metrics,
                    quarantined=quarantined,
                ),
                {},
            )

        result = plugin.process_batch(current_records, stage_config, ctx)
        if not isinstance(result, ProcessResult):
            raise ValueError(f"Stage '{ctx.stage_name}' must return ProcessResult for {plugin.stage_type} plugins")
        return list(result.records), dict(result.artifacts)

    def _handle_stage_exception(
        self,
        exc: Exception,
        stage_name: str,
        on_error: str,
        input_snapshot: List[Record],
        current_records: List[Record],
        metrics: StageRuntimeMetrics,
        quarantined: List[Record],
    ) -> List[Record]:
        self.log.error("Processing stage failed (%s): %s", stage_name, exc)
        if metrics.errors == 0:
            metrics.errors += 1

        if on_error == "fail":
            raise
        if on_error == "skip":
            return input_snapshot
        if on_error == "quarantine":
            quarantined.extend(input_snapshot)
            return []
        raise ValueError(f"Invalid on_error policy '{on_error}' for stage '{stage_name}'") from exc

    def _finalize_stage_metrics(
        self,
        metrics: StageRuntimeMetrics,
        current_records: List[Record],
        started: float,
        stage_metrics: Dict[str, Dict[str, Any]],
        stage_name: str,
    ) -> None:
        metrics.records_out = len(current_records)
        metrics.dropped = max(metrics.records_in - metrics.records_out, 0)
        metrics.latency_ms = (time.perf_counter() - started) * 1000.0
        stage_metrics[stage_name] = metrics.as_dict()

    def _run_record_stage(
        self,
        plugin,
        records: List[Record],
        stage_config: Dict[str, Any],
        ctx: ProcessContext,
        on_error: str,
        metrics: StageRuntimeMetrics,
        quarantined: List[Record],
    ) -> List[Record]:
        output: List[Record] = []
        for record in records:
            try:
                next_record = plugin.process_record(record, stage_config, ctx)
            except Exception as exc:
                self.log.error("Record processing failed (%s): %s", ctx.stage_name, exc)
                metrics.errors += 1

                if on_error == "fail":
                    raise
                if on_error == "skip":
                    output.append(record)
                    continue
                if on_error == "quarantine":
                    quarantined.append(record)
                    continue
                raise ValueError(f"Invalid on_error policy '{on_error}' for stage '{ctx.stage_name}'") from exc

            if next_record is None:
                metrics.dropped += 1
                continue

            output.append(next_record)

        return output

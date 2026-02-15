from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from scraper_framework.core.models import Record
from scraper_framework.process.base import ProcessContext, ProcessResult

_NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")
_RANGE_RE = re.compile(r"\$?\s*(\d+(?:[.,]\d+)?)\s*[-to]+\s*\$?\s*(\d+(?:[.,]\d+)?)")
_AGE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(minute|minutes|min|hour|hours|hr|hrs|day|days|week|weeks)")


def _field_value(record: Record, field_name: str) -> Any:
    if field_name == "id":
        return record.id
    if field_name == "source_url":
        return record.source_url
    if field_name == "scraped_at_utc":
        return record.scraped_at_utc
    return record.fields.get(field_name)


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip().replace(",", "")
    if not s:
        return default

    m = _NUM_RE.search(s)
    if not m:
        return default
    try:
        return float(m.group(0))
    except ValueError:
        return default


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    s = str(value).strip().lower()
    return s in {"true", "1", "yes", "y", "verified"}


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

        value: Any
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
            discovered: set[str] = set()
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


class ScoreLeadFitProcessor:
    """
    Computes a weighted lead score and writes it to a record field.

    Config:
      - weights (dict[str,float], optional):
          numeric score weights by field name.
      - presence_weights (dict[str,float], optional):
          adds weight when field value is present/non-empty.
      - output_field (str, optional): destination field (default: "lead_score")
      - round_digits (int, optional): score rounding precision (default: 4)
    """

    name = "score_lead_fit"
    stage_type = "record"
    input_schema_version = "1.0"
    output_schema_version = "1.0"
    idempotent = True

    def process_record(self, record: Record, config: Dict[str, Any], ctx: ProcessContext) -> Optional[Record]:
        weights = config.get("weights") or {}
        presence_weights = config.get("presence_weights") or {}
        output_field = str(config.get("output_field", "lead_score")).strip() or "lead_score"
        round_digits = int(config.get("round_digits", 4))

        if not isinstance(weights, dict):
            raise ValueError("score_lead_fit config.weights must be a dict")
        if not isinstance(presence_weights, dict):
            raise ValueError("score_lead_fit config.presence_weights must be a dict")
        if not weights and not presence_weights:
            raise ValueError("score_lead_fit requires config.weights and/or config.presence_weights")

        score = 0.0

        for field_name, weight in weights.items():
            w = float(weight)
            value = _field_value(record, str(field_name))
            score += _to_float(value) * w

        for field_name, weight in presence_weights.items():
            w = float(weight)
            value = _field_value(record, str(field_name))
            if value is not None and str(value).strip() != "":
                score += w

        record.fields[output_field] = round(score, round_digits)
        return record

    def process_batch(self, records: List[Record], config: Dict[str, Any], ctx: ProcessContext) -> ProcessResult:
        # Runner executes record stages per-record via process_record.
        return ProcessResult(records=list(records))


class TopNPerSegmentProcessor:
    """
    Keeps top N records per segment based on score field.

    Config:
      - segment_field (str, required)
      - score_field (str, optional, default: "lead_score")
      - top_n (int, optional, default: 1)
      - include_missing_segment (bool, optional, default: False)
    """

    name = "top_n_per_segment"
    stage_type = "batch"
    input_schema_version = "1.0"
    output_schema_version = "1.0"
    idempotent = True

    def process_record(self, record: Record, config: Dict[str, Any], ctx: ProcessContext) -> Optional[Record]:
        return record

    def process_batch(self, records: List[Record], config: Dict[str, Any], ctx: ProcessContext) -> ProcessResult:
        segment_field = str(config.get("segment_field", "")).strip()
        if not segment_field:
            raise ValueError("top_n_per_segment requires config.segment_field")

        score_field = str(config.get("score_field", "lead_score")).strip() or "lead_score"
        top_n = int(config.get("top_n", 1))
        include_missing_segment = bool(config.get("include_missing_segment", False))

        if top_n < 1:
            raise ValueError("top_n_per_segment config.top_n must be >= 1")

        groups: Dict[str, List[tuple[int, Record]]] = {}
        for idx, rec in enumerate(records):
            segment_val = _field_value(rec, segment_field)
            segment = "" if segment_val is None else str(segment_val).strip()
            if not segment and not include_missing_segment:
                continue

            key = segment if segment else "__missing__"
            groups.setdefault(key, []).append((idx, rec))

        selected: List[Record] = []
        segment_counts: Dict[str, int] = {}

        for segment, items in groups.items():
            ranked = sorted(
                items,
                key=lambda p: (
                    -_to_float(_field_value(p[1], score_field), default=0.0),
                    p[0],  # stable tie-breaker by original order
                ),
            )
            winners = ranked[:top_n]
            segment_counts[segment] = len(winners)
            selected.extend([rec for _, rec in winners])

        index_by_identity = {id(rec): idx for idx, rec in enumerate(records)}
        selected_sorted = sorted(
            selected,
            key=lambda rec: index_by_identity.get(id(rec), 0),
        )
        artifacts = {
            "total_input": len(records),
            "total_output": len(selected_sorted),
            "top_n": top_n,
            "segment_field": segment_field,
            "score_field": score_field,
            "selected_per_segment": segment_counts,
        }
        return ProcessResult(records=selected_sorted, artifacts=artifacts)


class NormalizeUpworkBudgetProcessor:
    """
    Parse budget strings into normalized numeric fields.

    Config:
      - input_field (str, optional, default: "budget")
      - output_prefix (str, optional, default: "budget")
      - hourly_to_usd_hours (int, optional, default: 160)
    """

    name = "normalize_upwork_budget"
    stage_type = "record"
    input_schema_version = "1.0"
    output_schema_version = "1.0"
    idempotent = True

    def process_record(self, record: Record, config: Dict[str, Any], ctx: ProcessContext) -> Optional[Record]:
        input_field = str(config.get("input_field", "budget")).strip() or "budget"
        output_prefix = str(config.get("output_prefix", "budget")).strip() or "budget"
        hourly_to_usd_hours = int(config.get("hourly_to_usd_hours", 160))

        raw = _field_value(record, input_field)
        text = "" if raw is None else str(raw).strip()
        if not text:
            return record

        lower = text.lower()
        budget_type = "hourly" if ("hour" in lower or "/hr" in lower) else "fixed"

        min_val, max_val = self._extract_min_max(text)
        if min_val is None and max_val is None:
            return record

        if min_val is None:
            min_val = max_val
        if max_val is None:
            max_val = min_val

        if min_val is None or max_val is None:
            return record

        avg = (float(min_val) + float(max_val)) / 2.0
        usd_est = avg * hourly_to_usd_hours if budget_type == "hourly" else avg

        record.fields[f"{output_prefix}_type"] = budget_type
        record.fields[f"{output_prefix}_currency"] = "USD"
        record.fields[f"{output_prefix}_min"] = round(float(min_val), 4)
        record.fields[f"{output_prefix}_max"] = round(float(max_val), 4)
        record.fields[f"{output_prefix}_usd_est"] = round(float(usd_est), 4)
        return record

    def process_batch(self, records: List[Record], config: Dict[str, Any], ctx: ProcessContext) -> ProcessResult:
        return ProcessResult(records=list(records))

    def _extract_min_max(self, text: str) -> tuple[Optional[float], Optional[float]]:
        m = _RANGE_RE.search(text.lower().replace(",", ""))
        if m:
            lo = _to_float(m.group(1), default=0.0)
            hi = _to_float(m.group(2), default=0.0)
            if hi < lo:
                lo, hi = hi, lo
            return lo, hi

        nums = [float(n.replace(",", "")) for n in re.findall(r"\d+(?:[.,]\d+)?", text)]
        if not nums:
            return None, None
        if len(nums) >= 2:
            lo, hi = nums[0], nums[1]
            if hi < lo:
                lo, hi = hi, lo
            return lo, hi
        return nums[0], nums[0]


class NormalizeUpworkAgeProcessor:
    """
    Parse relative posted age into normalized hour metric.

    Config:
      - input_field (str, optional, default: "posted_ago")
      - output_hours_field (str, optional, default: "post_age_hours")
      - output_bucket_field (str, optional, default: "post_age_bucket")
      - now_utc (str, optional): ISO timestamp for deterministic parsing
    """

    name = "normalize_upwork_age"
    stage_type = "record"
    input_schema_version = "1.0"
    output_schema_version = "1.0"
    idempotent = True

    def process_record(self, record: Record, config: Dict[str, Any], ctx: ProcessContext) -> Optional[Record]:
        input_field = str(config.get("input_field", "posted_ago")).strip() or "posted_ago"
        output_hours_field = str(config.get("output_hours_field", "post_age_hours")).strip() or "post_age_hours"
        output_bucket_field = str(config.get("output_bucket_field", "post_age_bucket")).strip() or "post_age_bucket"
        now_utc = str(config.get("now_utc", ctx.run_started_utc)).strip() or ctx.run_started_utc

        raw = _field_value(record, input_field)
        text = "" if raw is None else str(raw).strip()
        if not text:
            return record

        hours = self._parse_age_hours(text, now_utc)
        if hours is None:
            return record

        record.fields[output_hours_field] = round(hours, 4)
        record.fields[output_bucket_field] = self._bucket(hours)
        return record

    def process_batch(self, records: List[Record], config: Dict[str, Any], ctx: ProcessContext) -> ProcessResult:
        return ProcessResult(records=list(records))

    def _parse_age_hours(self, text: str, now_utc: str) -> Optional[float]:
        s = text.strip().lower()
        if not s:
            return None

        if "just now" in s or s == "now":
            return 0.0
        if "yesterday" in s:
            return 24.0
        if "today" in s:
            return 0.0

        m = _AGE_RE.search(s)
        if m:
            qty = _to_float(m.group(1), default=0.0)
            unit = m.group(2)
            if unit in {"minute", "minutes", "min"}:
                return qty / 60.0
            if unit in {"hour", "hours", "hr", "hrs"}:
                return qty
            if unit in {"day", "days"}:
                return qty * 24.0
            if unit in {"week", "weeks"}:
                return qty * 24.0 * 7.0

        # If value is an ISO datetime, compute age from provided now.
        parsed_dt = self._parse_iso_datetime(text)
        now_dt = self._parse_iso_datetime(now_utc)
        if parsed_dt and now_dt:
            diff_h = (now_dt - parsed_dt).total_seconds() / 3600.0
            return max(diff_h, 0.0)

        return None

    def _parse_iso_datetime(self, text: str) -> Optional[datetime]:
        s = str(text).strip()
        if not s:
            return None
        try:
            if s.endswith("Z"):
                dt = datetime.fromisoformat(s[:-1] + "+00:00")
            else:
                dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            return None

    def _bucket(self, hours: float) -> str:
        if hours <= 1:
            return "fresh"
        if hours <= 24:
            return "recent"
        if hours <= 72:
            return "warm"
        return "stale"


class ClientQualityScoreProcessor:
    """
    Score client quality based on trust/payment and historical client signals.

    Config:
      - fields (dict, optional): override input field names:
          payment_verified, hire_rate, total_spent, avg_hourly_rate, reviews, jobs_posted
      - output_field (str, optional, default: "client_quality_score")
      - output_tier_field (str, optional, default: "client_quality_tier")
      - round_digits (int, optional, default: 2)
    """

    name = "client_quality_score"
    stage_type = "record"
    input_schema_version = "1.0"
    output_schema_version = "1.0"
    idempotent = True

    def process_record(self, record: Record, config: Dict[str, Any], ctx: ProcessContext) -> Optional[Record]:
        field_cfg = config.get("fields") or {}
        if not isinstance(field_cfg, dict):
            raise ValueError("client_quality_score config.fields must be a dict if provided")

        f_payment = str(field_cfg.get("payment_verified", "payment_verified"))
        f_hire_rate = str(field_cfg.get("hire_rate", "hire_rate"))
        f_total_spent = str(field_cfg.get("total_spent", "total_spent"))
        f_avg_hourly = str(field_cfg.get("avg_hourly_rate", "avg_hourly_rate"))
        f_reviews = str(field_cfg.get("reviews", "reviews"))
        f_jobs_posted = str(field_cfg.get("jobs_posted", "jobs_posted"))

        output_field = str(config.get("output_field", "client_quality_score")).strip() or "client_quality_score"
        output_tier_field = str(config.get("output_tier_field", "client_quality_tier")).strip() or "client_quality_tier"
        round_digits = int(config.get("round_digits", 2))

        payment_verified = _as_bool(_field_value(record, f_payment))
        hire_rate = max(0.0, min(_to_float(_field_value(record, f_hire_rate), default=0.0), 100.0))
        total_spent = max(0.0, _to_float(_field_value(record, f_total_spent), default=0.0))
        avg_hourly = max(0.0, _to_float(_field_value(record, f_avg_hourly), default=0.0))
        reviews = max(0.0, _to_float(_field_value(record, f_reviews), default=0.0))
        jobs_posted = max(0.0, _to_float(_field_value(record, f_jobs_posted), default=0.0))

        score = 0.0
        if payment_verified:
            score += 20.0
        score += (hire_rate / 100.0) * 35.0
        score += (min(total_spent, 100000.0) / 100000.0) * 20.0
        score += (min(reviews, 50.0) / 50.0) * 15.0
        score += (min(avg_hourly, 100.0) / 100.0) * 10.0
        score += (min(jobs_posted, 100.0) / 100.0) * 10.0
        score = min(score, 100.0)

        tier = "high" if score >= 75.0 else ("medium" if score >= 50.0 else "low")
        record.fields[output_field] = round(score, round_digits)
        record.fields[output_tier_field] = tier
        return record

    def process_batch(self, records: List[Record], config: Dict[str, Any], ctx: ProcessContext) -> ProcessResult:
        return ProcessResult(records=list(records))

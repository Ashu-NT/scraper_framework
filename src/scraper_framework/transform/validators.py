from __future__ import annotations
from typing import Protocol, Set
from scraper_framework.core.models import Record, ValidationResult

class Validator(Protocol):
    """Protocol for record validators."""

    def validate(self, record: Record, required: Set[str]) -> ValidationResult: ...


class RequiredFieldsValidator:
    """Validator that checks for required fields."""

    def validate(self, record: Record, required: Set[str]) -> ValidationResult:
        """Validate that required fields are present and non-empty."""
        # required fields can be in record.fields OR core fields
        for f in required:
            if f == "source_url":
                if not record.source_url:
                    return ValidationResult(False, "missing_source_url")
            else:
                v = record.fields.get(f)
                if v is None or str(v).strip() == "":
                    return ValidationResult(False, f"missing_{f}")
        return ValidationResult(True, "")

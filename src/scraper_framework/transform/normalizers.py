from __future__ import annotations
import re
from typing import Any, Optional, Protocol
from scraper_framework.core.models import Record

class Normalizer(Protocol):
    """Protocol for record normalizers."""

    def normalize(self, record: Record) -> Record: ...


class DefaultNormalizer:
    """Default implementation of field normalization."""

    _num = re.compile(r"(\d+(?:[.,]\d+)?)")
    _int = re.compile(r"(\d+)")

    def normalize(self, record: Record) -> Record:
        """Normalize common fields in the record."""
        # Example: normalize common fields if present
        if "rating" in record.fields:
            record.fields["rating"] = self.parse_rating(record.fields.get("rating"))
        if "reviews" in record.fields:
            record.fields["reviews"] = self.parse_int(record.fields.get("reviews"))
        if "website" in record.fields:
            record.fields["website"] = self.clean_url(record.fields.get("website"))
        return record

    def parse_number(self, raw: Any) -> Optional[float]:
        """Parse a number from raw input."""
        if raw is None:
            return None
        s = str(raw).strip().replace(",", ".")
        m = self._num.search(s)
        return float(m.group(1)) if m else None

    def parse_int(self, raw: Any) -> Optional[int]:
        """Parse an integer from raw input."""
        if raw is None:
            return None
        s = str(raw).strip().replace(",", "")
        m = self._int.search(s)
        return int(m.group(1)) if m else None

    def parse_rating(self, raw: Any) -> Optional[float]:
        """Parse a rating value."""
        if raw is None:
            return None
        s = str(raw).strip()
        # star fallback
        stars = s.count("★")
        if stars:
            return float(stars)
        return self.parse_number(s)

    def clean_text(self, raw: Any) -> Optional[str]:
        """Clean and normalize text."""
        if raw is None:
            return None
        return " ".join(str(raw).split())

    def clean_url(self, raw: Any) -> Optional[str]:
        """Clean and normalize a URL."""
        if raw is None:
            return None
        return str(raw).strip()

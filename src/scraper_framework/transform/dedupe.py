from __future__ import annotations
from typing import Dict, List, Protocol
from scraper_framework.core.models import Record
from scraper_framework.utils.hashing import stable_hash

class DedupeStrategy(Protocol):
    """Protocol for deduplication strategies."""

    def key(self, record: Record) -> str: ...
    def dedupe(self, records: List[Record]) -> List[Record]: ...


class UrlDedupeStrategy:
    """Deduplicate records by source URL."""

    def key(self, record: Record) -> str:
        """Generate dedupe key from source URL."""
        return record.source_url.strip()

    def dedupe(self, records: List[Record]) -> List[Record]:
        """Remove duplicate records based on source URL."""
        seen: Dict[str, Record] = {}
        for r in records:
            k = self.key(r)
            if k:
                seen.setdefault(k, r)
        return list(seen.values())


class HashDedupeStrategy:
    """Deduplicate records by hash of source URL or name."""

    def key(self, record: Record) -> str:
        """Generate dedupe key from hash of source URL or name."""
        basis = record.source_url or (record.fields.get("name") or "")
        return stable_hash(str(basis))

    def dedupe(self, records: List[Record]) -> List[Record]:
        """Remove duplicate records based on hash."""
        seen: Dict[str, Record] = {}
        for r in records:
            k = self.key(r)
            seen.setdefault(k, r)
        return list(seen.values())

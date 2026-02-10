from __future__ import annotations
from typing import Dict, List, Protocol
from scraper_framework.core.models import Record
from scraper_framework.utils.hashing import stable_hash
from scraper_framework.utils.logging import get_logger


class DedupeStrategy(Protocol):
    """Protocol for deduplication strategies."""

    def key(self, record: Record) -> str: ...
    def dedupe(self, records: List[Record]) -> List[Record]: ...


class UrlDedupeStrategy:
    """Deduplicate records by source URL."""

    def __init__(self):
        self.log = get_logger("scraper_framework.dedupe.url")

    def key(self, record: Record) -> str:
        """Generate dedupe key from source URL."""
        return (record.source_url or "").strip()

    def dedupe(self, records: List[Record]) -> List[Record]:
        """Remove duplicate records based on source URL."""
        seen: Dict[str, Record] = {}
        duplicates = 0

        for r in records:
            k = self.key(r)
            if not k:
                continue

            if k in seen:
                duplicates += 1
                self.log.debug("Duplicate URL skipped: %s", k)
                continue

            seen[k] = r

        result = list(seen.values())

        # Summary log (INFO, low noise)
        self.log.info(
            "URL dedupe: input=%d unique=%d removed=%d",
            len(records),
            len(result),
            duplicates,
        )

        return result


class HashDedupeStrategy:
    """Deduplicate records by hash of source URL or name."""

    def __init__(self):
        self.log = get_logger("scraper_framework.dedupe.hash")

    def key(self, record: Record) -> str:
        """Generate dedupe key from hash of source URL or name."""
        basis = record.source_url or (record.fields.get("name") or "")
        return stable_hash(str(basis))

    def dedupe(self, records: List[Record]) -> List[Record]:
        """Remove duplicate records based on hash."""
        seen: Dict[str, Record] = {}
        duplicates = 0

        for r in records:
            k = self.key(r)
            if k in seen:
                duplicates += 1
                self.log.debug("Duplicate hash skipped: %s", k)
                continue
            seen[k] = r

        result = list(seen.values())

        # Summary log
        self.log.info(
            "Hash dedupe: input=%d unique=%d removed=%d",
            len(records),
            len(result),
            duplicates,
        )

        return result

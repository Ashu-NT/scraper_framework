from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import List

from scraper_framework.core.models import Record, ScrapeJob
from scraper_framework.sinks.base import Sink
from scraper_framework.utils.logging import get_logger


class CsvSink(Sink):
    """Sink that writes records to a CSV file."""

    def __init__(self):
        self._stream_initialized = False
        self._stream_path = None
        self.log = get_logger("scraper_framework.sink.csv")

    def write(self, job: ScrapeJob, records: List[Record]) -> None:
        """Write records to CSV using the job's field_schema for column order."""
        path = job.sink_config.get("path", "output.csv")
        write_mode = str(job.sink_config.get("write_mode", "overwrite")).lower()
        execution_mode = str(getattr(job, "execution_mode", "memory")).lower()

        self._ensure_parent_dir(path)

        if write_mode not in {"overwrite", "append"}:
            raise ValueError("csv sink write_mode must be 'overwrite' or 'append'")

        # Use field_schema to determine columns (ensures consistent schema across runs)
        # Standard columns + fields from job.field_schema
        cols = ["id", "source_url", "scraped_at_utc"]
        cols += job.field_schema

        file_mode, write_header = self._resolve_file_mode(path, write_mode, execution_mode)

        with open(path, file_mode, newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            if write_header:
                w.writeheader()
            for r in records:
                row = {"id": r.id, "source_url": r.source_url, "scraped_at_utc": r.scraped_at_utc}
                row.update(r.fields)
                w.writerow(row)

        self.log.info(
            "CSV write: path=%s rows=%d execution_mode=%s write_mode=%s file_mode=%s",
            path,
            len(records),
            execution_mode,
            write_mode,
            file_mode,
        )

    def _resolve_file_mode(self, path: str, write_mode: str, execution_mode: str) -> tuple[str, bool]:
        if execution_mode == "stream":
            if self._stream_path != path:
                self._stream_initialized = False
                self._stream_path = path

            if write_mode == "overwrite":
                file_mode = "w" if not self._stream_initialized else "a"
            else:
                file_mode = "a"

            self._stream_initialized = True
            write_header = file_mode == "w" or not self._file_has_content(path)
            return file_mode, write_header

        if write_mode == "append":
            return "a", not self._file_has_content(path)

        return "w", True

    def _file_has_content(self, path: str) -> bool:
        return os.path.exists(path) and os.path.getsize(path) > 0

    def _ensure_parent_dir(self, path: str) -> None:
        parent = Path(path).parent
        if str(parent) not in {"", "."}:
            parent.mkdir(parents=True, exist_ok=True)

from __future__ import annotations
import csv
from typing import List
from scraper_framework.core.models import Record, ScrapeJob
from scraper_framework.sinks.base import Sink

class CsvSink(Sink):
    """Sink that writes records to a CSV file."""

    def write(self, job: ScrapeJob, records: List[Record]) -> None:
        """Write records to CSV using the job's field_schema for column order."""
        path = job.sink_config.get("path", "output.csv")
        
        # Use field_schema to determine columns (ensures consistent schema across runs)
        # Standard columns + fields from job.field_schema
        cols = ["id", "source_url", "scraped_at_utc"]
        cols += job.field_schema

        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in records:
                row = {"id": r.id, "source_url": r.source_url, "scraped_at_utc": r.scraped_at_utc}
                row.update(r.fields)
                w.writerow(row)

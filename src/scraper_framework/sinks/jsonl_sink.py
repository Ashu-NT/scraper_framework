import json
from typing import List
from scraper_framework.core.models import Record, ScrapeJob
from scraper_framework.sinks.base import Sink

class JsonlSink(Sink):
    """Sink that writes records to a JSONL (JSON Lines) file."""

    def write(self, job: ScrapeJob, records: List[Record]) -> None:
        """Write records to JSONL using the job's field_schema for consistent output."""
        path = job.sink_config.get("path", "output.jsonl")
        
        # Standard columns + fields from job.field_schema
        cols = ["id", "source_url", "scraped_at_utc"]
        cols += job.field_schema

        with open(path, "w", encoding="utf-8") as f:
            for r in records:
                record_dict = {"id": r.id, "source_url": r.source_url, "scraped_at_utc": r.scraped_at_utc}
                record_dict.update(r.fields)
                f.write(json.dumps(record_dict, ensure_ascii=False) + "\n")
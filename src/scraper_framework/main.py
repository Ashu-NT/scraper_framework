from __future__ import annotations

import sys
import yaml

from scraper_framework.adapters.registry import get as get_adapter
from scraper_framework.adapters.sites import register_all
from scraper_framework.core.factory import ComponentFactory
from scraper_framework.core.models import ScrapeJob, RequestSpec, DedupeMode, EnrichConfig
from scraper_framework.utils.logging import setup_logging

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ScraperFramework/0.1)"}


def load_job(path: str) -> tuple[ScrapeJob, str]:
    """
    Load a scrape job configuration from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        A tuple of (ScrapeJob, adapter_key).
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    job_cfg = cfg["job"]
    sink_cfg = cfg.get("sink", {})
    enrich_cfg = cfg.get("enrich", {})

    start = RequestSpec(
        url=job_cfg["start_url"],
        method=job_cfg.get("method", "GET"),
        headers={**DEFAULT_HEADERS, **job_cfg.get("headers", {})},
        params=job_cfg.get("params", {}),
        body=job_cfg.get("body"),
    )

    enrich = EnrichConfig(
        enabled=bool(enrich_cfg.get("enabled", False)),
        fields=set(enrich_cfg.get("fields", [])),
    )

    job = ScrapeJob(
        id=job_cfg["id"],
        name=job_cfg["name"],
        start=start,
        max_pages=int(job_cfg.get("max_pages", 5)),
        delay_ms=int(job_cfg.get("delay_ms", 800)),
        required_fields=set(job_cfg.get("required_fields", ["name", "source_url"])),
        dedupe_mode=DedupeMode(job_cfg.get("dedupe_mode", "BY_SOURCE_URL")),
        field_schema=set(job_cfg.get("field_schema", [])),
        enrich=enrich,
        sink_config=sink_cfg,
    )

    adapter_key = job_cfg["adapter"]
    return job, adapter_key


def main() -> None:
    """Main entry point for the scraper framework."""
    if len(sys.argv) < 2:
        print("Usage: scrape configs/jobs/<job>.yaml")
        raise SystemExit(2)

    setup_logging("configs/logging.yaml")
    register_all()

    job, adapter_key = load_job(sys.argv[1])
    adapter = get_adapter(adapter_key)

    factory = ComponentFactory(http_timeout_s=30)
    built = factory.build(job, adapter)

    report = built.engine.run(job)
    print("DONE:", report)


if __name__ == "__main__":
    main()

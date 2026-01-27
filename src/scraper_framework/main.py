from __future__ import annotations

import sys
import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from scraper_framework.adapters.registry import get as get_adapter
from scraper_framework.adapters.sites import register_all
from scraper_framework.core.factory import ComponentFactory
from scraper_framework.core.models import ScrapeJob, RequestSpec, DedupeMode, EnrichConfig
from scraper_framework.utils.logging import setup_logging

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ScraperFramework/0.1)"}

def load_job(path: str) -> tuple[ScrapeJob, str, dict]:
    """
    Load a scrape job configuration from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        A tuple of (ScrapeJob, adapter_key, schedule_config).
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    job_cfg = cfg["job"]
    sink_cfg = cfg.get("sink", {})
    enrich_cfg = cfg.get("enrich", {})
    
    schedule_cfg = cfg.get("schedule", {})
    enabled_schedule_cfg = bool(schedule_cfg.get("enabled", False))
    

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
    return job, adapter_key, enabled_schedule_cfg


def run_one(job: ScrapeJob, adapter_key: str) -> None:
    """Run a single scraping job."""
    setup_logging("configs/logging.yaml")
    register_all()

    adapter = get_adapter(adapter_key)

    factory = ComponentFactory(http_timeout_s=30)
    built = factory.build(job, adapter)

    report = built.engine.run(job)
    print("DONE:", report)


def run_schedule(job: ScrapeJob, adapter_key: str, schedule_cfg: dict, job_path: str) -> None:
    """Run a scheduled scraping job."""
    if not schedule_cfg:
        print("Error: No schedule configuration found in job file")
        raise SystemExit(1)

    # Set up scheduler
    scheduler = BlockingScheduler()

    # Add job to scheduler
    interval_hours = schedule_cfg.get("interval_hours", 24)
    print(f"Scheduling job every {interval_hours} hours")
    trigger = IntervalTrigger(hours=interval_hours)

    scheduler.add_job(
        run_one,
        trigger=trigger,
        args=[job, adapter_key],
        id=f"scrape_{job.id}",
        name=f"Scheduled scrape: {job.name}"
    )

    print(f"Starting scheduled scraper for job '{job.name}' (every {interval_hours} hours)")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("Scheduler stopped by user")


def main() -> None:
    """Main entry point for the scraper framework."""
    if len(sys.argv) < 2:
        print("Usage: scrape configs/jobs/<job>.yaml")
        raise SystemExit(2)

    job_path = sys.argv[1]
    print(f"Loading job from {job_path}")

    # Load job configuration
    job, adapter_key, schedule_cfg = load_job(job_path)

    # Determine mode based on schedule config presence
    if schedule_cfg:
        print("Running in scheduled mode")
        run_schedule(job, adapter_key, schedule_cfg, job_path)
    else:
        print("Running in one-time mode")
        run_one(job, adapter_key)


if __name__ == "__main__":
    main()

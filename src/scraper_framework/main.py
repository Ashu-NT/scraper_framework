from __future__ import annotations

import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pydantic import ValidationError

from scraper_framework.adapters.registry import get as get_adapter
from scraper_framework.adapters.sites import register_all
from scraper_framework.config_models import config_to_job_objects, load_and_validate_config
from scraper_framework.core.factory import ComponentFactory
from scraper_framework.core.models import ScrapeJob
from scraper_framework.utils.logging import setup_logging

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ScraperFramework/0.1)"}


def load_job(path: str) -> tuple[ScrapeJob, str, dict]:
    """
    Load and validate a scrape job configuration from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        A tuple of (ScrapeJob, adapter_key, schedule_config).

    Raises:
        ValidationError: If configuration is invalid
        FileNotFoundError: If config file doesn't exist
        ValueError: If YAML is malformed
    """
    config = load_and_validate_config(path)
    return config_to_job_objects(config)


def run_one(job: ScrapeJob, adapter_key: str) -> None:
    """Run a single scraping job."""
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

    scheduler = BlockingScheduler()

    interval_hours = schedule_cfg.get("interval_hours", 24)
    print(f"Scheduling job every {interval_hours} hours")
    trigger = IntervalTrigger(hours=interval_hours)

    scheduler.add_job(
        run_one,
        trigger=trigger,
        args=[job, adapter_key],
        id=f"scrape_{job.id}",
        name=f"Scheduled scrape: {job.name}",
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

    try:
        job, adapter_key, schedule_cfg = load_job(job_path)
    except ValidationError as e:
        print("Configuration validation failed:")
        print(str(e))
        raise SystemExit(1)
    except (FileNotFoundError, ValueError) as e:
        print(f"Configuration loading failed: {e}")
        raise SystemExit(1)

    setup_logging("configs/logging.yaml")
    register_all()

    
    if schedule_cfg:
        print("Running in scheduled mode")
        run_schedule(job, adapter_key, schedule_cfg, job_path)
    else:
        print("Running in one-time mode")
        run_one(job, adapter_key)


if __name__ == "__main__":
    main()

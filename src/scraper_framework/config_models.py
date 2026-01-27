"""
Pydantic models for YAML configuration validation.
Provides schema validation with clear error messages for job configurations.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from enum import Enum


class DedupeMode(str, Enum):
    """Enumeration for deduplication modes."""
    BY_SOURCE_URL = "BY_SOURCE_URL"
    BY_HASH = "BY_HASH"


class JobConfig(BaseModel):
    """Configuration for a scraping job."""
    id: str = Field(..., description="Unique identifier for the job")
    name: str = Field(..., description="Human-readable name for the job")
    adapter: str = Field(..., description="Adapter key to use for this job")
    start_url: str = Field(..., description="Initial URL to start scraping from")
    method: str = Field("GET", description="HTTP method to use")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    params: Dict[str, Any] = Field(default_factory=dict, description="URL query parameters")
    body: Optional[Any] = Field(None, description="Request body for POST requests")
    max_pages: int = Field(5, ge=1, le=1000, description="Maximum number of pages to scrape")
    delay_ms: int = Field(800, ge=0, le=60000, description="Delay between requests in milliseconds")
    dedupe_mode: DedupeMode = Field(DedupeMode.BY_SOURCE_URL, description="Deduplication strategy")
    required_fields: List[str] = Field(
        default_factory=lambda: ["name", "source_url"],
        description="Fields that must be present in scraped records"
    )
    field_schema: List[str] = Field(default_factory=list, description="Expected fields in records")

    @field_validator('start_url')
    @classmethod
    def validate_start_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('start_url must be a valid HTTP/HTTPS URL')
        return v

    @field_validator('required_fields')
    @classmethod
    def validate_required_fields(cls, v):
        if not v:
            raise ValueError('required_fields cannot be empty')
        return list(set(v))  # Remove duplicates


class EnrichConfig(BaseModel):
    """Configuration for enrichment features."""
    enabled: bool = Field(False, description="Whether to enable enrichment")
    fields: List[str] = Field(default_factory=list, description="Fields to enrich")

    @model_validator(mode='after')
    def validate_enrich_config(self):
        if self.enabled and not self.fields:
            raise ValueError('enrich.fields cannot be empty when enrich.enabled is True')
        return self


class CsvSinkConfig(BaseModel):
    """Configuration for CSV sink."""
    type: Literal["csv"]
    path: str = Field(..., description="Path to output CSV file")


class GoogleSheetsSinkConfig(BaseModel):
    """Configuration for Google Sheets sink."""
    type: Literal["google_sheets"]
    sheet_id: str = Field(..., description="Google Sheets ID")
    tab: str = Field(..., description="Sheet tab name")
    credentials_path: str = Field("service_account.json", description="Path to service account credentials")
    mode: str = Field("append", description="Write mode: append, replace, or upsert")
    key_field: Optional[str] = Field(None, description="Field to use as key for upsert operations")

    @field_validator('mode')
    @classmethod
    def validate_mode(cls, v):
        if v not in ["append", "replace", "upsert"]:
            raise ValueError('mode must be one of: append, replace, upsert')
        return v

    @model_validator(mode='after')
    def validate_upsert_config(self):
        if self.mode == "upsert" and not self.key_field:
            raise ValueError('key_field is required when mode is "upsert"')
        return self


class ScheduleConfig(BaseModel):
    """Configuration for scheduled execution."""
    enabled: bool = Field(False, description="Whether scheduling is enabled")
    interval_hours: int = Field(24, ge=1, le=168, description="Interval between runs in hours")

    @model_validator(mode='after')
    def validate_schedule_config(self):
        if self.enabled and self.interval_hours < 1:
            raise ValueError('interval_hours must be at least 1 when scheduling is enabled')
        return self


class ScraperConfig(BaseModel):
    """Root configuration model for scraper jobs."""
    job: JobConfig
    sink: Dict[str, Any] = Field(..., description="Sink configuration")
    enrich: EnrichConfig = Field(default_factory=EnrichConfig)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)

    @model_validator(mode='after')
    def validate_sink_config(self):
        """Validate and convert sink configuration."""
        sink_data = self.sink
        sink_type = sink_data.get('type')

        if sink_type == 'csv':
            # Validate CSV sink
            if 'path' not in sink_data:
                raise ValueError('CSV sink requires "path" field')
            # Create and assign the validated model
            self.sink = CsvSinkConfig(**sink_data)
        elif sink_type == 'google_sheets':
            # Validate Google Sheets sink
            required_fields = ['sheet_id']
            missing = [f for f in required_fields if f not in sink_data]
            if missing:
                raise ValueError(f'Google Sheets sink missing required fields: {missing}')
            # Create and assign the validated model
            self.sink = GoogleSheetsSinkConfig(**sink_data)
        else:
            raise ValueError(f'Unknown sink type: {sink_type}. Must be "csv" or "google_sheets"')

        return self

    @model_validator(mode='after')
    def validate_config_consistency(self):
        """Validate that configuration components are consistent."""
        # Check that enrich fields are in the field schema
        if self.enrich.enabled:
            missing_fields = set(self.enrich.fields) - set(self.job.field_schema)
            if missing_fields:
                raise ValueError(f'Enrich fields {missing_fields} must be declared in job.field_schema')

        return self


def load_and_validate_config(config_path: str) -> ScraperConfig:
    """
    Load and validate a scraper configuration from YAML file.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Validated ScraperConfig object

    Raises:
        ValidationError: If configuration is invalid
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If YAML is malformed
    """
    import yaml

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {config_path}: {e}")

    try:
        config = ScraperConfig(**raw_config)
        return config
    except ValidationError as e:
        # Format validation errors nicely
        error_messages = []
        for error in e.errors():
            field_path = '.'.join(str(loc) for loc in error['loc'])
            error_messages.append(f"  {field_path}: {error['msg']}")

        raise ValueError(
            f"Configuration validation failed for {config_path}:\n" +
            '\n'.join(error_messages)
        ) from e


def config_to_job_objects(config: ScraperConfig) -> tuple:
    """
    Convert validated config to the objects expected by the scraper framework.

    Returns:
        Tuple of (ScrapeJob, adapter_key, schedule_config_dict)
    """
    from scraper_framework.core.models import ScrapeJob, RequestSpec, EnrichConfig as CoreEnrichConfig

    # Convert job config
    start = RequestSpec(
        url=config.job.start_url,
        method=config.job.method,
        headers=config.job.headers,
        params=config.job.params,
        body=config.job.body,
    )

    enrich = CoreEnrichConfig(
        enabled=config.enrich.enabled,
        fields=set(config.enrich.fields),
    )

    job = ScrapeJob(
        id=config.job.id,
        name=config.job.name,
        start=start,
        max_pages=config.job.max_pages,
        delay_ms=config.job.delay_ms,
        required_fields=set(config.job.required_fields),
        dedupe_mode=config.job.dedupe_mode,
        field_schema=set(config.job.field_schema),
        enrich=enrich,
        sink_config=config.sink.model_dump(),  # Convert model back to dict for framework
    )

    adapter_key = config.job.adapter
    schedule_config = config.schedule.model_dump() if config.schedule.enabled else {}

    return job, adapter_key, schedule_config
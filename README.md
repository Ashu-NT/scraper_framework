# Scraper Framework

A config-driven scraping framework with clean extension points for:

- Static HTML scraping (Requests + BeautifulSoup)
- JSON API scraping
- Dynamic rendering with Selenium
- Optional detail-page enrichment
- Post-scrape processing pipeline (business rules + analytics)
- Output sinks: CSV, JSONL, Google Sheets
- Streaming/chunked execution for large runs

---

## Why This Project Exists

Most scraper codebases fail long-term because they become:

- tightly coupled to one website
- hard to extend for new clients
- hard to test safely
- expensive to maintain when HTML changes

This project separates responsibilities so you can scale work:

- adapters define where data is on each site
- the engine defines how scraping runs
- processing stages define business and analytics logic
- YAML defines per-client behavior without core rewrites

---

## High-Level Flow

Pipeline:

`Fetch -> Parse -> Extract -> Enrich -> Normalize -> Validate -> Dedupe -> Processing -> Sink`

Execution modes:

- `memory` (default): process all valid records at the end of the run
- `stream`: buffer records and flush by chunk (`batch_size`)

In `stream` mode each flush does:

`chunk -> dedupe (global across chunks) -> processing -> sink.write`

Global dedupe means a record seen in chunk 1 is still treated as duplicate in chunk N.

---

## Project Structure

```text
scraper_framework/
├─ configs/
│  ├─ jobs/
│  └─ logging.yaml
├─ src/scraper_framework/
│  ├─ adapters/
│  ├─ core/
│  ├─ enrich/
│  ├─ fetch/
│  ├─ http/
│  ├─ parse/
│  ├─ process/
│  ├─ sinks/
│  ├─ transform/
│  └─ utils/
├─ tests/
└─ README.md
```

---

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

For dev/test tools:

```bash
pip install -e .[dev]
```

---

## Run a Job

```bash
scrape configs/jobs/run_csv.yaml
```

Also see:

- `configs/jobs/run_processing_example.yaml`

---

## How To Scrape And Process A New Website

### 1. Inspect the target site

Identify:

- scraping mode: `STATIC_HTML`, `JSON_API`, or `DYNAMIC`
- card/listing container selector or JSON path
- selectors/paths for each field in `field_schema`
- pagination strategy

### 2. Create a site adapter

Create `src/scraper_framework/adapters/sites/<site_name>.py`:

```python
class MySiteAdapter:
    def key(self):
        return "my_site"

    def mode(self):
        return "STATIC_HTML"

    def card_locator(self):
        return ".listing-card"

    def field_locator(self, field):
        return {
            "name": ".title",
            "price": ".price",
            "detail:phone": ".phone",
        }.get(field)

    def extract_source_url(self, card, page):
        return card.get_attr("a", "href")

    def next_request(self, page, current):
        return None
```

### 3. Register the adapter

Add it to adapter registration in `src/scraper_framework/adapters/sites/__init__.py` so `register_all()` includes it.

### 4. Create a job YAML for scrape + process

Create `configs/jobs/my_site.yaml`:

```yaml
job:
  id: "my_site_job"
  name: "My Site Job"
  adapter: "my_site"
  start_url: "https://example.com/search"
  execution_mode: "stream"
  batch_size: 500
  field_schema: ["name", "price", "phone"]
  required_fields: ["name", "source_url"]

enrich:
  enabled: true
  fields: ["phone"]

processing:
  enabled: true
  schema_version: "1.0"
  stages:
    - plugin: "drop_if_field_empty"
      type: "record"
      on_error: "quarantine"
      config:
        field: "phone"

sink:
  type: "jsonl"
  path: "output_my_site.jsonl"
  write_mode: "overwrite"
```

### 5. Run it

```bash
scrape configs/jobs/my_site.yaml
```

### 6. Add custom processing logic (optional)

If built-in plugins are not enough:

1. add a new plugin class under `src/scraper_framework/process/plugins/`
2. register it in `src/scraper_framework/process/plugins/__init__.py` (`built_in_plugin_factories`)
3. reference it in YAML `processing.stages[].plugin`

---

## Job YAML Example

```yaml
job:
  id: "books_csv"
  name: "BooksToScrape to CSV"
  adapter: "books_toscrape"
  start_url: "https://books.toscrape.com/catalogue/page-1.html"
  method: "GET"
  execution_mode: "memory"      # memory | stream
  batch_size: 500               # used when execution_mode=stream
  max_pages: 2
  delay_ms: 800
  dedupe_mode: "BY_SOURCE_URL"  # BY_SOURCE_URL | BY_HASH
  required_fields: ["name", "source_url"]
  field_schema: ["name", "price", "rating"]

enrich:
  enabled: false
  fields: []

processing:
  enabled: false
  schema_version: "1.0"
  stages: []

sink:
  type: "csv"                   # csv | jsonl | google_sheets
  path: "output_books.csv"
  write_mode: "overwrite"       # csv/jsonl only: overwrite | append

schedule:
  enabled: false
  interval_hours: 24
```

---

## Streaming for Large Runs

Use chunked mode when record volume is high:

```yaml
job:
  execution_mode: "stream"
  batch_size: 500
```

Behavior in stream mode:

- records are buffered until `batch_size`
- each chunk is deduped and processed before writing
- dedupe is global across chunks (not just per chunk)
- sink writes happen incrementally
- chunk-level logs are emitted (`Chunk flushed: ...`)

---

## Processing Pipeline

Processing is optional and runs after dedupe.

```yaml
processing:
  enabled: true
  schema_version: "1.0"
  stages:
    - plugin: "drop_if_field_empty"
      type: "record"            # record | batch | analytics
      on_error: "quarantine"    # fail | skip | quarantine
      config:
        field: "phone"
    - plugin: "field_coverage_analytics"
      type: "analytics"
      on_error: "skip"
      config:
        fields: ["name", "phone", "address"]
```

Current built-in plugins:

- `drop_if_field_empty`
- `field_coverage_analytics`
- `pass_through`
- `score_lead_fit`
- `top_n_per_segment`
- `normalize_upwork_budget`
- `normalize_upwork_age`
- `client_quality_score`

Example scoring + ranking stages:

```yaml
processing:
  enabled: true
  schema_version: "1.0"
  stages:
    - plugin: "score_lead_fit"
      type: "record"
      on_error: "fail"
      config:
        weights:
          budget: 0.01
        presence_weights:
          phone: 5
        output_field: "lead_score"
    - plugin: "top_n_per_segment"
      type: "batch"
      on_error: "fail"
      config:
        segment_field: "category"
        score_field: "lead_score"
        top_n: 5
```

Example Upwork-focused stages:

```yaml
processing:
  enabled: true
  schema_version: "1.0"
  stages:
    - plugin: "normalize_upwork_budget"
      type: "record"
      on_error: "skip"
      config:
        input_field: "budget"
        hourly_to_usd_hours: 160
    - plugin: "normalize_upwork_age"
      type: "record"
      on_error: "skip"
      config:
        input_field: "posted_ago"
    - plugin: "client_quality_score"
      type: "record"
      on_error: "skip"
      config: {}
    - plugin: "score_lead_fit"
      type: "record"
      on_error: "fail"
      config:
        weights:
          budget_usd_est: 0.001
          client_quality_score: 0.5
        presence_weights:
          payment_verified: 3
        output_field: "lead_score"
```

Contracts enforced by the runner:

- schema version compatibility (`1.0`)
- idempotent plugins
- per-stage error policy (`fail`, `skip`, `quarantine`)
- per-stage metrics:
  - `records_in`
  - `records_out`
  - `dropped`
  - `errors`
  - `latency_ms`

Runtime report fields include:

- `records_quarantined`
- `processing_stage_metrics`
- `processing_artifacts`

---

## Sink Behavior

### CSV

```yaml
sink:
  type: "csv"
  path: "output.csv"
  write_mode: "overwrite"       # overwrite | append
```

### JSONL

```yaml
sink:
  type: "jsonl"
  path: "output.jsonl"
  write_mode: "overwrite"       # overwrite | append
```

### Google Sheets

```yaml
sink:
  type: "google_sheets"
  sheet_id: "YOUR_SHEET_ID"
  tab: "Leads"
  credentials_path: "service_account.json"
  mode: "append"                # append | replace | upsert
  key_field: "source_url"       # required when mode=upsert
```

Google Sheets header safety:

- if row 1 is empty, header is created
- if row 1 matches expected header, write continues
- if row 1 mismatches, write fails fast (no destructive clear)

Note:

- `replace` is accepted by config validation. Current sink implementation writes like append unless `upsert` is selected.

---

## Enrichment

Enable detail-page enrichment:

```yaml
enrich:
  enabled: true
  fields: ["phone", "website", "address"]
```

For enrichable fields, adapter selectors should use `detail:` prefixes, for example:

- `detail:phone`
- `detail:website`
- `detail:address`

---

## Dynamic Scraping (Selenium)

Use adapter mode `DYNAMIC` for JS-rendered pages.

Common `job.params` keys:

- `wait_selector`
- `wait_time`
- `click_selectors`

---

## Config Validation (Pydantic)

Validation includes:

- required fields and types
- URL checks for `start_url`
- enum checks:
  - `dedupe_mode`
  - `execution_mode`
  - sink `type`
  - processing stage `type`
  - processing stage `on_error`
  - sink `write_mode` (csv/jsonl)
- range checks (`max_pages`, `delay_ms`, `batch_size`, `interval_hours`)
- cross-field checks (for example, `enrich.fields` in `field_schema`)

---

## Full Config Schema (Quick Reference)

```yaml
job:
  id: string
  name: string
  adapter: string
  start_url: http(s) url
  method: string = GET
  headers: dict = {}
  params: dict = {}
  body: any = null
  execution_mode: memory|stream = memory
  batch_size: int(1..100000) = 500
  max_pages: int(1..1000) = 5
  delay_ms: int(0..60000) = 800
  dedupe_mode: BY_SOURCE_URL|BY_HASH = BY_SOURCE_URL
  required_fields: [string]
  field_schema: [string]

sink:
  # CSV
  type: csv
  path: string
  write_mode: overwrite|append = overwrite
  # JSONL
  type: jsonl
  path: string
  write_mode: overwrite|append = overwrite
  # Google Sheets
  type: google_sheets
  sheet_id: string
  tab: string
  credentials_path: string = service_account.json
  mode: append|replace|upsert = append
  key_field: string (required if mode=upsert)

enrich:
  enabled: bool = false
  fields: [string] = []

processing:
  enabled: bool = false
  schema_version: "1.0"
  stages:
    - plugin: string
      type: record|batch|analytics = record
      on_error: fail|skip|quarantine = fail
      config: dict = {}

schedule:
  enabled: bool = false
  interval_hours: int(1..168) = 24
```

---

## Logging

Configured in `configs/logging.yaml`.

Logs include:

- page fetch progress
- cards found
- chunk flush summaries (stream mode)
- sink write summaries
- records emitted/skipped/quarantined
- processing stage failures and timings

---

## Reliability Notes

- retry + backoff in HTTP layer
- rate limiting between requests
- dedupe strategies (`BY_SOURCE_URL`, `BY_HASH`)
- non-destructive Google Sheets header handling
- processing error policies (`fail`, `skip`, `quarantine`)

---

## GitHub Actions

Automate scheduled/manual runs using:

- `scheduled-scraping.yml`
- `manual-scraping.yml`

See `GITHUB_ACTIONS_SETUP.md` for setup details.

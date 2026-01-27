# Scraper Framework

A **config-driven, extensible web scraping framework** built with clean architecture principles.  
Supports **HTML websites**, **JS-heavy sites via hidden APIs**, **Google Sheets / CSV outputs**, and **optional detail-page enrichment** — without changing core code.

---

## 🚀 What this project solves

Most scraping projects fail because they are:
- tightly coupled to one website
- hard to maintain when HTML changes
- written as one-off scripts
- impossible to extend cleanly

This framework solves that by:
- separating **what to scrape** (config)
- from **how to scrape** (engine)
- from **where data lives on a site** (adapter)

You can add a **new site** by writing a single adapter file — or later, only by editing config.

---

## 🧠 Core Concepts (High-Level)

| Concept | Responsibility |
|------|---------------|
| ScrapeJob | Defines what to scrape (URLs, fields, limits) |
| ScrapeEngine | Orchestrates the scraping pipeline |
| FetchStrategy | Downloads pages (HTML or JSON API) |
| PageParser | Converts pages into repeated containers (“cards”) |
| SiteAdapter | Knows where data lives on a specific site |
| Record | Generic output model |
| Normalizer | Cleans messy data |
| Validator | Ensures required fields exist |
| DedupeStrategy | Removes duplicates |
| Enricher | Optional detail-page scraping |
| Sink | Writes output (CSV / Google Sheets) |

---

## 🧱 Architecture Overview

Pipeline:

Fetch → Parse → Extract → Enrich → Normalize → Validate → Dedupe → Sink

Design principles:
- Open / Closed
- Single Responsibility
- Adapter Pattern
- Strategy Pattern

---

## 📁 Project Structure

```
scraper_framework/
├─ configs/
│  ├─ jobs/
│  └─ logging.yaml
├─ src/scraper_framework/
│  ├─ core/
│  ├─ fetch/
│  ├─ http/
│  ├─ parse/
│  ├─ adapters/
│  ├─ transform/
│  ├─ enrich/
│  ├─ sinks/
│  └─ utils/
├─ tests/
└─ README.md
```

---

## ⚙️ Installation (first time only)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

This tells Python to load the package from `src/`.

---

## ▶️ Running a Scraping Job

### Step 1: Create or choose a job config

Job configs live in:
```
configs/jobs/
```

Example: `configs/jobs/run_csv.yaml`

```yaml
job:
  id: "books_csv"
  name: "BooksToScrape to CSV"
  adapter: "books_toscrape"
  start_url: "https://books.toscrape.com/catalogue/page-1.html"
  max_pages: 2
  delay_ms: 800
  dedupe_mode: "BY_SOURCE_URL"
  required_fields: ["name", "source_url"]
  field_schema: ["name", "price", "rating"]

enrich:
  enabled: false

sink:
  type: "csv"
  path: "output_books.csv"
```

### Step 2: Run the job

From the project root:

```bash
scrape configs/jobs/run_csv.yaml
```

Output:
```
output_books.csv
```

### Job Flow

```
scrape (CLI command)
  ↓
scraper_framework.main.main()
  ↓
load YAML
  ↓
factory builds components
  ↓
engine.run(job)
```

---

## 📤 Output options

### CSV (local file)

```yaml
sink:
  type: "csv"
  path: "output.csv"
```

### Google Sheets

```yaml
sink:
  type: "google_sheets"
  sheet_id: "YOUR_SHEET_ID"
  tab: "Leads"
  credentials_path: "service_account.json"
  mode: "upsert"
  key_field: "source_url"
```

#### Google Sheets setup (one-time)

1. Create a Google Cloud **Service Account**
2. Download JSON key → `service_account.json`
3. Share the target Google Sheet with the service account email

---

## 🧩 Site Adapters

Adapters isolate site-specific logic:
- card location
- field extraction
- pagination
- optional detail selectors

Adding a new site usually means adding **one file**.

---

## 🧩 How to scrape a NEW website (most important section)

### Step 1: Inspect the site

Open DevTools and identify:
- the **repeated container** (listing card)
- where fields live inside that container

### Step 2: Create a site adapter

Create a new file:
```
src/scraper_framework/adapters/sites/my_site.py
```

You only define selectors and pagination:

```python
class MySiteAdapter(SiteAdapter):
    def key(self):
        return "my_site"

    def mode(self):
        return "STATIC_HTML"

    def card_locator(self):
        return ".listing-card"

    def field_locator(self, field):
        return {
            "name": "h2",
            "price": ".price",
            "rating": ".rating",
            "detail:phone": ".phone",
        }.get(field)

    def extract_source_url(self, card, page):
        return card.get_attr("a", "href")

    def next_request(self, page, current):
        ...
```

Register it in:
```
src/scraper_framework/adapters/sites/__init__.py
```

### Step 3: Create a YAML job for that adapter

```yaml
job:
  adapter: "my_site"
  start_url: "https://example.com/search"
  field_schema: ["name", "price", "rating"]
```

Run it:

```bash
scrape configs/jobs/my_site.yaml
```

---

## 🧠 Enrichment (detail pages)

Some sites hide data (phone, website) on detail pages.

Enable enrichment:

```yaml
enrich:
  enabled: true
  fields: ["phone", "website"]
```

Add detail selectors in adapter using:

```
detail:<field>
```

Example:

```python
"detail:phone": ".phone, a[href^='tel:']"
```

The engine automatically fetches detail pages and fills missing fields.

---

## 🧼 Normalization

Centralized cleanup:
- ratings (4.7, ★★★★★, Rated 4.7/5)
- reviews (1,234 / 1.2k)
- prices ($12.99 / EUR 12,99)
- phones, URLs, text cleanup

---

## ⏰ Scheduled Scraping

For recurring data collection, add a `schedule` section to your job YAML:

```yaml
job:
  # ... job config ...

schedule:
  enabled: true
  interval_hours: 24  # Run every 24 hours
```

Run the scheduled job:

```bash
scrape configs/jobs/scheduled_job.yaml
```

The framework automatically detects scheduled jobs and runs them continuously. Stop with Ctrl+C.

---

## 🏗 Factory Layer

All dependency wiring lives in one place, keeping the entrypoint clean and testable.

---

## 🛡️ Configuration Validation

The framework uses **Pydantic** for robust YAML configuration validation with clear error messages:

### Benefits

- **🛡️ Runtime Safety**: Catch configuration errors before scraping starts
- **📝 Clear Messages**: Field-specific error messages with suggestions
- **🔧 Developer Experience**: IDE autocompletion and type hints
- **📚 Self-Documenting**: Models serve as configuration documentation
- **🚀 Production Ready**: Prevents runtime failures from bad configs

### Validation Features

- ✅ **Required fields** validation
- ✅ **Type checking** (strings, integers, enums)
- ✅ **URL validation** for start URLs
- ✅ **Enum validation** for dedupe modes and sink types
- ✅ **Range validation** for delays and page limits
- ✅ **Cross-field validation** (enrich fields must be in schema)
- ✅ **Clear error messages** with field paths

### Validation Features

- ✅ **Required fields** validation
- ✅ **Type checking** (strings, integers, enums)
- ✅ **URL validation** for start URLs
- ✅ **Enum validation** for dedupe modes and sink types
- ✅ **Range validation** for delays and page limits
- ✅ **Cross-field validation** (enrich fields must be in schema)
- ✅ **Clear error messages** with field paths

### Testing Validation

Run the validation test script to see examples:

```bash
python test_validation.py
```

This demonstrates:
- ✅ Valid configurations pass
- ❌ Invalid configurations fail with clear messages
- 🔍 Various validation scenarios (missing fields, invalid enums, etc.)

**Example Error Output:**
```bash
❌ Configuration validation failed:
  job.dedupe_mode: Input should be 'BY_SOURCE_URL' or 'BY_HASH'
  job.max_pages: Input should be less than or equal to 1000
  sink.type: Unknown sink type: 'invalid'. Must be "csv" or "google_sheets"
```

### Configuration Schema

```yaml
job:
  id: string (required)           # Unique job identifier
  name: string (required)         # Human-readable name
  adapter: string (required)      # Adapter to use
  start_url: url (required)       # Must be http/https
  max_pages: int (1-1000)         # Default: 5
  delay_ms: int (0-60000)         # Default: 800
  dedupe_mode: enum               # BY_SOURCE_URL or BY_HASH
  required_fields: [string]       # Default: ["name", "source_url"]
  field_schema: [string]          # Expected output fields

sink:  # Required
  type: enum (csv|google_sheets)
  # CSV options
  path: string
  # Google Sheets options
  sheet_id: string
  tab: string
  credentials_path: string
  mode: enum (append|replace|upsert)
  key_field: string  # Required for upsert

enrich:  # Optional
  enabled: bool
  fields: [string]  # Must be in field_schema if enabled

schedule:  # Optional
  enabled: bool
  interval_hours: int (1-168)
```

---

## 📊 Logging

Logging is configured in:

```
configs/logging.yaml
```

Logs show:
- page fetch progress
- cards found
- records emitted / skipped
- enrichment failures

---

## 📊 Reliability

- structured logging
- retry with backoff
- rate limiting
- failure reporting

---

## 🚀 GitHub Actions Integration

Run scheduled scraping jobs automatically using GitHub Actions:

### Automated Scheduling

The `scheduled-scraping.yml` workflow runs on a cron schedule:

```yaml
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM UTC
```

### Manual Execution

Use `manual-scraping.yml` for on-demand runs:

- Go to Actions tab → Manual Scraping → Run workflow
- Choose your job configuration file
- Select execution mode (one-time or scheduled)

### Setup Requirements

1. **Google Sheets (optional)**: Add `GOOGLE_SHEETS_CREDENTIALS` secret
2. **Schedule customization**: Edit cron expression in workflow file
3. **Job configurations**: Create YAML files in `configs/jobs/`

See [GITHUB_ACTIONS_SETUP.md](GITHUB_ACTIONS_SETUP.md) for detailed setup instructions.

### Benefits

- ✅ No server maintenance
- ✅ Automatic result commits
- ✅ Artifact storage for logs
- ✅ Free for public repos
- ✅ Manual override capability

---

## 🎯 Typical usage flow

1. Choose a website
2. Write a small adapter (selectors only)
3. Create a YAML job
4. Run the job (one-time or scheduled)
5. Deliver CSV / Google Sheet

This mirrors real-world client work.

---

## 🏁 Summary

This framework is:
- reusable
- testable
- config-driven
- production-oriented

It is designed to scale beyond one-off scripts.

---

If you are using this as a portfolio project, this README is intentionally written to be **client-readable and engineer-respectable**.

# Client Quickstart (Docker)

This guide lets a non-technical client run the scraper in one command.

## 1. Prerequisites

- Install Docker Desktop
- Open terminal in the project root

## 2. Configure the job

Edit `configs/jobs/client_template.yaml`:

- `job.adapter`: your adapter key
- `job.start_url`: target URL
- `job.field_schema`: required output fields
- `processing` stages (optional)
- `sink` output target

If using Google Sheets, set:

- `sink.type: google_sheets`
- `sink.sheet_id`
- `sink.tab`
- `sink.credentials_path: /app/secrets/service_account.json`

Then place credentials at:

- `secrets/service_account.json`

## 3. Run in one command

### PowerShell (Windows)

```powershell
.\scripts\run-client-docker.ps1
```

### Bash (Mac/Linux)

```bash
./scripts/run-client-docker.sh
```

## 4. Run a different config

### PowerShell

```powershell
.\scripts\run-client-docker.ps1 configs/jobs/my_job.yaml
```

### Bash

```bash
./scripts/run-client-docker.sh configs/jobs/my_job.yaml
```

## 5. Output location

- File sinks (CSV/JSONL) write to `output/`
- Google Sheets writes directly to your configured spreadsheet

## 6. Security notes

- Do not commit `secrets/`, `.env`, or real credentials
- Keep client-specific sensitive configs in a private repo

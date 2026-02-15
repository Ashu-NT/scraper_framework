# Client Quickstart (Prebuilt Docker Image)

This setup is for clients who should run the framework without building from source.

## 1. For framework owner: publish image once

Build and push a versioned image:

```bash
docker build -t ghcr.io/your-org/scraper-framework-client:1.0.0 .
docker push ghcr.io/your-org/scraper-framework-client:1.0.0
```

Use your real registry/repo/tag.

## 2. What to share with client

Share only:

- `docker-compose.client.yml`
- `scripts/run-client-docker.ps1`
- `scripts/run-client-docker.sh`
- `configs/jobs/client_template.yaml` (or client-specific YAML)
- `README_CLIENT.md`

## 3. Client prerequisites

- Install Docker Desktop
- Open terminal in the folder containing the files above

## 4. Set image tag

Option A: create `.env` beside `docker-compose.client.yml`:

```env
SCRAPER_IMAGE=ghcr.io/your-org/scraper-framework-client:1.0.0
```

Option B: pass image tag when running script.

## 5. Configure job YAML

Edit `configs/jobs/client_template.yaml`:

- `job.adapter`
- `job.start_url`
- `job.field_schema`
- `processing` (optional)
- `sink`

For file sinks, keep output under `output/` (example: `output/results.csv`).

For Google Sheets:

- `sink.type: google_sheets`
- `sink.sheet_id`
- `sink.tab`
- `sink.credentials_path: /app/secrets/service_account.json`

Put credentials in `secrets/service_account.json`.

## 6. Run one command

PowerShell:

```powershell
.\scripts\run-client-docker.ps1
```

Bash:

```bash
./scripts/run-client-docker.sh
```

Run custom config:

```powershell
.\scripts\run-client-docker.ps1 configs/jobs/my_job.yaml
```

```bash
./scripts/run-client-docker.sh configs/jobs/my_job.yaml
```

Override image tag directly:

```powershell
.\scripts\run-client-docker.ps1 configs/jobs/my_job.yaml ghcr.io/your-org/scraper-framework-client:1.0.1
```

```bash
./scripts/run-client-docker.sh configs/jobs/my_job.yaml ghcr.io/your-org/scraper-framework-client:1.0.1
```

## 7. Where output goes

- CSV/JSONL files are written to local `output/`
- Google Sheets writes directly to the configured sheet

## 8. Update and rollback

- Update: change image tag (for example `1.0.0` -> `1.1.0`) and rerun
- Rollback: switch back to previous tag and rerun

## 9. Security notes

- Do not commit `secrets/`, `.env`, or credentials
- Keep client-specific configs private when they include sensitive URLs or keys

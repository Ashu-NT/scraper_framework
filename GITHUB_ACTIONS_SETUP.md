# GitHub Actions Setup Guide

## Setting up Scheduled Scraping with GitHub Actions

### 1. Repository Setup

Ensure your repository has:
- `.github/workflows/scheduled-scraping.yml`
- `.github/workflows/manual-scraping.yml`
- Job configuration files in `configs/jobs/`

### 2. Google Sheets Integration (Optional)

If your jobs use Google Sheets output:

1. **Create a Service Account**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing
   - Enable Google Sheets API
   - Create a Service Account with Editor permissions

2. **Generate Credentials**:
   - Download the JSON key file
   - Base64 encode the entire JSON content:
     ```bash
     cat credentials.json | base64 -w 0
     ```

3. **Add to GitHub Secrets**:
   - Go to your repository → Settings → Secrets and variables → Actions
   - Add new secret: `GOOGLE_SHEETS_CREDENTIALS`
   - Paste the base64-encoded JSON

### 3. Customize Schedule

Edit `.github/workflows/scheduled-scraping.yml`:

```yaml
schedule:
  - cron: '0 2 * * *'  # Daily at 2 AM UTC
```

Common cron expressions:
- `'0 */6 * * *'` - Every 6 hours
- `'0 0 * * *'` - Daily at midnight
- `'0 0 * * 1'` - Weekly on Monday

### 4. Job Configuration

Create scheduled job files in `configs/jobs/` with a `schedule` section:

```yaml
job:
  # ... job configuration ...

schedule:
  enabled: true
  interval_hours: 24
```

### 5. Testing

- **Manual runs**: Use the "Manual Scraping" workflow
- **Scheduled runs**: Push to main branch and wait for schedule
- **Local testing**: Run `scrape configs/jobs/your_job.yaml`

### 6. Monitoring

- Check Actions tab for run history
- Download artifacts for logs and results
- Monitor commit history for automatic data updates

## Troubleshooting

- **Authentication errors**: Verify Google Sheets credentials
- **Import errors**: Check Python dependencies in workflow
- **Schedule not running**: Ensure workflow is on default branch
- **Results not committing**: Check if output files changed
#!/usr/bin/env sh
set -eu

JOB_CONFIG="${1:-configs/jobs/client_template.yaml}"
docker compose -f docker-compose.client.yml run --rm scraper "$JOB_CONFIG"

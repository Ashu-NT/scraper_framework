#!/usr/bin/env sh
set -eu

JOB_CONFIG="${1:-configs/jobs/client_template.yaml}"
IMAGE="${2:-}"

if [ -n "$IMAGE" ]; then
  export SCRAPER_IMAGE="$IMAGE"
fi

docker compose -f docker-compose.client.yml pull scraper
docker compose -f docker-compose.client.yml run --rm scraper "$JOB_CONFIG"

param(
    [string]$JobConfig = "configs/jobs/client_template.yaml"
)

docker compose -f docker-compose.client.yml run --rm scraper $JobConfig

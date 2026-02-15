param(
    [string]$JobConfig = "configs/jobs/client_template.yaml",
    [string]$Image = ""
)

if ($Image) {
    $env:SCRAPER_IMAGE = $Image
}

docker compose -f docker-compose.client.yml pull scraper
docker compose -f docker-compose.client.yml run --rm scraper $JobConfig

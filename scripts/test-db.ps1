# Starts a throwaway Postgres for the test suite and sets the variable the
# tests read. Each test creates and drops its own schema inside it.
$name = 'crashsafe-pg'
$port = if ($env:PORT) { $env:PORT } else { '55432' }

docker inspect $name *> $null
if ($LASTEXITCODE -ne 0) {
    docker run -d --name $name -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=gateway `
        -p "127.0.0.1:${port}:5432" postgres:17-alpine | Out-Null
}
docker start $name | Out-Null

do {
    Start-Sleep -Milliseconds 500
    docker exec $name pg_isready -U postgres *> $null
} until ($LASTEXITCODE -eq 0)

$env:GATEWAY_TEST_DATABASE_URL = "postgres://postgres:postgres@127.0.0.1:${port}/gateway?sslmode=disable"
Write-Output "GATEWAY_TEST_DATABASE_URL=$env:GATEWAY_TEST_DATABASE_URL"

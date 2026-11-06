#!/usr/bin/env sh
# Starts a throwaway Postgres for the test suite and prints the variable to
# export. Each test creates and drops its own schema inside it.
set -eu

name=crashsafe-pg
port=${PORT:-55432}

if ! docker inspect "$name" >/dev/null 2>&1; then
  docker run -d --name "$name" \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=gateway \
    -p "127.0.0.1:$port:5432" postgres:17-alpine >/dev/null
fi
docker start "$name" >/dev/null

until docker exec "$name" pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done

echo "export GATEWAY_TEST_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:$port/gateway?sslmode=disable'"

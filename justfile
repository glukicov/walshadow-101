# Mini physical-WAL decoder demo + the real walshadow quickstart
set shell := ["bash", "-euo", "pipefail", "-c"]

pg_image := "postgres:17"
compose := "docker compose -f upstream/walshadow/docker/docker-compose.yml -f upstream/walshadow/docker/docker-compose.quickstart.yml -f docker/macos-override.yml"

default:
    @just --list

# --- Part A: hand-rolled WAL decoder (needs only Docker + uv) ---------------

# start a throwaway Postgres 17 on :5544 (wal_level=logical)
pg-up level="logical":
    -docker rm -f wal101-pg >/dev/null 2>&1
    docker run -d --name wal101-pg -e POSTGRES_PASSWORD=postgres -p 5544:5432 {{pg_image}} \
        -c wal_level={{level}} -c max_wal_senders=4 -c max_replication_slots=4 >/dev/null
    until docker exec wal101-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done
    sleep 1

pg-down:
    -docker rm -f wal101-pg

# run the demo with wal_level=logical
demo: (pg-up "logical")
    uv run python demo/wal101.py

# same demo with wal_level=replica: watch DELETE lose its key and UPDATE lose its tuple
demo-replica: (pg-up "replica")
    uv run python demo/wal101.py

# --- Part B: the real thing (builds a Rust image, ~5-10 min first time) -----

# clone upstream (gitignored) with its submodules
ws-clone:
    mkdir -p upstream
    test -d upstream/walshadow || git clone --recurse-submodules https://github.com/ClickHouse/walshadow.git upstream/walshadow

ws-build:
    {{compose}} build walshadow

# seed PG18 + ClickHouse, select all tables, start replication, wait for bootstrap
ws-up:
    {{compose}} run --rm walshadow init --all-tables
    {{compose}} up -d
    until {{compose}} logs walshadow 2>&1 | grep -q "shadow caught up to bootstrap end_lsn"; do sleep 2; done
    @echo "ready: just ws-update"

# update a row in Postgres, wait for it to land in ClickHouse, print it
ws-update email="alice@walshadow.dev":
    {{compose}} exec -T postgres psql -U postgres -qc "UPDATE public.users SET email='{{email}}' WHERE id=1"
    until [ "$({{compose}} exec -T clickhouse clickhouse-client --query "SELECT email FROM walshadow.users FINAL WHERE id=1")" = "{{email}}" ]; do sleep 0.1; done
    {{compose}} exec -T clickhouse clickhouse-client --query "SELECT id, name, email FROM walshadow.users FINAL ORDER BY id"

ws-logs:
    {{compose}} logs -f walshadow

ws-down:
    {{compose}} down -v

test:
    uv run pytest -q

check:
    uv run ruff check . && uv run ruff format --check . && uv run ty check && uv run pytest -q

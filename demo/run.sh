#!/usr/bin/env bash
# Guided demo: starts the stack, walks through the main behaviours and checks each one.
# Needs only Docker. Takes about two minutes on the first run (image builds).
set -euo pipefail
cd "$(dirname "$0")/.."

STATE="$PWD/.demo-state"   # inside the project: Docker Desktop/Colima only share the home directory
rm -rf "$STATE" && mkdir -p "$STATE" && echo 1 > "$STATE/next_step"
py() {
  docker run --rm --network mailq_default -v "$PWD/demo:/demo:ro" -v "$STATE:/tmp" -w /demo \
    -e DEMO_STEP="$(cat "$STATE/next_step")" python:3.13-alpine python -u demo.py "$1"
}
quiet() { "$@" >/dev/null 2>&1; }

echo "Starting the stack (API, 2 workers, Postgres, RabbitMQ, Mailpit)…"
quiet docker compose up -d --build --wait
py basics
py priority
py failures

quiet docker compose stop rabbitmq
py outbox_send
quiet docker compose start rabbitmq
# Workers exit when they lose the broker (crash-only) and are restarted by Docker.
quiet docker compose up -d --wait
py outbox_check
rm -rf "$STATE"
echo "Explore: Mailpit http://localhost:8025 · RabbitMQ http://localhost:15672 (mail/mail) · stop → make down"

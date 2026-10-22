#!/usr/bin/env bash
# Chaos test: emails keep arriving while workers are SIGKILLed every 4 s, the API and
# RabbitMQ are killed once each, and 5% of SMTP calls fail with 451.
# Checks after everything settles:
#   - no email left queued, sending or retrying, none failed
#   - every email marked sent is in the SMTP sink (nothing lost)
#   - duplicates are counted and reported: email is at-least-once by nature (a worker
#     killed after the SMTP server accepted a message but before it was marked sent
#     will send it again)
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${DURATION:-60}
RESULTS="$PWD/bench/results"
mkdir -p "$RESULTS"
export RATE_BULK=500

sql() { docker compose exec -T postgres psql -U mail -d mail -tAc "$1"; }
chaos() {
  curl -sf -XPUT localhost:8025/api/v1/chaos -H 'content-type: application/json' \
    -d "{\"Recipient\":{\"ErrorCode\":451,\"Probability\":$1}}" >/dev/null
}

docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build --wait >/dev/null
chaos 5
echo "stack up; load for ${DURATION}s"

docker run --rm --network mailq_default -v "$PWD":/app -w /app -e API_URL=http://api:3000 -e SECONDS="$DURATION" \
  node:24-alpine npx tsx bench/chaos-load.ts 2>/dev/null > "$RESULTS/chaos-load.txt" &
LOAD=$!

kills=0
api_killed=no
rabbit_killed=no
end=$((SECONDS + DURATION - 5))
while [ $SECONDS -lt $end ]; do
  sleep 4
  ids=($(docker compose ps -q worker))
  victim=${ids[$((RANDOM % ${#ids[@]}))]}
  docker kill -s KILL "$victim" >/dev/null && kills=$((kills + 1))
  ( sleep 2; docker start "$victim" >/dev/null 2>&1 || true ) &
  remaining=$((end - SECONDS))
  if [ "$api_killed" = no ] && [ $remaining -lt $((DURATION * 2 / 3)) ]; then
    echo "killing the API"
    docker compose kill -s KILL api >/dev/null; sleep 2; docker compose start api >/dev/null
    api_killed=yes
  fi
  if [ "$rabbit_killed" = no ] && [ $remaining -lt $((DURATION / 3)) ]; then
    echo "killing RabbitMQ"
    docker compose kill -s KILL rabbitmq >/dev/null; sleep 5; docker compose start rabbitmq >/dev/null
    rabbit_killed=yes
  fi
done
wait $LOAD
wait
chaos 0
docker compose up -d --wait >/dev/null   # workers exit when RabbitMQ goes away; bring every one back
echo "load done; killed $kills workers, the API and RabbitMQ; waiting for retries (the ladder goes up to 5 min)"

for _ in $(seq 1 240); do
  open=$(sql "SELECT count(*) FROM emails WHERE status IN ('queued', 'sending', 'retrying')")
  [ "$open" = "0" ] && break
  sleep 2
done

accepted=$(grep -o 'ACCEPTED [0-9]*' "$RESULTS/chaos-load.txt" | cut -d' ' -f2)
total=$(sql "SELECT count(*) FROM emails")
sent=$(sql "SELECT count(*) FROM emails WHERE status = 'sent'")
failed=$(sql "SELECT count(*) FROM emails WHERE status = 'failed'")
open=$(sql "SELECT count(*) FROM emails WHERE status IN ('queued', 'sending', 'retrying')")
retried=$(sql "SELECT count(*) FROM emails WHERE attempts > 1")
republished=$(docker compose logs worker 2>/dev/null | grep -c 'outbox republished' || true)

# Every message the SMTP sink received, by recipient.
docker run --rm --network mailq_default python:3.13-alpine python -c '
import json, urllib.request, collections
counts = collections.Counter()
start = 0
while True:
    page = json.load(urllib.request.urlopen(f"http://mailpit:8025/api/v1/messages?start={start}&limit=1000"))
    for m in page["messages"]:
        for to in m["To"]:
            counts[to["Address"]] += 1
    start += len(page["messages"])
    if not page["messages"] or start >= page["total"]:
        break
for addr, n in counts.items():
    print(addr, n)
' > "$RESULTS/delivered.txt"
sql "SELECT to_address FROM emails WHERE status = 'sent'" | sort > "$RESULTS/sent.txt"
cut -d' ' -f1 "$RESULTS/delivered.txt" | sort > "$RESULTS/received.txt"
lost=$(comm -23 "$RESULTS/sent.txt" "$RESULTS/received.txt" | wc -l | tr -d ' ')
dupes=$(awk '$2 > 1 {d += $2 - 1} END {print d + 0}' "$RESULTS/delivered.txt")

ok=yes
for v in "$open" "$failed" "$lost"; do [ "$v" = "0" ] || ok=no; done
[ "$accepted" -le "$total" ] || ok=no

{
  echo "Load: ${DURATION}s of transactional emails (8 senders, Idempotency-Key) plus a 500-recipient bulk batch every 5 s. SIGKILLed $kills worker containers (every 4 s), the API once and RabbitMQ once; 5% of SMTP calls failed with 451 during the load."
  echo
  echo "| emails accepted | stored | sent | needed a retry | failed | not finished | sent but missing from the SMTP sink | extra copies delivered |"
  echo "|---:|---:|---:|---:|---:|---:|---:|---:|"
  echo "| $accepted | $total | $sent | $retried | **$failed** | **$open** | **$lost** | $dupes |"
  echo
  echo "Extra copies: email is at-least-once. A worker killed after the SMTP server accepted a message but before the row was marked sent sends it again on redelivery. With $kills kills this happened $dupes times out of $sent emails."
  echo
  [ "$ok" = yes ] && echo "**PASS**" || echo "**FAIL**"
} | tee "$RESULTS/chaos.md"
[ "$ok" = yes ]

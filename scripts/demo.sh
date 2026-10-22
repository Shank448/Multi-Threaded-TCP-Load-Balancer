#!/usr/bin/env bash
# End-to-end demo: 3 backends behind the load balancer, then a backend "crash".
set -euo pipefail
cd "$(dirname "$0")/.."
make -s all
B=build
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

for port in 9001 9002 9003; do
  $B/echo_backend --port $port --name "backend-$port" >/dev/null 2>&1 & PIDS+=($!)
done
sleep 0.3
$B/lb --port 8080 --strategy lc --health-interval 500 --stats-interval 0 \
      --backend 127.0.0.1:9001 --backend 127.0.0.1:9002 --backend 127.0.0.1:9003 2>build/lb.log & LB=$!
PIDS+=($LB)
sleep 0.5

echo "=== 1) 20 clients x 200 requests through the balancer"
$B/load_client --port 8080 --clients 20 --requests 200

echo; echo "=== 2) crash backend-9002, then send the same load again"
kill "${PIDS[1]}"; sleep 0.1
$B/load_client --port 8080 --clients 20 --requests 200

echo; echo "=== 3) graceful shutdown + final stats (from the LB's own log)"
kill -TERM $LB; wait $LB || true
grep -v DEBUG build/lb.log | sed 's/^/   /'

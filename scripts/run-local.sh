#!/usr/bin/env bash
# Launch a local 3-node JamRaft cluster as separate processes (no Docker needed).
#
#   ./scripts/run-local.sh
#   open http://localhost:8080
#
# Ctrl-C stops all nodes.
set -euo pipefail

cd "$(dirname "$0")/.."

N=${N:-3}
GRPC_BASE=${GRPC_BASE:-7000}
HTTP_BASE=${HTTP_BASE:-8080}
DATA_DIR=${DATA_DIR:-./data}

# Build the peer lists.
PEERS=""
HTTP_PEERS=""
for i in $(seq 0 $((N - 1))); do
  sep=""
  [ -n "$PEERS" ] && sep=","
  PEERS="${PEERS}${sep}n${i}=localhost:$((GRPC_BASE + i))"
  HTTP_PEERS="${HTTP_PEERS}${sep}n${i}=http://localhost:$((HTTP_BASE + i))"
done

echo "Building jamnode..."
go build -o ./bin/jamnode ./cmd/node

mkdir -p "$DATA_DIR"
pids=()
cleanup() {
  echo
  echo "Stopping nodes..."
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for i in $(seq 0 $((N - 1))); do
  ./bin/jamnode \
    -id "n${i}" \
    -grpc ":$((GRPC_BASE + i))" \
    -http ":$((HTTP_BASE + i))" \
    -peers "$PEERS" \
    -http-peers "$HTTP_PEERS" \
    -data "${DATA_DIR}/n${i}" &
  pids+=("$!")
  echo "started n${i}: gRPC :$((GRPC_BASE + i))  HTTP http://localhost:$((HTTP_BASE + i))"
done

echo
echo "Cluster up. Open http://localhost:${HTTP_BASE}"
echo "Press Ctrl-C to stop."
wait

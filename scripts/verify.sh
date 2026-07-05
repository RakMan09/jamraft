#!/usr/bin/env bash
# One command that proves JamRaft works: it builds, runs the full test suite
# (unit + deterministic simulator + Jepsen chaos), and runs a batch of
# randomized fault-injected histories checked for linearizability.
#
#   ./scripts/verify.sh            # ~1-2 minutes
#   HISTORIES=500 ./scripts/verify.sh   # heavier chaos batch
set -euo pipefail
cd "$(dirname "$0")/.."

HISTORIES=${HISTORIES:-100}

echo "==============================================="
echo " JamRaft verification"
echo "==============================================="

echo
echo ">> [1/3] Building (native binary + WebAssembly demo)"
go build ./...
GOOS=js GOARCH=wasm go build -o /dev/null ./cmd/wasm
echo "   build OK"

echo
echo ">> [2/3] Running tests (unit + network simulator + chaos)"
go test ./... -count=1

echo
echo ">> [3/3] Linearizability under randomized faults ($HISTORIES histories)"
go run ./cmd/chaos -histories "$HISTORIES" -clients 2 -ops 25

echo
echo "==============================================="
echo " ALL CHECKS PASSED"
echo "==============================================="

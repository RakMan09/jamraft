#!/usr/bin/env bash
# Build the JamRaft WebAssembly demo into ./demo.
#
#   ./scripts/build-wasm.sh
#   # then serve ./demo over HTTP, e.g.:
#   (cd demo && python3 -m http.server 8000) && open http://localhost:8000
set -euo pipefail

cd "$(dirname "$0")/.."

OUT=demo
echo "Compiling Raft core to WebAssembly..."
GOOS=js GOARCH=wasm go build -trimpath -o "$OUT/jamraft.wasm" ./cmd/wasm

# Copy the Go WASM runtime shim that matches this toolchain.
GOROOT=$(go env GOROOT)
if [ -f "$GOROOT/lib/wasm/wasm_exec.js" ]; then
  cp "$GOROOT/lib/wasm/wasm_exec.js" "$OUT/wasm_exec.js"
elif [ -f "$GOROOT/misc/wasm/wasm_exec.js" ]; then
  cp "$GOROOT/misc/wasm/wasm_exec.js" "$OUT/wasm_exec.js"
else
  echo "error: wasm_exec.js not found in GOROOT ($GOROOT)" >&2
  exit 1
fi

SIZE=$(du -h "$OUT/jamraft.wasm" | cut -f1)
echo "Built $OUT/jamraft.wasm ($SIZE) and $OUT/wasm_exec.js"
echo "Serve it: (cd $OUT && python3 -m http.server 8000)  ->  http://localhost:8000"

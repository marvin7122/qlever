#!/bin/bash
# benchmark_noatime_sweep.sh — evaluates cold vocabulary NVMe sweep throughput with/without atime updates
set -euo pipefail

RUNS=5
VOCAB_FILE="${1:-/local/data-ssd/stoetzem/qlever-indices/wikidata/wikidata.index.pos}"
CLEAR_CACHE_BIN="/local/data-ssd/stoetzem/clear-caches"

echo "=== Optimization 5: NVMe Access Time & Storage Sweep ==="
echo "Target File: $VOCAB_FILE"
echo "Clearing OS Buffer Caches between iterations..."

for i in $(seq 1 $RUNS); do
  if [ -x "$CLEAR_CACHE_BIN" ]; then
    "$CLEAR_CACHE_BIN" >/dev/null 2>&1 || sudo sync
  fi
  echo "--- Run $i / $RUNS (Cold Cache) ---"
  time dd if="$VOCAB_FILE" of=/dev/null bs=4M count=500 iflag=direct status=progress
done

echo "=== Benchmark Complete ==="

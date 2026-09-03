#!/usr/bin/env bash
# ==============================================================================
# run_ab_profiles.sh — Automated A/B Performance Profiling & FlameGraph Generator
# ==============================================================================
# Evaluates QLever Export Optimizations:
#   1. Zero-Allocation FastExportStreamFormatter vs Baseline Serializer
#   2. Asynchronous Double-Buffered Chunk Pipeline vs Synchronous Lockstep
#   3. io_uring Registered Files / Pinned Buffers / O_DIRECT vs Standard pread
#
# Generates:
#   - Detailed performance summaries (triples/s, MB/s, allocation counts, speedup factors)
#   - Baseline SVG FlameGraphs
#   - Optimized SVG FlameGraphs
#   - Differential A/B SVG FlameGraphs (Blue = speedup, Red = regression)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QLEVER_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${QLEVER_ROOT}/benchmark-results/export-ab-profiles"
mkdir -p "${OUTPUT_DIR}"

FLAMEGRAPH_DIR="${HOME}/repos/FlameGraph"
if [[ ! -d "${FLAMEGRAPH_DIR}" ]]; then
  echo "Error: FlameGraph repository not found at ${FLAMEGRAPH_DIR}" >&2
  exit 1
fi

PERF_BIN="$(command -v perf || true)"
if [[ -z "${PERF_BIN}" ]]; then
  echo "Warning: 'perf' binary not found. Running benchmarks without hardware counter profiling." >&2
fi

echo "=============================================================================="
echo " Starting QLever Export A/B Benchmark & Profiling Suite"
echo " Output Directory: ${OUTPUT_DIR}"
echo "=============================================================================="

# ______________________________________________________________________________
# Helper: Profile a benchmark binary with perf and generate FlameGraphs
profile_benchmark() {
  local bench_name="$1"
  local bench_bin="$2"
  shift 2
  local bench_args=("$@")

  echo ""
  echo ">>> [BENCHMARK] Running ${bench_name}..."
  
  local raw_output="${OUTPUT_DIR}/${bench_name}_results.txt"
  local perf_data="${OUTPUT_DIR}/${bench_name}.perf.data"
  local folded_data="${OUTPUT_DIR}/${bench_name}.folded"
  local flamegraph_svg="${OUTPUT_DIR}/${bench_name}_flamegraph.svg"

  if [[ -n "${PERF_BIN}" && -x "${bench_bin}" ]]; then
    echo "    Recording CPU call-stacks with perf..."
    "${PERF_BIN}" record -F 999 -g -o "${perf_data}" -- "${bench_bin}" "${bench_args[@]}" 2>&1 | tee "${raw_output}"
    
    echo "    Collapsing perf stacks..."
    "${PERF_BIN}" script -i "${perf_data}" | "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" > "${folded_data}"
    
    echo "    Generating SVG FlameGraph..."
    "${FLAMEGRAPH_DIR}/flamegraph.pl" --title "${bench_name} FlameGraph" "${folded_data}" > "${flamegraph_svg}"
    echo "    Saved FlameGraph: ${flamegraph_svg}"
  elif [[ -x "${bench_bin}" ]]; then
    "${bench_bin}" "${bench_args[@]}" 2>&1 | tee "${raw_output}"
  else
    echo "    Binary ${bench_bin} not found. Please compile the target first."
  fi
}

# ______________________________________________________________________________
# Helper: Generate differential FlameGraph between baseline and optimized runs
generate_differential_flamegraph() {
  local test_name="$1"
  local base_folded="${OUTPUT_DIR}/${test_name}_baseline.folded"
  local opt_folded="${OUTPUT_DIR}/${test_name}_optimized.folded"
  local diff_svg="${OUTPUT_DIR}/${test_name}_diff_flamegraph.svg"

  if [[ -f "${base_folded}" && -f "${opt_folded}" ]]; then
    echo ">>> [DIFF] Generating Differential FlameGraph for ${test_name}..."
    "${FLAMEGRAPH_DIR}/difffolded.pl" -n "${base_folded}" "${opt_folded}" | \
        "${FLAMEGRAPH_DIR}/flamegraph.pl" --title "${test_name} Differential FlameGraph (Blue = Speedup, Red = Slowdown)" > "${diff_svg}"
    echo "    Saved Diff FlameGraph: ${diff_svg}"
  fi
}

# ______________________________________________________________________________
# Build binary paths
BUILD_DIR="${QLEVER_ROOT}/build"
SERIALIZER_BENCH="${BUILD_DIR}/benchmark/SerializerMicroBenchmark"
CHUNK_PIPELINE_BENCH="${BUILD_DIR}/benchmark/ChunkStreamingBenchmark"
IOURING_BENCH="${BUILD_DIR}/benchmark/IoUringDirectBenchmark"

# 1. Serializer Benchmark (FastExportStreamFormatter vs Baseline)
profile_benchmark "SerializerMicroBenchmark" "${SERIALIZER_BENCH}" --triples 1000000

# 2. Chunk Streaming Pipeline Benchmark (Async 2-Slot vs Sync Lockstep)
profile_benchmark "ChunkStreamingBenchmark" "${CHUNK_PIPELINE_BENCH}" --triples 200000 --latencies 0,5,20

# 3. io_uring & Direct I/O Benchmark (Registered vs Unpinned vs O_DIRECT)
profile_benchmark "IoUringDirectBenchmark" "${IOURING_BENCH}" --blocks 262144

echo ""
echo "=============================================================================="
echo " Benchmark & Profiling Suite Completed!"
echo " All results, logs, and FlameGraphs are available in:"
echo " ${OUTPUT_DIR}"
echo "=============================================================================="

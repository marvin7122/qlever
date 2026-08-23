# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Code review:** apply the standing review conventions in `/home/userNoPriv/.claude/qlever-code-review-guidelines.md` (imperative doc comments, assertion split, lifetime/owner-struct patterns, test-fixture conventions). Load it for any `/code-review` in this repo.

## What is QLever

QLever is a high-performance SPARQL 1.1 / RDF graph database developed at the University of Freiburg. It handles datasets with hundreds of billions of triples on a single machine. The two main binaries are `qlever-index` (offline index builder) and `qlever-server` (HTTP SPARQL endpoint).

## Build Commands

```bash
# Configure (RelWithDebInfo is standard for local dev)
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo

# Build everything
cmake --build build -j $(nproc)

# Build a single target
cmake --build build --target SomeTest -j $(nproc)
```

Key CMake options:
- `-DSINGLE_TEST_BINARY=ON` — links all tests into one `QLeverAllUnitTestsMain` binary (used in CI)
- `-DUSE_PRECOMPILED_HEADERS=ON` — significantly speeds up builds
- `-DENABLE_EXPENSIVE_CHECKS=ON` — enables extra runtime assertions
- `-DCMAKE_BUILD_TYPE=Asan` — AddressSanitizer build
- `-DADDITIONAL_COMPILER_FLAGS="-Wall -Wextra -Werror"` — used in CI

Active build directories in this repo: `build/`, `build-relwithdebinfo/`, `cmake-build-debug/`, `cmake-build-release/`, `cmake-build-relwithdebinfo/`.

## Running Tests

```bash
# Run all tests via ctest
ctest --test-dir build/test

# Run a single test binary
./build/test/SomeTest
./build/test/engine/SomeEngineTest --gtest_filter='*PatternToMatch*'

# Single binary mode (when SINGLE_TEST_BINARY=ON)
./build/test/QLeverAllUnitTestsMain --gtest_filter='*PatternToMatch*'
```

Test binaries mirror the `test/` directory structure: `build/test/`, `build/test/engine/`, `build/test/parser/`, `build/test/index/`, etc.

## Formatting

clang-format-18 is the enforced formatter (exact version matters).  The local
`clang-format` symlink MUST point to v18 — v16 produces different output and
CI will reject it.

```bash
# Verify version
clang-format --version  # must say 18.x

# Check (what CI runs)
./misc/format-check.sh

# Format a file in-place
clang-format-18 -i src/engine/MyFile.cpp
```

The `.clang-format` config is based on Google style with left pointer alignment.

## Before pushing — mandatory pre-flight

After any code change, run these locally before `git push`.  CI failures
that local CI should have caught are a bug — fix local CI, don't rely on
GitHub to catch them.

```bash
# 1. Format check (clang-format 18 — exact version)
./misc/format-check.sh

# 2. Build with strict warnings (gcc)
cmake -B build -DCMAKE_BUILD_TYPE=Release \
    -DADDITIONAL_COMPILER_FLAGS="-Wall -Wextra -Werror"
cmake --build build -j $(nproc)

# 3. If C++17 compat matters, also build the cpp17 variant
cmake -B build-cpp17 -DCMAKE_BUILD_TYPE=Release \
    -DUSE_CPP_17_BACKPORTS=ON \
    -DEXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17=ON
cmake --build build-cpp17 -j $(nproc)
```

If `localci` is configured (`.localci.toml` in the repo), `git push` runs
these automatically.  Do not bypass with `--no-verify` unless localci itself
is broken — fix localci instead.

## Architecture

### Data Model

All RDF terms are represented as 64-bit `ValueId` (`src/global/ValueId.h`). The upper bits encode a `Datatype` tag; remaining bits encode the value directly (for integers, doubles, dates, booleans, geo points) or store an index into a vocabulary (for IRIs and string literals). `VocabIndex` IDs require a potentially on-disk vocabulary lookup; all other datatypes are resolved from the ID bits or an in-memory `LocalVocab`.

### Index Layer (`src/index/`)

`IndexImpl` (pimpl-wrapped by `Index`) stores the six RDF permutations (SPO, SOP, PSO, POS, OSP, OPS) as compressed, block-structured files on disk via `CompressedRelation`. `Vocabulary` maps string RDF terms ↔ `VocabIndex`; multiple implementations exist (uncompressed in-memory, compressed, on-disk, geo). `DeltaTriples` handles SPARQL Update on top of the static index.

### Query Engine (`src/engine/`)

- `Operation` — abstract base for all physical operators. Holds `RuntimeInformation` and `CancellationHandle`.
- `QueryExecutionTree` — tree of `Operation` nodes; the query plan.
- `QueryPlanner` — cost-based optimizer that produces a `QueryExecutionTree` from a `ParsedQuery`.
- `QueryExecutionContext` — runtime singleton holding the `Index`, result cache, and `RuntimeParameters`.
- `IdTable` (in `engine/idTable/`) — the columnar result type: a table of `ValueId` rows passed between operators.
- `ExportQueryExecutionTrees` — converts query results to wire formats (JSON, TSV, CSV, Turtle, N-Triples). Its `idToStringAndType` / `idsToStringAndType` methods are the central `ValueId` → human-readable string conversion path.

Physical operators include: `IndexScan`, `Join`, `OptionalJoin`, `MultiColumnJoin`, `CartesianProductJoin`, `Filter`, `Sort`, `OrderBy`, `Distinct`, `GroupBy`, `Union`, `Minus`, `Values`, `Bind`, `Service`, `TransitivePath`, `PathSearch`, `SpatialJoin`, `TextIndexScanForWord/Entity`, and others.

CONSTRUCT query pipeline: `ConstructTemplatePreprocessor` → `ConstructBatchEvaluator` → `TableWithRangeEvaluator` → `ConstructTripleInstantiator` → `ConstructTripleGenerator`.

### Parser (`src/parser/`)

`SparqlParser` wraps an ANTLR4-generated parser. The output is a `ParsedQuery` IR that the `QueryPlanner` consumes. `RdfParser` handles Turtle/N-Triples input for indexing.

### SPARQL Expressions (`src/engine/sparqlExpressions/`)

Expression tree for FILTER, HAVING, BIND, SELECT: `SparqlExpression` hierarchy with specializations for aggregation, string operations, regex, geo, relational comparisons, etc.

### Utilities (`src/util/`)

Namespace `ad_utility`. Key utilities: `LRUCache`/`LRUCacheWithStatistics`, `HashMap`, `AllocatorWithLimit`, `CancellationHandle`, `stream_generator` (coroutine-based chunked output), `MmapVector`, `MemorySize` (with `_GB`/`_MB` literals), `Synchronized`, `TaskQueue`, and `ConfigManager` (structured runtime configuration). The `http/` subdirectory contains the Boost.Beast HTTP/WebSocket server.

### Backports (`src/backports/`)

C++20 features backported under `namespace ql` and `namespace ql::ranges` / `ql::views` for optional C++17 compatibility. Use `ql::span`, `ql::ranges::sort`, `ql::views::*` etc. rather than `std::` equivalents.

## Coding Conventions

**Header guards:** `#ifndef QLEVER_SRC_<PATH>_H` style (not `#pragma once`).

**Namespaces:** `ad_utility` for `src/util/`, `ql`/`ql::ranges`/`ql::views` for backports, `sparqlExpression` for expression code, `qlever::constructExport` for CONSTRUCT pipeline, global namespace for most engine classes.

**Assertions:**
- `AD_CONTRACT_CHECK(cond)` — precondition violations (always checked).
- `AD_CORRECTNESS_CHECK(cond)` — internal logic errors (always checked).

**Template constraints:** use range-v3 `CPP_template`/`CPP_fun` macros (declared as `AttributeMacros` in `.clang-format`) rather than raw `requires` when C++17 compat matters.

always use span instead of (const)vector&

**Tests:** named `<Subject>Test.cpp`, registered in `test/CMakeLists.txt` via `addLinkAndDiscoverTest(BaseName, [libs...])`. Tests needing a real index use `ad_utility::testing::getQec(...)` from `test/util/IndexTestHelpers.h`. White-box tests use `FRIEND_TEST`.

Use [[maybe_unused]] if the object returned by a function is always used / should always be used.

Use ranges or for-each loops instead of for-loops with explicit indices whenever possible.

## Benchmarking — QLever (always on Ural)

**All QLever benchmarks MUST run on Ural, never on the workstation.**

For ANY QLever benchmark, follow the `qlever-benchmarking` skill (it is the
authoritative procedure, including one-concern-per-run, interleaved A/B, and
the metric correction) and ALWAYS run the deterministic gate first and last:

```
# before running
scripts/qlever-benchmark-gate.sh preflight <binary> <commit> --require-iouring
# after running
scripts/qlever-benchmark-gate.sh postflight <run-dir> --expect-cold
```

**Artifact-linking rule (anti-hallucination):** a benchmark PR comment must
never assert bare numbers. Link the run directory (`experiments/runs/<id>/`
with `results.csv` + `conclusion.md`), a `build-env.txt` (compiler version,
CMAKE_BUILD_TYPE, USE_IO_URING, liburing version, per-binary commit + io_uring
symbol count), and the gate verdicts. Without these artifacts the numbers are
unverifiable — say so explicitly if any artifact is missing.

The gate is MANDATORY and checks EVERY concern that can silently fake a
result: stale binary (wrong commit after a failed build), io_uring silently
compiled out (liburing missing → SyncIoPolicy fallback), A/B arms that are
byte-identical, a stale server holding the port, CPU governor/load drift,
missing venv deps, a cold cache that wasn't actually cold, reps that failed
or served 0 bytes, and a run killed by an SSH drop.  Exit 0 = trustworthy.
Never trust numbers that did not pass the gate — a whole benchmark batch was
invalidated 2026-08-12 by skipping exactly these checks.

Existing infrastructure lives in `~/thesis/`:
- **Harness:** `~/thesis/scripts/benchmark_export.py` — protocol-aware benchmark
  harness that starts QLever server, controls cache (cold/warm), runs queries,
  collects timing and checksums.  Use `--qlever-server <binary>` to point at
  a specific build.
- **Cache eviction:** `~/thesis/scripts/evict_file_cache.py` —
  `posix_fadvise(DONTNEED)`, no root needed.
- **Experiment scaffolding:** `~/thesis/scripts/thesis-exp.py` — creates
  `experiments/runs/<id>/` with config + conclusion template.
- **Previous runs:** `~/thesis/experiments/runs/` — structured output
  (raw/, config.txt, conclusion.md).

**How to benchmark a branch:**
1. Build the branch on Ural: `ssh ural-ci` →
   `cd /local/data-ssd/stoetzem/qlever-src` → checkout → cmake → ninja.
2. Run the harness on Ural with the branch's binary against DBLP
   (`/local/data-ssd/stoetzem/dblp/`).
3. Control cache state: use `--cache-state cold` (evicts serving files
   before each rep) or `warm` (pre-loads).
4. Results land in `~/thesis/experiments/runs/<id>/`.  Write a
   `conclusion.md` with the comparison.

**Benchmark pitfalls (learned 2026-08-12):**
- The harness binds a FIXED port (default 7015).  A stale
  `qlever-server-*` process from a previous run holds the port, so every
  subsequent rep silently fails with `0 bytes` and status `failed`.
  Always `pkill -f 'qlever-server-'` on Ural before starting a run.
- After a run, confirm reps are `complete` AND `response_bytes > 0`.
  `failed`/0-byte rows mean the server never served the query.
- Run via `nohup` + a COMPLETE marker file so SSH drops don't kill the
  measurement mid-run.  Poll the marker, not the SSH session.

**MANDATORY artifact verification (learned 2026-08-12 — invalidated a whole
benchmark batch):**
- `USE_IO_URING=ON` does NOT guarantee io_uring is in the binary.  If
  `liburing` is missing, CMake silently falls back to `SyncIoPolicy`
  (`pread`), the build still "succeeds", and every benchmark silently
  measures the sync fallback.  liburing is NOT in apt on Ural — build it
  from source: `git clone https://github.com/axboe/liburing` →
  `./configure --prefix=$HOME/liburing-install && make -j4 && make install`.
- Building against a source-installed liburing needs THREE env vars (missing
  any one silently breaks the build or the runtime):
  - `PKG_CONFIG_PATH=.../lib/pkgconfig` (so `pkg_check_modules` finds it)
  - `LIBRARY_PATH=.../lib` (so the LINKER finds `-luring` — the trap: `ld:
    cannot find -luring` even though configure said "Found liburing")
  - `LD_LIBRARY_PATH=.../lib` (so the BINARY loads `liburing.so.2` at runtime)
- Before EVERY benchmark, run the deterministic gate (checks EVERY known
  concern — stale binary, io_uring compiled out, A/B arms byte-identical,
  stale server, CPU governor, load, cold cache):
  `scripts/qlever-benchmark-gate.sh preflight <binary> <commit> --require-iouring`
  then `scripts/qlever-benchmark-gate.sh postflight <run-dir> --expect-cold`.
  Exit 0 = trustworthy.  Never trust numbers that did not pass the gate —
  a whole benchmark batch was invalidated 2026-08-12 by skipping exactly
  these checks.
- Build with `set -e` and NEVER pipe `cmake --build` through `tail` —
  ninja failures were masked that way, and a stale binary got copied over
  the branch's binary.  Check the exit code, and verify `--version` shows
  the branch's commit before trusting any binary.

**Available indexes on Ural:**
- DBLP: `/local/data-ssd/stoetzem/dblp/` (primary benchmark index)
- Wikidata (if available): check `/local/data-ssd/stoetzem/`

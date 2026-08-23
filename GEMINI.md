# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

clang-format-16 is the enforced formatter (exact version matters).

```bash
# Check (what CI runs)
./misc/format-check.sh

# Format a file in-place
clang-format-16 -i src/engine/MyFile.cpp
```

The `.clang-format` config is based on Google style with left pointer alignment.

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

**Tests:** named `<Subject>Test.cpp`, registered in `test/CMakeLists.txt` via `addLinkAndDiscoverTest(BaseName, [libs...])`. Tests needing a real index use `ad_utility::testing::getQec(...)` from `test/util/IndexTestHelpers.h`. White-box tests use `FRIEND_TEST`.

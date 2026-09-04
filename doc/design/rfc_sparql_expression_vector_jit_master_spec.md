# RFC: High-Performance SPARQL Expression Engine — Vectorized Morsels and Cost-Gated Machine-Code JIT

**Status:** Proposed  
**Document Version:** 1.0.0  
**Authors:** Marvin Stoetzel (`stoetzem@email.uni-freiburg.de`)  
**Target Repository:** `marvin7122/qlever` (`feat/engine-v2-jit-expression-vm`)  
**Architecture Grounding:** `~/ARCHITECTURE.md` (7 Universal Laws) & Single-Core/Elastic Multi-Core Discipline  

---

## 1. Executive Summary & Problem Diagnosis

SPARQL query evaluation frequently features complex filter, bind, and order expressions:
```sparql
FILTER (?score * 1.5 + ?rank < 100 && (?year >= 2000 || !BOUND(?deleted)))
```

### The AST Interpretation Bottleneck (Legacy QLever)
In current relational and graph engines, including QLever's baseline, expressions are modeled as Abstract Syntax Trees (AST) derived from `sparqlExpression::SparqlExpression`. Evaluating an AST involves recursive virtual method calls (`evaluate(EvaluationContext*)`):
1. **Virtual Call Churn & Branch Mispredictions:** Every node in the AST incurs an indirect call via a vtable pointer.
2. **Intermediate Buffer Churn:** Sub-expressions materialize temporary `std::vector` or `ExpressionResult` variants on the heap.
3. **Loss of SIMD Vectorization:** Compilers cannot auto-vectorize loops across virtual function boundaries.

### The Microarchitectural Trap: Interpreted Bytecode Regressions (PR #111 Diagnosis)
PR #111 prototyped an 8-byte bytecode interpreter (`JitExpressionBytecodeVm`) operating over 64-row morsels. When benchmarked on Ural over 10,000,000 rows (`(col0 * 2) + col1 > 100`), the bytecode interpreter achieved **78.87 M rows/sec (126.8 ms)** versus the virtual AST baseline of **119.59 M rows/sec (83.62 ms)** — a **1.51x slowdown (0.66x throughput)**.

The root causes:
* **The Switch-Dispatch Tax:** Replacing virtual dispatch with a bytecode loop introduces an internal `switch(op)` dispatch loop, operand stack adjustments (`sp++`, `sp--`), and branch predictor stalls that cost more cycles than the compiler-inlined virtual tree.
* **Absence of Machine Instructions:** Interpreted bytecode still runs on an emulated software stack instead of native CPU registers.

### The Solution: Two-Tier Hybrid Architecture
This RFC specifies a **Two-Tier Expression Architecture** combining:
1. **Tier 1 (Default): Monomorphic Vectorized Chunk Kernels** (DuckDB / ClickHouse pattern): Zero compilation latency, cache-aligned chunks (1024 rows), branchless validity bitmasks, and compiler-unrolled SIMD auto-vectorization.
2. **Tier 2 (Cost-Gated): Native x86-64 Machine-Code JIT via `AsmJit`** (Umbra / PostgreSQL cost-gating pattern): For large table scans ($> 500{,}000$ rows), compile expressions directly into native x86-64 machine code in $< 50\,\mu\text{s}$, executing at $> 300\,\text{M rows/sec}$.

---

## 2. Frontier Systems Architecture & Prior Art

| Engine | Primary Paradigm | Compilation Latency | Execution Throughput | Fallback & Safety Strategy |
| :--- | :--- | :--- | :--- | :--- |
| **DuckDB** | Vectorized Chunk Kernels (`VectorOperations`) | $0\,\mu\text{s}$ (Instant) | $150\text{–}250\,\text{M rows/s}$ | Pure C++ templates; no runtime code generation. |
| **ClickHouse** | Vectorized Batch Functions (`IFunction`) | $0\,\mu\text{s}$ (Instant) | $150\text{–}300\,\text{M rows/s}$ | Primitive vector templates + optional LLVM JIT for fused operations. |
| **PostgreSQL** | Cost-Gated LLVM JIT | $5\text{–}50\,\text{ms}$ | $100\text{–}200\,\text{M rows/s}$ | `jit_above_cost = 100000`; short queries bypass JIT completely. |
| **Umbra** | Flying-Start One-Pass Machine Code JIT | $< 1\,\text{ms}$ | $300\text{–}500\,\text{M rows/s}$ | Emits raw x86-64 directly; switches to optimized LLVM only for long queries. |
| **Velox (Meta)** | SIMD Vector Functions + Expression Compiler | $0\,\mu\text{s}$ | $200\text{–}350\,\text{M rows/s}$ | Pre-compiled vector stencils over columnar arrow buffers. |

---

## 3. Two-Tier System Architecture for QLever

```
                            SPARQL Expression AST (`ParsedQuery`)
                                              │
                                              ▼
               ┌─────────────────────────────────────────────────────────────┐
               │           Cost & Capability Gate (`ExpressionRouter`)        │
               │  • Inspect expression types (integers, doubles, dates)      │
               │  • Estimate input cardinality (from QueryPlanner / CBO)     │
               └──────────────────────────────┬──────────────────────────────┘
                                              │
                    ┌─────────────────────────┴─────────────────────────┐
                    │                                                   │
                    ▼ (Cardinality < 500,000 or Unsupported Types)       ▼ (Cardinality >= 500,000 & Supported Ops)
     ┌──────────────────────────────────────────────┐    ┌──────────────────────────────────────────────┐
     │  Tier 1: Monomorphic Vector Operations       │    │  Tier 2: Native x86-64 JIT (AsmJit)          │
     ├──────────────────────────────────────────────┤    ├──────────────────────────────────────────────┤
     │ • 1024-row contiguous chunks                 │    │ • On-the-fly machine code generation         │
     │ • Zero compilation latency (0 µs)            │    │ • Compile latency: < 50 µs                   │
     │ • SIMD auto-vectorization (AVX2/AVX-512)     │    │ • Registers hold variables directly (RAX,RCX)│
     │ • 64-bit validity word masking               │    │ • Single unrolled loop with direct branch    │
     │ • Zero dynamic memory allocations            │    │ • Zero interpretation, zero stack frame      │
     └──────────────────────────────────────────────┘    └──────────────────────────────────────────────┘
                    │                                                   │
                    └─────────────────────────┬─────────────────────────┘
                                              │
                                              ▼
                             Bitmask-Filtered Result / Materialized IdTable
```

---

## 4. Software Architecture Standard Compliance (`~/ARCHITECTURE.md`)

1. **Deep Modules (Universal Law 1):**
   `ExpressionExecutor` encapsulates expression compilation, type analysis, cost gating, morsel batching, and native code caching behind a single clean method:
   ```cpp
   void evaluateFilter(const sparqlExpression::SparqlExpression& expr,
                       const IdTable& inputTable,
                       IdTable& resultTable,
                       const EvaluationContext& context);
   ```
2. **Information Hiding & Zero Accounting Leakage (Universal Law 2):**
   Callers (e.g. `Filter.cpp`, `GroupBy.cpp`, `OrderBy.cpp`) never deal with validity bitmasks, morsel offsets, JIT buffers, or AST compilation state.
3. **Defining Errors Out of Existence (Universal Law 4):**
   If an expression encounters undefined terms, type errors (e.g. dividing by zero or comparing IRI with integer), or unhandled AST nodes, the engine does not abort or crash. Unsupported subtrees fall back seamlessly to standard evaluation with zero client-visible error.
4. **Pulling Complexity Downward (Universal Law 3):**
   All memory alignment, AVX register spilling, and thread-local scratch arena lifecycles are handled internally.

---

## 5. Detailed Work Packages (WP1 – WP6)

### WP1: Tier 1 Monomorphic Vectorized Expression Kernels
* **Objective:** Implement DuckDB-style vectorized primitive operations over 1024-row chunk vectors (`ql::span<const Id>`).
* **Components:**
  * `src/engine/sparqlExpressions/VectorOperations.h`: Monomorphic binary kernels for arithmetic (`+`, `-`, `*`, `/`, `%`) and comparisons (`<`, `<=`, `>`, `>=`, `==`, `!=`).
  * `ValidityBitmask`: 64-bit word validity bitmasks handling `UNDEF` without branch prediction stalls.
* **Target:** $> 160\,\text{M rows/sec}$ throughput, $0\,\mu\text{s}$ compilation latency.
* **DoD:** Unit tests in `test/VectorOperationsTest.cpp` covering numeric boundary limits ($0$, INT64_MIN, NaN, Inf) and validity masking.

### WP2: Dual-Mode Expression Planner & Cost Gate
* **Objective:** Establish the automatic routing and feature-detection gate.
* **Components:**
  * `src/engine/sparqlExpressions/ExpressionCostGate.h`: Inspects `sparqlExpression::SparqlExpression` trees to classify operator eligibility and evaluate input cardinality.
  * Threshold: Inputs $< 500{,}000$ rows or containing non-primitive types route to Tier 1; large primitive filters route to Tier 2.
* **DoD:** 100% test coverage in `test/ExpressionCostGateTest.cpp` verifying route stability.

### WP3: Native x86-64 Machine-Code JIT Engine (`AsmJit`)
* **Objective:** Implement true Just-In-Time native code compilation using `AsmJit`.
* **Components:**
  * Add lightweight `asmjit` subproject to CMake.
  * `src/engine/sparqlExpressions/NativeExpressionJit.h`: AST-to-x86_64 compiler generating native assembly into an executable page (`mprotect` PROT_EXEC).
  * Direct register mapping: Assigns input column pointers to `RDI`, `RSI`, `RDX`, row counts to `RCX`, and accumulator masks to `RAX`.
* **Target:** $< 50\,\mu\text{s}$ compilation time; $> 300\,\text{M rows/sec}$ evaluation throughput.
* **DoD:** `NativeExpressionJitTest.cpp` validating code emission, execution correctness, and memory cleanup.

### WP4: Heterogeneous Type & Tri-State Logic Specialization
* **Objective:** Full SPARQL 1.1 tri-state logic (`true`, `false`, `error`) and numeric type coercion without branch degradation.
* **Components:**
  * Support inlined mixed `Int` and `Double` arithmetic via AVX2 conversion instructions (`_mm256_cvtepi64_pd`).
  * Date/time in-register evaluation integrating with `IntegerDateOperations`.
* **DoD:** Verification against W3C SPARQL 1.1 expression compliance test suite.

### WP5: Operator Integration (`Filter`, `GroupBy`, `OrderBy`)
* **Objective:** Wire the dual-mode expression engine into QLever's relational operations.
* **Components:**
  * `src/engine/Filter.cpp`: Direct morsel-vector filtering emitting matching rows without temporary table materializations.
  * Integration into `GroupBy` aggregation expressions and `OrderBy` sorting projections.
* **DoD:** All existing QLever filter and expression tests pass with zero regressions.

### WP6: Ural Cluster Benchmark & Performance Audit Suite
* **Objective:** Full end-to-end benchmark suite on the Ural CI server comparing Legacy AST, Tier 1 Vectorized Kernels, and Tier 2 Native JIT across 100M-row datasets.
* **Components:**
  * `benchmark/ExpressionEngineBenchmark.cpp`: Parametric benchmark testing expression depth, cardinality ($1\text{K} \rightarrow 100\text{M}$ rows), selectivity ($0.1\% \rightarrow 99\%$), and memory allocations.
  * FlameGraph profiling and hardware perf counter tracking (IPC, L1D misses, branch mispredictions).
* **DoD:** Complete empirical audit verifying $> 2.5\times$ speedup over baseline AST on large queries and zero regression on interactive queries.

---

## 6. Implementation & Verification Timeline

```
Milestone     Deliverables                                  Target Latency / IPC
────────────────────────────────────────────────────────────────────────────────
WP1 (T1 Ops)  VectorOperations.h (1024-row morsels)         > 160 M rows/s (0 µs compile)
WP2 (Gate)    ExpressionCostGate.h & router                 < 100 ns decision
WP3 (T2 JIT)  NativeExpressionJit.h (AsmJit backend)        > 300 M rows/s (< 50 µs compile)
WP4 (Logic)   Heterogeneous Float/Int + Date arithmetic     Zero branch misprediction
WP5 (Wiring)  Filter.cpp & Operator integration             100% CTest pass rate
WP6 (Audit)   Ural Cluster E2E Benchmark & Thesis audit     Bit-identical verification
```

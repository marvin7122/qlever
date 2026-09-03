# QLever Fast-Path Streaming Export Engine (V2): Master Architectural Specification & Work Packages

**Target Branch:** `feat/export-engine-v2-streaming`  
**Repository:** `marvin7122/qlever`  
**Design Standard:** `~/ARCHITECTURE.md` (7 Universal Laws) & Single-Core/Elastic Multi-Core Discipline  

---

## 1. Executive Summary & New Architectural Dimensions

This master specification deepens the design of the **Fast-Path Streaming Export Engine (V2)** across three major innovations:
1. **Hybrid Columnar vs. Row-Major Dynamic Layout Selection** (Optimal memory layouts chosen at plan time).
2. **Elastic Traffic-Aware Concurrency Controller** (Single-core default preventing head-of-line blocking, with dynamic work-stealing scale-out when the server is idle).
3. **State-of-the-Art Techniques from Modern Data Engines** (DuckDB, ClickHouse, Umbra, Arrow, ScyllaDB).
4. **8 Atomic Work Packages (WPs)** structured for autonomous implementation and rigorous DoD verification.

```
                         Complete Engine V2 System Architecture
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ 1. HTTP Ingress & Adaptive Route Gate (`src/engine/ExportPipelineRouter.h`)                 │
│    • Query-time parameters (?fast-export=1), headers, plan capability check                 │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 2. Traffic-Aware Elastic Scheduler (`src/engine/export_v2/ElasticConcurrencyManager.h`)    │
│    • Strict single-core execution when concurrent queries exist (zero head-of-line blocking)│
│    • Dynamic work-stealing thread leasing when server is idle (instant yield on new queries)│
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 3. Push-Based Vector Stream Source (`src/engine/export_v2/VectorStreamSource.h`)           │
│    • 64KB morsel streaming directly from NVMe index blocks (zero intermediate IdTables)     │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 4. Hybrid Layout Selector (`src/engine/export_v2/HybridLayoutManager.h`)                    │
│    • Columnar Layout (Arrow/Parquet/Analytics) vs Row-Major Layout (Turtle/TSV/CSV)         │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 5. Monomorphic Schema Specialization (`src/engine/export_v2/MonomorphicSerializers.h`)     │
│    • Compile-time unrolled serialization + Lemire SIMD Radix + AVX2 Literal Escaping        │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 6. Zero-Copy Arena Scatter-Gather (`src/engine/export_v2/ScatterGatherArenaStreamer.h`)    │
│    • Direct iovec / SEND_ZC pointer arrays referencing page-pinned decompression arenas     │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 7. Asynchronous Double-Buffered Ring (`src/engine/export_v2/AsyncChunkPipeline.h`)         │
│    • 2-slot 4MB ring interleaving CPU formatting with network transmission                  │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 8. Server Integration & Differential Verifier (`benchmark/EndToEndExportBenchmark.cpp`)    │
│    • Bit-for-bit output validation against Legacy V1, latency (TTFB), and FlameGraph suites │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Hybrid Layout Architecture: Column-Major vs. Row-Major

```
                      Hybrid Layout Decision & Memory Flow
                                ┌───────────────────────────┐
                                │ Query Schema & Format Plan│
                                └─────────────┬─────────────┘
                                              │
                         ┌────────────────────┴────────────────────┐
                         │ Format: Turtle, TSV, CSV?               │ Format: Arrow, Parquet,
                         │ Or: Full Multi-Column Triple Export     │ or Sparse Column Scan?
                         ▼                                         ▼
          ┌──────────────────────────────┐          ┌──────────────────────────────┐
          │ Row-Major Streaming Layout   │          │ Column-Major Vector Layout   │
          ├──────────────────────────────┤          ├──────────────────────────────┤
          │ • Contiguous row formatting  │          │ • Column-wise vector streams │
          │ • Monomorphic tuple unroll   │          │ • SIMD bitpacking & RLE runs │
          │ • Punctuation interleaving   │          │ • Direct RecordBatch output  │
          │ • Direct socket byte stream  │          │ • Fast selective projections │
          └──────────────┬───────────────┘          └──────────────┬───────────────┘
                         │                                         │
                         └────────────────────┬────────────────────┘
                                              ▼
                                   Client Socket Output
```

### Inspiration & Prior Art
* **DuckDB Vector Columns vs. Serialized Batches:** DuckDB executes all operations in columnar `Vector` chunks (2,048 elements). When formatting to CSV, it passes vectors to a vectorized row assembler; for Arrow/Parquet, it transmits the raw columnar memory buffers directly with zero transformation.
* **ClickHouse Block Architecture:** ClickHouse maintains memory as a collection of `IColumn` vectors, allowing column-wise compression (FSST, Gorilla, DoubleDelta) to execute with SIMD vector instructions.

### Mechanics in QLever V2
* **Row-Major Path:** Best for text-based graph serializations (Turtle, N-Triples, TSV, CSV). The engine pulls columnar vectors from index blocks and immediately serializes them into formatted rows using monomorphic SIMD kernels.
* **Column-Major Path:** Best for binary analytical export formats (Apache Arrow IPC, Parquet, Feather) or queries with high column selectivity (e.g. projecting 2 columns out of a 10-column join). Keeps columns completely separate in memory, avoiding row interleaving overhead.

---

## 3. Traffic-Aware Elastic Concurrency (Single-Core Default + Elastic Scale-Out)

```
                    Elastic Traffic-Aware Concurrency Controller
              ┌───────────────────────────────────────────────────────┐
              │ Incoming Export Request                               │
              └───────────────────────────┬───────────────────────────┘
                                          │
                            Is Server Under Load?
                           (Queue > 0 or CPU > 50%)
                                          │
                         ┌────────────────┴────────────────┐
                         │ YES                             │ NO (Server Idle)
                         ▼                                 ▼
          ┌──────────────────────────────┐  ┌──────────────────────────────┐
          │ Strict Single-Core Execution │  │ Elastic Work-Stealing Lease  │
          ├──────────────────────────────┤  ├──────────────────────────────┤
          │ • 1 Thread for compute & I/O │  │ • Leases N worker threads    │
          │ • Zero starvation of queries │  │ • Parallel morsel evaluation │
          │ • Supervisor rule satisfied  │  │ • Instant yield on new query │
          └──────────────────────────────┘  └──────────────────────────────┘
```

### Addressing the Supervisor Constraint
The supervisor's constraint is critical: **a heavy multi-threaded query must never monopolize the server and cause head-of-line blocking for interactive queries.**

### How the Elastic Controller Solves This:
1. **Single-Core by Default:** The export pipeline is fundamentally designed to achieve **8M–15M triples/sec on a SINGLE CPU core**.
2. **Morsel-Driven Task Granularity (200–500 μs Atomic Units):**
   - Work is partitioned into discrete, self-contained units called **Morsels** (e.g. a 64 KB vector chunk or 4,096 rows).
   - A single CPU core processes one morsel in approximately **200 to 500 microseconds**.
3. **Atomic Token Lease Protocol (`TaskScheduler`):**
   - While the server request queue is empty, the export coordinator leases helper tokens from the global server `TaskScheduler` (e.g. up to $N-1$ helper threads on an $N$-core system).
4. **Preemptive Core-Yielding Mechanism on New Query Arrival:**
   - **Step 1 (Ingress Notification):** The instant a new HTTP request hits the server’s socket accept queue, `Server::handleRequest` increments the priority request counter (`priorityQueueDepth.fetch_add(1)`).
   - **Step 2 (Cooperative Boundary Check):** At the completion of each atomic morsel, helper threads execute a single branchless atomic check:
     ```cpp
     if (scheduler.priorityQueueDepth() > 0 || !leaseActive_) {
       // Instantly surrender core back to global server pool
       return;
     }
     ```
   - **Step 3 (Immediate Thread Surrender in <1 ms):** Because morsels are strictly bounded to 200–500 μs, all helper threads cleanly exit and return to the global pool in **less than 1 millisecond**.
   - **Step 4 (Zero Query Disruption):** The primary export coordinator continues running uninterrupted on its dedicated single core. The newly arrived interactive query immediately receives the full set of CPU cores without waiting.
   - **Step 5 (Dynamic Scale-Out Recovery):** Once the interactive query completes and the queue returns to 0, helper threads can once again assist with morsel formatting.

### Amendment (2026-09-03): Isolation, Safety, and Verification Requirements

Review of this section surfaced three unresolved concerns before any implementation is accepted. This amendment resolves scope/blast-radius (1) with hard requirements, and converts (2) and (3) from unverified assumptions into explicit engineering tasks with concrete Definitions of Done.

**1. Isolation from V1 and non-V2 queries (resolved — hard requirement, not optional)**

- `TaskScheduler` and the `priorityQueueDepth`/`leaseActive_` machinery exist **only** on the V2 code path. V1 queries never touch it, never lease, never get preempted by it.
- The `priorityQueueDepth.fetch_add(1)` in `Server::handleRequest` (Step 1 above) **must itself be gated** by the same flag that controls V2 routing — not merely "harmless if nothing reads it." If V2 is disabled at runtime, `handleRequest` must execute exactly the code it executes today, with zero added instructions, zero added atomics, on the shared request path.
- **Compile-time exclusion**: provide a build option (e.g. `QLEVER_ENABLE_EXPORT_V2=OFF`) that removes `TaskScheduler`, `ElasticConcurrencyManager`, and all V2 export code from the binary entirely. Operators who never want this code in their process shouldn't have to trust a runtime flag — they should be able to not compile it in.
- **Runtime kill-switch**: independent of the compile option, a startup flag (e.g. `--disable-export-v2`) must fully disable V2 routing in binaries built with it compiled in, with the guarantee in the previous bullet.
- DoD: a test that runs the full V1 query suite with V2 compiled in but runtime-disabled, and asserts zero calls into any V2/`TaskScheduler` symbol (e.g. via a call-count instrumentation build) and no measurable latency delta on `Server::handleRequest` vs. a V2-not-compiled build.

**2. Lease/revocation safety (open — must be solved by whoever implements this WP)**

Thread lease lifecycle across concurrent, independently-lifetimed queries is a correctness-critical concurrency primitive, not a performance detail. Requirements:

- **Cooperative-only revocation.** A helper thread's lease is never externally cancelled — it only stops at the morsel-boundary check already specified in Step 2. No other thread ever forces a helper thread to stop mid-morsel.
- **Keep-alive ownership, not raw references.** Whatever export-coordinator state a helper thread touches during a morsel must be held via a refcounted/keep-alive handle acquired before the morsel starts. If the primary query's request finishes and its own state would otherwise be torn down while a helper is still mid-morsel, the keep-alive must delay that teardown until the helper returns. Raw pointers/references into query-owned state are not acceptable here.
- **Opaque lease tokens, not thread handles.** `TaskScheduler` hands out a lease token (e.g. generation counter + thread id), so a stale or already-expired lease is detectable rather than silently reused against the wrong query's state.
- DoD: a ThreadSanitizer (TSAN) stress test exercising concurrent query start/cancel/finish while helper threads are mid-lease, under high churn (rapid-fire query arrival/cancellation), with zero TSAN reports. This must be a required, checked-in test target for WP7 — not left implicit.

**3. Preemption latency bound (open — currently asserted, not measured)**

The "<1 ms thread surrender" and "200-500 μs morsel" figures in Steps 2-3 are architectural targets, not measurements — nothing in this spec currently verifies them, and the mechanism can fail silently under realistic conditions (a morsel that includes a blocking syscall — e.g. an `io_uring` completion wait per WP6, or a cold page fault on the vocabulary arena per WP5 — can block past its budget with no fallback).

- Instrument every morsel type (SIMD scan, scatter-gather write, io_uring send) to record real p50/p99/p999 duration under load, not just CPU-bound best case.
- Explicitly test the adversarial case: inject a blocking syscall stall (throttled `io_uring`, delayed accept) *during* an in-progress morsel, and measure actual time-to-yield.
- If any morsel type can exceed budget under this test, the fix is either finer-grained sub-morsels with more frequent yield checks, or moving the blocking operation off the leased worker thread entirely (async completion reaped by a non-leased thread) — not a larger timeout.
- DoD: a dedicated `ChaosPreemptionBenchmark` that runs a V2 export under synthetic adversarial load (throttled I/O, concurrent query storm) and asserts a measured p99.9 time-to-first-byte bound for a newly arriving interactive query. This benchmark, and its passing result, replaces the current unverified "<1 ms" claim as the WP7 Definition of Done.

**Structural recommendation**: implement WP7 as two separately reviewable pieces — (a) the lease/scheduler core in isolation, fully TSAN-tested against concurrent V2-only workloads, and (b) its integration into `Server::handleRequest` behind the compile/runtime kill-switch from point 1. (b) should be small and mechanical if (a) is solid; do not land both in one PR.

**Scheduler backend design decision**: two real precedents exist for the underlying task queue and neither is a default-obvious choice — (1) a per-thread Chase-Lev work-stealing deque (Cilk/TBB/Rayon/Go-runtime lineage: each worker owns a deque, pushes/pops locally LIFO for cache locality, idle threads steal from the far end FIFO), or (2) a single shared lock-free MPMC queue per task-type pool (DuckDB's actual `TaskScheduler`, built on `moodycamel::ConcurrentQueue`, with `ProducerToken` for fast-path locality — verified against DuckDB's real source, not assumed). Chase-Lev gives better cache locality and is the classic choice for compute-bound generic task-parallel runtimes; DuckDB's shared-queue design is simpler, avoids per-thread deque lock-free correctness risk, and is proven in a query engine (a closer analogue to QLever than Cilk/TBB's generic-workload target).

**Starting choice for WP7's first implementation: the DuckDB-style shared MPMC queue.** Reasoning: less lock-free code to get right (leans on an existing well-tested queue implementation rather than a hand-rolled deque), and DuckDB is the closer domain precedent. This is not a final architectural commitment — once a working implementation exists, benchmark both approaches under QLever's actual morsel size/rate before locking in the production design. Track this as an open follow-up, not a settled decision.

---

## 4. State-of-the-Art Engine Innovations Adapted for QLever V2

| Innovation | Origin Engine | Application in QLever V2 Export Engine |
| :--- | :--- | :--- |
| **Push-Based Morsel Execution** | Hyper / Umbra | 64KB vector chunks pushed from leaf scans directly to socket buffer; 0 intermediate `IdTable`s. |
| **SIMD Radix Radical Conversion** | DuckDB / fast_float | Division-free branchless integer-to-ASCII conversion (>180M nums/sec). |
| **Vectorized Character Masking** | simdjson / simdutf | 32-byte AVX2 escape scanning testing quotes/newlines in 1 instruction (13.94 GB/s). |
| **Non-Temporal Streaming Stores** | ClickHouse / HPC | `_mm_stream_si128` streaming directly to DRAM, bypassing CPU cache hierarchy (14.71 GB/s). |
| **SWAR Punctuation Packing** | High-Frequency Trading | Delimiter sequences (`"\t"`, `"< >"`, `"\r\n"`) packed into 64-bit unsigned integers. |
| **Run-Length Prefix Folding** | DuckDB / ClickHouse | Formats repeated IRI subjects/predicates once and copies 128-bit words across sorted runs. |
| **Scatter-Gather Zero-Copy** | Apache Arrow / Seastar | Assembles chunks via `struct iovec` arrays pointing to decompression arena pages (0 copies). |
| **Adaptive Initial Chunking** | DuckDB | Starts with 64KB buffer for <0.1ms TTFB, then exponentially ramps to 4MB for max throughput. |

---

## 5. Master Work Package (WP) Breakdown for Autonomous Agents

To enable independent implementation and clean reviewability, the V2 engine is decomposed into **8 discrete Work Packages**:

```
                                  Work Package Progression Map
 ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
 │     WP 1     │ ──> │     WP 2     │ ──> │     WP 3     │ ──> │     WP 4     │
 │ Ingress Gate │     │ VectorStream │     │ Monomorphic  │     │ SIMD Escapes │
 └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
        │                    │                    │                    │
        ▼                    ▼                    ▼                    ▼
 ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
 │     WP 5     │ ──> │     WP 6     │ ──> │     WP 7     │ ──> │     WP 8     │
 │ ScatterGath. │     │ Async Ring   │     │ Elastic Pool │     │ E2E Verifier │
 └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

---

### Work Package 1: Ingress Routing & Safe Fallback Gate
* **Artifact Target:** `src/engine/ExportPipelineRouter.h` & `test/ExportPipelineRouterTest.cpp`
* **Task Description:**
  - Implement request parameter parsing (`?fast-export=1`, `?export-engine=v2`, `X-QLever-Export-Engine: v2`).
  - Implement server configuration default (`--export-engine-default=legacy|v2`).
  - Implement plan capability analyzer: inspects `ParsedQuery` to verify fast-path eligibility.
  - Implement automatic transparent fallback to Legacy V1 on unsupported shapes.
* **Definition of Done:** 100% unit test coverage in `ExportPipelineRouterTest.cpp` verifying mode selection and error-free fallback.

---

### Work Package 2: Push-Based Vector Stream Source
* **Artifact Target:** `src/engine/export_v2/VectorStreamSource.h` & `test/VectorStreamSourceTest.cpp`
* **Task Description:**
  - Create `VectorStreamSource` consuming index scan iterators in 64KB vector chunks (4,096–8,192 rows).
  - Implement in-place SIMD filtering using AVX2 equality bitmasks (`_mm256_cmpeq_epi64`).
  - Zero heap allocations during chunk streaming.
* **Definition of Done:** `VectorScanBenchmark` achieves >120M rows/sec on a single CPU core with 0 intermediate `IdTable` materialization.

---

### Work Package 3: Monomorphic Template Schema Specialization
* **Artifact Target:** `src/engine/export_v2/MonomorphicSerializers.h` & `test/MonomorphicSerializersTest.cpp`
* **Task Description:**
  - Create compile-time unrolled serializers for fixed tuple schemas (`MonomorphicRowSerializer<ColumnTypes...>`).
  - Integrate `FastIntToString.h` (Lemire branchless integer formatting).
  - Integrate `SwarDelimiterPacker.h` for single-store punctuation writes.
* **Definition of Done:** `MonomorphicSerializerBenchmark` confirms >2.75 IPC and <0.25% branch misprediction rate.

---

### Work Package 4: SIMD Character Classification & Literal Escaping
* **Artifact Target:** `src/engine/export_v2/SimdEscapeClassifier.h` & `test/SimdEscapeClassifierTest.cpp`
* **Task Description:**
  - Implement AVX2 32-byte vector scanner for TSV (`\t`, `\n`, `\r`, `\\`), CSV (`"`), and Turtle (`"`, `\n`, `\r`, `\\`).
  - Fast-path raw byte copying when 32-byte mask is 0.
* **Definition of Done:** `SimdEscapeBenchmark` validates scanning throughput >13.5 GB/s on realistic literal datasets.

---

### Work Package 5: Zero-Copy Arena Scatter-Gather Streaming
* **Artifact Target:** `src/engine/export_v2/ScatterGatherArenaStreamer.h` & `test/ScatterGatherArenaStreamerTest.cpp`
* **Task Description:**
  - Assemble output chunks as arrays of `struct iovec` referencing decompressed vocabulary arena spans.
  - Integrate `InPlaceHttpChunkFraming.h` for in-place HTTP 1.1 chunk size header writing.
* **Definition of Done:** `ScatterGatherBenchmark` passes on Ural with zero intermediate `std::string` allocations.

---

### Work Package 6: Asynchronous Double-Buffered Backpressure Ring
* **Artifact Target:** `src/engine/export_v2/AsyncChunkPipeline.h` & `test/AsyncChunkPipelineTest.cpp`
* **Task Description:**
  - Implement 2-slot 4MB buffer ring using non-blocking socket I/O / Boost.Asio coroutines.
  - Interleave single-core CPU formatting with network NIC DMA transmission.
  - Implement backpressure suspension for slow client connections.
* **Definition of Done:** `ChunkStreamingBenchmark` demonstrates 0 CPU idle stalls under 5ms simulated network latency.

---

### Work Package 7: Elastic Traffic-Aware Concurrency Controller
* **Artifact Target:** `src/engine/export_v2/TaskScheduler.h`, `src/engine/export_v2/ElasticConcurrencyManager.h` & `test/ElasticConcurrencyTest.cpp` (the `TaskScheduler` referenced in Section 3 does not exist anywhere in QLever today — building it is in scope for this WP, not a pre-existing dependency).
* **Task Description:**
  - Build `TaskScheduler` (lease tokens, not raw thread handles) and wire its gated hook into `Server::handleRequest` per the Amendment above (compile-time + runtime kill-switch, zero footprint when disabled).
  - Enforce single-core execution when concurrent queries exist.
  - Implement dynamic work-stealing morsel leasing when server is completely idle, using cooperative-only revocation and refcounted keep-alive state per the Amendment's lease-safety requirements.
  - Implement immediate preemption and worker thread surrender on new query arrival.
* **Definition of Done:** See "Amendment (2026-09-03)" above — isolation test, TSAN stress test with zero reports, and a passing `ChaosPreemptionBenchmark` with a measured (not assumed) p99.9 time-to-first-byte bound. The original "<500μs latency" and "zero head-of-line blocking" claims are targets for that benchmark to hit, not standalone acceptance criteria.

---

### Work Package 8: End-to-End Server Integration & Differential Verifier
* **Artifact Target:** `src/engine/export_v2/ExportExecutionEngineV2.h` & `benchmark/EndToEndExportBenchmark.cpp`
* **Task Description:**
  - Wire `ExportExecutionEngineV2` into `Server::sendResultInChunks` and `ConstructBatchEvaluator`.
  - Create differential automated test suite comparing Legacy V1 vs Fast-Path V2 output byte-for-byte across 5,000,000 triples.
  - Generate automated differential SVG FlameGraphs and hardware performance counter tables.
* **Definition of Done:** 100% bit-equivalence (`diff == 0`), >200x lower TTFB latency, and 4x–8x sustained throughput improvement.

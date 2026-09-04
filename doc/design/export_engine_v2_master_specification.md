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
│ 2. Traffic-Aware Elastic Scheduler (`src/engine/export_v2/ElasticExportScheduler.h`)       │
│    • Strict single-core execution when concurrent queries exist                            │
│    • Bounded helper leasing when the V2 export is the only registered query                │
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
                         Is Another Query Active?
                      (`QueryRegistry` active count > 1)
                                          │
                         ┌────────────────┴────────────────┐
                         │ YES                             │ NO (Server Idle)
                         ▼                                 ▼
          ┌──────────────────────────────┐  ┌──────────────────────────────┐
          │ Strict Single-Core Execution │  │ Elastic Work-Stealing Lease  │
          ├──────────────────────────────┤  ├──────────────────────────────┤
          │ • 1 Thread for compute & I/O │  │ • Leases N worker threads    │
          │ • No helper work admitted    │  │ • Parallel CPU morsels       │
          │ • Primary path continues     │  │ • Cooperative revocation     │
          └──────────────────────────────┘  └──────────────────────────────┘
```

### Addressing the Supervisor Constraint
The supervisor's constraint is critical: **a heavy multi-threaded query must never monopolize the server and cause head-of-line blocking for interactive queries.**

### How the Elastic Controller Solves This

1. **Single-core baseline:** The primary export coordinator can complete every export without helpers.
2. **Measured morsels:** CPU work is partitioned into owned morsels. Their size and duration remain experimental parameters until measured.
3. **Isolated helper pool:** V2 helpers use `ElasticExportScheduler`. They never enqueue work on `Server::queryThreadPool_`.
4. **Foreground signal:** Runtime-enabled `QueryRegistry` callbacks update the active-query count and demand epoch.
5. **Cooperative revocation:** A new registered query invalidates helper leases. Running helpers finish one CPU morsel before yielding.
6. **Recovery:** Helper admission resumes when the V2 export is again the only registered query.

The design makes no fixed preemption claim. WP7 instrumentation must establish p50, p99, and p99.9 latency under the specified adversarial workloads.

### Amendment (2026-09-03): Isolation, Safety, and Verification Requirements

Review of this section surfaced three unresolved concerns. This amendment converts them into implementation requirements and verification tasks.

The detailed prerequisite design is in `export_engine_v2_wp7_prerequisites.md`. It grounds WP7 in QLever's current `QueryRegistry`, `queryThreadPool_`, and HTTP coroutine architecture.

**1. Isolation from V1 and non-V2 queries**

- `ElasticExportScheduler` exists only on the V2 code path. V1 queries never submit work to it or consume its helper pool.
- Runtime-enabled V2 subscribes to `QueryRegistry` query-registration and query-deregistration callbacks. The scheduler counts the current V2 export request and admits helpers only while that active-query count is exactly one. Runtime-disabled V2 registers no callbacks and constructs no scheduler.
- `QLEVER_ENABLE_EXPORT_V2=OFF` excludes the scheduler and every V2 production source.
- A startup flag disables V2 routing in binaries that include it.
- The isolation test compares output, request latency, and retired instructions across excluded and runtime-disabled builds.

**2. Lease and revocation safety**

The scheduler owns every lease identity, epoch, task state, and result slot.

- Revocation occurs only between morsels. No thread cancels another thread during a morsel.
- Every morsel owns its job state through a keep-alive handle.
- Private job and epoch identifiers reject stale work.
- TSAN stress tests cover concurrent admission, revocation, cancellation, completion, and destruction.

**3. Preemption latency verification**

The specification defines no preemption bound before measurement.

- Instrument every morsel kind and report p50, p99, and p99.9 wall time.
- Keep socket operations and completion waits outside leased helper threads.
- Inject CPU pressure, page faults, delayed output, and concurrent query arrivals.
- Publish measurements before selecting the acceptance threshold.

Implement the scheduler core and server integration as separate changes. The first change uses a fake demand source and contains no `Server` modification.

The first backend is a bounded mutex queue. Compare MPMC and Chase-Lev alternatives only after the correctness baseline passes.

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
* **Design prerequisite:** `doc/design/export_engine_v2_wp7_prerequisites.md`
* **Artifact Target:** `src/engine/export_v2/ElasticExportScheduler.h`, its implementation file, and focused scheduler tests.
* **Task Description:**
  - Build an isolated V2 helper pool. Do not enqueue V2 helper work on `Server::queryThreadPool_`.
  - Observe foreground demand through runtime-enabled `QueryRegistry` callbacks. QLever has no `Server::handleRequest` function.
  - Enforce single-core execution when concurrent queries exist.
  - Implement dynamic work-stealing morsel leasing when server is completely idle, using cooperative-only revocation and refcounted keep-alive state per the Amendment's lease-safety requirements.
  - Revoke outstanding helper leases immediately when a new query arrives; workers currently executing a morsel finish that morsel and then surrender without starting another one.
* **Definition of Done:** See the amendment and prerequisite design. Required evidence includes isolation tests, TSAN results, and measured p99.9 preemption and first-byte latency.

---

### Work Package 8: End-to-End Server Integration & Differential Verifier
* **Artifact Target:** `src/engine/export_v2/ExportExecutionEngineV2.h` & `benchmark/EndToEndExportBenchmark.cpp`
* **Task Description:**
  - Wire `ExportExecutionEngineV2` into `Server::sendResultInChunks` and `ConstructBatchEvaluator`.
  - Create differential automated test suite comparing Legacy V1 vs Fast-Path V2 output byte-for-byte across 5,000,000 triples.
  - Generate automated differential SVG FlameGraphs and hardware performance counter tables.
* **Definition of Done:** 100% bit-equivalence (`diff == 0`), >200x lower TTFB latency, and 4x–8x sustained throughput improvement.

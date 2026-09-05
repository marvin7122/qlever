# Design: Export V2 Ring Integration (AsyncChunkPipeline on the Live Path)

**Author:** Muse Code (integration planner)
**Status:** Proposed
**Created:** 2026-09-05
**Branch:** `feat/export-v2-ring-integration` (from `feat/export-v2-unified-pipeline-wired` tip `3eb1388e4`)
**Target Repository:** `marvin7122/qlever`

---

## 1. Goal

Put `AsyncChunkPipeline` on the `ExportEngineV2` live HTTP path so that morsel
serialization overlaps socket send through the bounded ring instead of through
the generic `runStreamAsync` prefetch thread, without changing a single
response byte and without adding threads.

The coroutine-frame problem this resolves: `ExportEngineV2::computeResultChunks`
is a coroutine, so GCC rewrites its body into a coroutine frame and rejects
`std::thread` plus `AsyncChunkPipeline` locals inside it (commit `91a9a7845`).
Overlap therefore currently comes from *outside* the coroutine: the iovec
branch wraps the chunk generator in `runStreamAsync` with depth 2
(`src/engine/Server.cpp:1050-1056`), and the default string branch wraps the
string generator in `runStreamAsync` with depth 100 inside `setBody`
(`src/util/http/HttpUtils.h:138`). The ring replaces the outer prefetch with a
purpose-built handoff whose ownership lives outside any coroutine frame.

## 2. Success Criteria

1. **Overlap is measurable.** On a multi-morsel export (at least 3 morsels, so
   the pipeline can fill), `AsyncChunkPipelineStats` reports both
   `producerWaits_ > 0` and `consumerWaits_ > 0` across repeated runs, which
   proves the producer serializes while the socket drains and vice versa.
   Total wall time for the H-size export is less than or equal to the
   `runStreamAsync` baseline measured on the same host and index.
2. **Latency does not regress.** Time to first byte on the iovec path is less
   than or equal to the baseline. The ring adds no extra hop: the producer
   pushes morsels directly, whereas today each chunk passes through an extra
   `ThreadSafeQueue` plus a re-yield coroutine (`HttpUtils.h:139-143`).
3. **Byte identity.** The full response bytes of the ring path are identical to
   the `runStreamAsync` path for the same query, index, and send mode, for
   both bounded (`LIMIT`/`OFFSET`) and unbounded SELECT CSV/TSV queries.
4. **No regressions.** All of `AsyncChunkPipelineTest`,
   `AsyncChunkPipelineDisabledTest`, `StreamableBodyTest`,
   `ScatterGatherHttpBodyTest`, `ExportEngineV2Test`,
   `ElasticExportSchedulerTest`, and `ExportMorselPlannerTest` pass.
5. **No new threads.** Exactly one producer thread exists per in-flight export
   on the migrated path, the same count as today (the `runStreamAsync`
   thread is removed when the ring takes over).

## 3. Context And Current Facts

All citations refer to the `feat/export-v2-ring-integration` worktree, which
is identical to `feat/export-v2-unified-pipeline-wired` tip `3eb1388e4` plus
this document.

### 3.1 The coroutine and its comment

* `ExportEngineV2::computeResultChunks`
  (`src/engine/export_v2/ExportEngineV2.cpp:495-514`) is a
  `cppcoro::generator<ScatterGatherChunk>` coroutine. Its comment
  (`ExportEngineV2.cpp:501-504`) states that GCC rewrites the function as a
  coroutine frame and rejected `std::thread` plus `AsyncChunkPipeline` locals
  (commit `91a9a7845`, verified to exist with that exact message). The body
  therefore serializes on the caller thread and yields each finalized chunk.
* The sibling `ExportEngineV2::computeResult`
  (`ExportEngineV2.cpp:465-492`) yields one `std::string` per morsel and
  additionally falls back to Legacy V1 for unsupported operation trees
  (`ExportEngineV2.cpp:472-485`); `computeResultChunks` instead requires
  `canHandle` (`ExportEngineV2.cpp:500`) and never falls back.

### 3.2 The live send path (`Server::sendStreamableResponse`)

* `Server::sendStreamableResponse` (`src/engine/Server.cpp:992-1145`) routes
  every export. Engine selection defaults to `LegacyV1`
  (`Server.cpp:1014-1015`); send-mode selection defaults to
  `ConcatenatedString` (`Server.cpp:1016-1017`).
* The iovec branch (`Server.cpp:1029-1072`) builds a
  `scatter_gather_body` response whose generator is
  `asScatterGatherBody(runStreamAsync(computeResultChunks(...), 2))`
  (`Server.cpp:1050-1056`). The comment at `Server.cpp:1048-1049` repeats the
  coroutine-frame rationale, and the adapter `asScatterGatherBody`
  (`Server.cpp:980-987`) exists only to own the prefetched range in a
  coroutine parameter instead of a `[&]` capture. Serializer exceptions
  already map to `EPIPE` in the writer
  (`src/engine/export_v2/ScatterGatherHttpBody.h:66-71`), and the send site
  swallows `EPIPE` (`Server.cpp:1061-1064`).
* The default string branch (`Server.cpp:1080-1095`) builds a
  `generator<std::string>` and hands it to `createOkResponse`, which reaches
  `setBody` (`HttpUtils.h:127-153`). `setBody` wraps the generator in
  `runStreamAsync(std::move(generator), 100)` (`HttpUtils.h:138`) and then
  re-yields the prefetched range through a second coroutine
  (`HttpUtils.h:139-143`), with optional compression on top.

### 3.3 What the generic prefetch provides today

* `runStreamAsync` (`src/util/AsyncStream.h:87-93`) spawns one `JThread`
  producer (`AsyncStream.h:38-49`) that pushes range elements into a
  `ThreadSafeQueue` of `bufferLimit`, forwards exceptions via `pushException`,
  and finishes the queue at the end. The consumer side pops through `get()`
  (`AsyncStream.h:55-68`).
* `ThreadSafeQueue` (`src/util/ThreadSafeQueue.h:29-140`) blocks the producer
  while full (`ThreadSafeQueue.h:56-69`), rethrows a pushed exception on every
  `pop` (`ThreadSafeQueue.h:73-86,123-139`), and drains already-queued
  elements before reporting `nullopt` after `finish()`
  (`ThreadSafeQueue.h:96-139`). Switching to the ring therefore changes three
  things: the queue type (purpose-built ring with statistics instead of the
  generic queue), the thread owner (a driver object outside the coroutine
  instead of a nested `AsyncStreamGenerator`), and the removal of the
  re-yield coroutine hop on the iovec path (the `asScatterGatherBody`
  adapter becomes unnecessary once the writer pulls from the ring directly).

### 3.4 The ring contract (`AsyncChunkPipeline`)

* `AsyncChunkPipeline` (`src/engine/export_v2/AsyncChunkPipeline.h:54-204`)
  is a bounded handoff queue that retains the PR #82 backpressure and
  exception propagation but creates no worker threads
  (`AsyncChunkPipeline.h:50-53`). Its docstring names the intended
  integration: the future HTTP integration drives the ring from the query
  executor and the socket completion handlers without violating the
  single-core scheduling contract.
* `push` blocks while the ring is full and reports `Closed` once the pipeline
  leaves `Running` (`AsyncChunkPipeline.h:108-129`). `pop` drains already
  queued chunks before rethrowing a producer failure
  (`AsyncChunkPipeline.h:134-155`), which matches the `ThreadSafeQueue`
  drain-then-throw semantics the send path relies on. `finish`, `fail`, and
  `cancel` (`AsyncChunkPipeline.h:157-198`) wake both sides; `cancel` drops
  queued chunks so a disconnected consumer releases owned buffers
  immediately. `stats()` (`AsyncChunkPipeline.h:200-203`) exposes produced,
  consumed, discarded, byte, and wait counters.
* Two kill switches gate the ring: the compile-time
  `kExportV2CompiledIn` flag (`AsyncChunkPipeline.h:27-31`) and the runtime
  `runtimeEnabled_` flag (`AsyncChunkPipeline.h:33-36,78-85`). Both default
  to off, so a default build behaves as a closed pipeline.
* `ChunkType` only needs `size()` for byte accounting
  (`AsyncChunkPipeline.h:69-75`, defaulting to 0 otherwise) and move
  construction (`AsyncChunkPipeline.h:124-126`). `ScatterGatherChunk`
  satisfies both: it exposes `size()` in bytes
  (`src/engine/export_v2/ScatterGatherArenaStreamer.h:196`), is default and
  move constructible (`ScatterGatherArenaStreamer.h:180`), and its segments
  are reference-counted (`shared_ptr` owners), so moving a chunk through the
  ring never copies payload bytes.

### 3.5 The consumer side

* The `scatter_gather_body::writer` (`ScatterGatherHttpBody.h:33-101`) is
  pull-based: Beast calls `get()`, which advances the generator, keeps the
  current chunk alive as a member, exposes its segments as `const_buffer`
  sequences, skips empty chunks (`ScatterGatherHttpBody.h:52-64`), and
  batches at most `UIO_MAXIOV` buffers per call
  (`ScatterGatherHttpBody.h:93-99`). The chunk-lifetime discipline the writer
  already implements (member storage, views valid while the chunk is alive,
  `ScatterGatherArenaStreamer.h:209-211`) is exactly what a ring-fed writer
  needs: `pop()` replaces generator advancement, nothing else changes.
* The `streamable_body::writer` (`src/util/http/streamable_body.h:45-120`)
  follows the same pull pattern with one `std::string` of member storage.
* Morsel production lives in `buildSerializedMorsels`
  (`ExportEngineV2.cpp:199-271`): one header builder, then one builder per
  8192-row morsel. With a scheduler, morsels run as session tasks and the
  coordinator consumes them (`ExportEngineV2.cpp:241-270`); without a
  scheduler, the loop serializes inline (`ExportEngineV2.cpp:217-236`).
  Bounded queries (`LIMIT`, `OFFSET`, text limit, export limit) force slot
  order; unbounded queries emit in completion order
  (`ExportEngineV2.cpp:242-247`).
* The session consume path (`ElasticExportScheduler.h:492-584`) implements
  both orders: slot-ordered consume for bounded queries
  (`ElasticExportScheduler.h:497-500`) and completion-order consume otherwise
  (`ElasticExportScheduler.h:501-534`), with a single-core inline fallback
  that executes a pending morsel on the coordinator thread
  (`ElasticExportScheduler.h:548-574`). Admission control is already
  backpressure-aware: `appendAndEnqueue` only offers morsels to helpers while
  `HelpersEligible` (`ElasticExportScheduler.h:634-660`), and live V2 posts
  helper work onto `Server::queryThreadPool_` instead of a second pool
  (`ElasticExportScheduler.h:180-185`, `Server.cpp:78-93`).

### 3.6 Prior art: WP6 branch

* Branch `feat/export-v2-wp6-async-ring` (tip `6aa3f7811`) contains the ring
  class itself (`37333be9e`: bounded pipeline without thread-spawning
  adapters, plus both kill switches and tests; `22f39f83f`: cancellation
  drops queued chunks) and CI fixes only. It contains no HTTP driver code,
  so there is no prior-art driver to reuse; the integration shape it intends
  is the one stated in the class docstring (query executor plus socket
  completion handlers, no worker threads) and in master-spec WP6
  (`doc/design/export_engine_v2_master_specification.md:244-247`: 2-slot
  ring over non-blocking socket I/O / Asio coroutines). The class is
  unchanged between that tip and this worktree except CI-motivated test
  formatting, so this design reuses it as specified.
* Master-spec WP7 prerequisites
  (`doc/design/export_engine_v2_wp7_prerequisites.md`) already cover the
  scheduler side; this design covers only the ring handoff between morsel
  production and the Beast writer.

### 3.7 Already pinned by tests (must keep passing)

* `test/AsyncChunkPipelineTest.cpp` (9 tests): zero capacity is rejected;
  the runtime kill switch leaves the pipeline closed; an empty finished
  pipeline yields no chunk; chunks move without copying their buffer;
  completion drains queued chunks in order; cancellation unblocks a waiting
  producer and counts discarded chunks; cancellation releases queued buffers;
  failure propagates after queued chunks drain; a freed slot unblocks the
  producer (backpressure reuse).
* `test/AsyncChunkPipelineDisabledTest.cpp`: the compile-time switch
  overrides runtime opt-in.
* `test/StreamableBodyTest.cpp`: writer init, generator exception to error
  code, empty generator, buffered multi-chunk results.
* `test/ScatterGatherHttpBodyTest.cpp`: writer init and the `EPIPE` mapping
  for serializer failures.

## 4. Constraints And Non-goals

* **Legacy default stays.** `selectEngine` keeps `LegacyV1` as the server
  default (`ExportPipelineRouter.h:81-125`, `Server.cpp:1014-1015`); the ring
  activates only inside the existing V2 branches. No routing change.
* **EPIPE failure contract stays** (decided 2026-09-04). Serializer-side
  failures map to `EPIPE` (`ScatterGatherHttpBody.h:66-71`) and both send
  sites swallow `EPIPE` while counting other errors
  (`Server.cpp:1061-1069,1117-1144`). The ring driver must translate
  `pipeline.pop()` rethrows into the same `EPIPE` at the writer boundary,
  never into a new error kind.
* **Ordered-prefix semantics stay.** Bounded queries consume morsels in slot
  order (`ElasticExportScheduler.h:468-473,497-500`,
  `ExportEngineV2.cpp:242-247`). The ring preserves whatever order the
  session hands out; it never reorders.
* **Non-goal: socket completion-handler drive.** The class docstring names
  completion handlers as the eventual consumer driver, and master-spec WP6
  names non-blocking socket I/O. This design deliberately stops one step
  short: the Beast writer stays pull-based (`get()` blocks in `pop()`), and
  completion-handler or io_uring send remains follow-up work. Rationale:
  Beast owns the serializer loop, so push-into-Beast would require a new
  body concept; the blocking-`pop()` step captures the overlap win with a
  minimal diff.
* **Non-goal: compression and middleware on the iovec path.** The existing
  fallback to concatenated strings when compression or response middleware
  is present (`Server.cpp:1030-1040`) is unchanged.
* **Non-goal: CONSTRUCT and non-CSV/TSV media types.** `canHandle` already
  excludes them (`ExportEngineV2.h:42-51`); the ring changes nothing about
  eligibility.

## 5. Key Decisions

### D1. Pipeline and thread ownership lives in a driver object in the send-function scope (recommended)

**Recommended:** introduce a non-coroutine RAII driver, `ExportRingDriver`,
owned as a local in `Server::sendStreamableResponse` (iovec branch first).
The driver owns one `AsyncChunkPipeline<ScatterGatherChunk>` and one
producer thread. The producer thread runs the existing morsel-consume loop
(`session.consumeNextResult` followed by `push`, `fail` on exception,
`finish` at the end); the send coroutine only holds the driver and pulls
from it. Because neither the pipeline nor the thread is a coroutine local,
the GCC frame problem (commit `91a9a7845`) cannot recur, and the existing
`asScatterGatherBody` adapter goes away with the `runStreamAsync` wrapper.

**Rejected alternatives.**

* *Hoist the pipeline and thread onto `Server` or into the export session.*
  This spreads one request's lifetime across shared server state and needs
  explicit per-request bookkeeping for cancellation and teardown. The
  function-scope driver ties lifetime to the response send, which is the
  actual consumption window, and `cancel()` in the driver destructor already
  unblocks a parked producer.
* *Split driver plus source with the session pushing directly into the ring
  from helper threads.* This would bypass the coordinator consume order and
  break slot-ordered emission for bounded queries. The producer thread must
  sit *behind* `consumeNextResult`, not beside it, so ordering and the
  single-core inline fallback keep working unchanged.
* *Keep `runStreamAsync` and place the ring behind it.* That stacks two
  queues and two wakeups per chunk with no benefit; the ring subsumes the
  prefetch queue (same drain-then-throw failure semantics, plus byte and
  wait statistics the generic queue lacks).

### D2. The Beast writer pulls from the ring with blocking `pop()`

**Recommended:** change the iovec writer path so `get()` calls
`pipeline.pop()` instead of advancing a generator iterator. The writer
already keeps the current chunk alive as a member and already skips empty
chunks; both behaviors are preserved. A rethrown producer exception at the
writer boundary maps to `EPIPE`, identical to today
(`ScatterGatherHttpBody.h:66-71`). The default string path keeps its
generator writer until the ring is proven on the iovec path (see D4).

**Rejected alternatives.**

* *Push from socket completion handlers into Beast now.* Beast's serializer
  is pull-based; pushing requires a new body concept plus flow control
  duplicated with the ring. Deferred to follow-up work; the docstring's
  long-term direction is explicitly *not* abandoned, only sequenced.
* *Non-blocking `pop()` with `boost::none` meaning "try later".* Beast
  treats `boost::none` as end-of-stream, so a spurious empty read would
  truncate the response. Blocking `pop()` is the correct primitive; the
  writer thread is already dedicated to this send.

### D3. Backpressure propagates through the existing scheduler chain

**Recommended:** no new signalling. A full ring parks the producer thread
inside `push()`; the parked producer stops calling `consumeNextResult`;
unconsumed session slots accumulate; the scheduler helper queue
(capacity 1024, `ElasticExportScheduler.h:280-283`) fills; helpers stop
being offered work via the existing `HelpersEligible` gate
(`ElasticExportScheduler.h:650-654`). `producerWaits_` becomes the
observable backpressure signal and should be logged per export at INFO
alongside the existing V2 log lines. Cancellation flows the other way:
client disconnect destroys the driver, `cancel()` drops queued chunks and
unblocks the producer, and the session is closed so helpers abandon
remaining morsels through the existing revocation checkpoints
(`ExportEngineV2.cpp:117-122,163-180`).

**Rejected alternatives.**

* *Unbounded ring or drop-on-full.* Unbounded breaks the memory bound that
  justifies the ring (chunks own their strings); dropping breaks byte
  identity.
* *A new explicit demand channel from writer to scheduler.* The blocking
  `push()` already is the demand channel; a second one would duplicate the
  PR #82 semantics the ring was adapted to preserve.

### D4. `runStreamAsync` leaves the iovec path, stays on the string path

**Recommended:** remove `runStreamAsync` (and with it the
`asScatterGatherBody` adapter) from the iovec branch only. The default
string branch keeps `runStreamAsync(..., 100)` via `setBody` until the ring
has demonstrated byte identity and overlap on the iovec path; migrating the
string path is a separate, smaller change (a `AsyncChunkPipeline<std::string>`
driver feeding the existing `streamable_body` writer) with its own
byte-identity gate.

**Rejected alternatives.**

* *Flag-day removal on both paths.* Doubles the blast radius; the string
  path additionally interacts with compression (`HttpUtils.h:145-150`),
  which the ring path does not touch.
* *Keep both prefetch and ring stacked.* Section D1 already rejects this:
  two queues, two threads' worth of wakeups, zero benefit.

### D5. One producer thread, depth 2

**Recommended:** exactly one producer thread per in-flight export on the
migrated path (net thread count unchanged: it replaces the `runStreamAsync`
thread), and ring capacity 2, keeping `kIovecPrefetchDepth = 2`
(`Server.cpp:1050`) and the `AsyncChunkPipelineConfig` default
(`AsyncChunkPipeline.h:33-36`) as the single depth constant. This matches
master-spec WP6 (2-slot ring) and bounds ring memory to two morsels
(2 x 8192 rows plus header).

**Rejected alternatives.**

* *Deeper ring (4-8).* More overlap headroom in theory, but each slot pins
  a full morsel of owned strings, and scheduler-side parallelism already
  covers producer-side latency. Revisit only with measurements showing the
  consumer starved while helpers were idle.
* *Zero extra threads (serialize inline on the writer thread).* That is
  today's `computeResultChunks`-without-prefetch behavior: no overlap at
  all. The one producer thread is what buys serialization-send overlap.

### D6. WP6 prior art: reuse the class, reject the adapters

**Recommended:** reuse `AsyncChunkPipeline` unchanged, including both kill
switches. Explicitly reject reintroducing anything resembling the PR #82
thread-spawning adapters the class was adapted to exclude
(`AsyncChunkPipeline.h:50-53`): the only thread involved is the single
driver-owned producer, posted nowhere near `queryThreadPool_` (morsel CPU
work already runs there; the producer thread only shuttles results).

## 6. Recommended Approach

The work proceeds in three slices, each independently committable and
revertible:

1. **Driver plus iovec wiring.** Add `ExportRingDriver` (header-only,
   next to `AsyncChunkPipeline.h`): constructor takes the morsel source
   (the `computeResultChunks` argument bundle: parsed query, execution
   tree, media type, cancellation handle, scheduler pointer) and starts
   the producer thread; `pop()` delegates to the ring; destructor cancels
   and joins. Rewrite the `Server.cpp:1029-1072` branch to construct the
   driver, hand its `pop()` interface to the writer path, and delete
   `asScatterGatherBody` plus the `runStreamAsync` wrapper. Depth constant
   stays 2.
2. **Observability.** Log `stats()` (produced, consumed, waits, bytes) at
   INFO when the iovec send completes, next to the existing
   "Using ExportEngineV2 scatter-gather HTTP body" line
   (`Server.cpp:1041-1043`). No new metrics labels; the existing
   `sendStreamableResponse` label (`src/util/metrics/Metrics.h:101`)
   already covers the path.
3. **String-path migration (separate change).** Repeat slice 1 for
   `std::string` chunks behind the same driver template, gated by its own
   byte-identity run. Only then consider removing the `HttpUtils.h:138`
   `runStreamAsync` call.

## 7. Work Plan

Ordered units with code surfaces:

1. `ExportRingDriver` header in `src/engine/export_v2/` (new file):
   owns `AsyncChunkPipeline<ScatterGatherChunk>`, one `JThread`, and the
   source-argument bundle. Producer loop: `consumeNextResult` equivalent
   over `ExportEngineV2::computeResultChunks` output, `push` per chunk,
   `fail(std::current_exception())` on error, `finish()` at end. Surfaces:
   `AsyncChunkPipeline.h:108-182`, `ExportEngineV2.h:104-108`.
2. Unit tests for the driver (new `test/ExportRingDriverTest.cpp`,
   registered in `test/CMakeLists.txt` next to `AsyncChunkPipelineTest`
   at line 630): driver yields every chunk in order; producer failure
   surfaces after drain; destruction unblocks the producer; stats show
   both waits on a 3-morsel stream with capacity 1.
3. Iovec branch rewrite in `Server::sendStreamableResponse`
   (`src/engine/Server.cpp:976-1072`): construct driver, feed writer via
   `pop()`, remove `asScatterGatherBody` and the depth-2 `runStreamAsync`
   wrapper, keep the compression/middleware fallback and both `EPIPE`
   handlers untouched.
4. Writer adaptation (`ScatterGatherHttpBody.h:44-101`): `loadNextChunk`
   pulls from the ring. Keep empty-chunk skipping, `UIO_MAXIOV` batching,
   and the `EPIPE` mapping byte-for-byte.
5. Stats logging at the iovec send site (`Server.cpp:1041-1043`,
   `AsyncChunkPipeline.h:200-203`).
6. String-path migration as a follow-up change (surfaces:
   `HttpUtils.h:127-153`, `streamable_body` writer), only after slice 1-5
   meet the success criteria.

## 8. Validation Plan

No build or cluster commands were run while writing this design (per task
constraints); the following is the exact gate sequence for implementation:

1. Unit tests on the cluster (from a clean checkout of the implementation
   branch):
   `cluster-wq build feat/export-v2-ring-integration AsyncChunkPipelineTest AsyncChunkPipelineDisabledTest StreamableBodyTest ScatterGatherHttpBodyTest ExportEngineV2Test ElasticExportSchedulerTest ExportMorselPlannerTest ScatterGatherArenaStreamerTest`
   All suites must pass; the two `AsyncChunkPipeline` suites pin the ring
   contract, the body suites pin the writer contract.
2. Byte-identity smoke: serve the same index twice (ring branch vs
   `feat/export-v2-unified-pipeline-wired` tip `3eb1388e4`), request the
   same SELECT CSV/TSV export once with `export-send=iovec` and once by
   default over both bounded (`LIMIT`/`OFFSET`) and unbounded queries, and
   `diff` the full response bodies. Any difference fails the gate.
3. H-size A/B re-run rule: run the H-size export (the 1M-row case noted at
   `ExportEngineV2.cpp:420-424`) at least twice per branch on the same
   host, alternating branches, and compare wall time plus time to first
   byte. The ring branch must be no slower on both measures; on a
   multi-morsel export its logged stats must show `producerWaits_ > 0`
   and `consumerWaits_ > 0`.
4. Cancellation probe: start a large export, abort the client mid-stream,
   and confirm the server logs no hang (driver destructor path) and the
   session closes.

## 9. Risks / Rollback

* **Risk: blocking `pop()` inside Beast stalls connection handling.**
  Mitigation: this is behavior-preserving; today the writer thread already
  blocks inside `runStreamAsync`'s `pop()` at the same call site, one queue
  further down. Rollback: revert the `Server.cpp` branch to the
  `runStreamAsync` wrapper (a ~15-line revert); the driver header is
  additive and needs no revert.
* **Risk: depth 2 starves either side on unusual morsel sizes.**
  Mitigation: `stats()` logging makes starvation visible immediately
  (one-sided waits). Rollback: bump the single depth constant; no design
  change.
* **Risk: exception translation gap at the writer boundary.**
  Mitigation: map every `pop()` rethrow to the existing `EPIPE` path and
  cover it with a writer unit test that fails the producer mid-stream
  (mirrors `ScatterGatherHttpBodyTest` failure cases). The `EPIPE`
  swallowing at `Server.cpp:1061-1064` is untouched.
* **Risk: ordered-prefix violation for bounded queries.**
  Mitigation: the producer sits behind `consumeNextResult`, so slot order
  is inherited, not reimplemented; the byte-identity gate includes bounded
  queries explicitly.

## 10. Open Questions

None. Every claim above was verified against the worktree code; the two
candidate ambiguities (prefetch depths, prior-art contents) were resolved
by reading the code (see Corrections below) and require no further input.

## Appendix A. Corrections to the Brief

The brief was accurate except for two points the code refines:

1. **Prefetch depth is not uniformly 2.** The iovec branch uses depth 2
   (`Server.cpp:1050`: `kIovecPrefetchDepth`), but the default string path
   uses depth 100 (`HttpUtils.h:138` inside `setBody`). The brief's
   "(generic thread + ThreadSafeQueue, depth 2)" conflates the two sites.
   Both sites funnel through the same `runStreamAsync` primitive
   (`AsyncStream.h:87-93`), which is what the ring replaces on the iovec
   path first.
2. **`Server::sendStreamableResponse` exists and is the right anchor.**
   The brief's phrasing is confirmed, not corrected: the function is
   defined at `Server.cpp:992` (an earlier file search missed it only
   because the server sources live in `src/engine/`, not `src/server/`).
   Likewise confirmed: the `computeResultChunks` comment
   (`ExportEngineV2.cpp:501-504`), the ring docstring's integration
   direction (`AsyncChunkPipeline.h:50-53`), the pull-based writer plus
   slot/completion-order consume split, and the listed test files (plus
   the unlisted but relevant `test/ScatterGatherHttpBodyTest.cpp`).

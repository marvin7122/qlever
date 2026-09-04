# Export Engine V2 WP7 Prerequisite Design

**Status:** Design prerequisite. The initial production scheduler implementation is included behind QLEVER_ENABLE_EXPORT_V2 and remains subject to the prerequisites described below.

**Source baseline:** `marvin7122/qlever` `origin/master` at `379568d8d`, inspected on 2026-09-03.

## 1. Purpose

WP7 may use idle CPU capacity for V2 export formatting. It must surrender that capacity when another SPARQL operation starts.

This document defines the ownership, admission, revocation, isolation, and measurement contracts required before implementation.

The first implementation must optimize for correctness and isolation. Queue backend optimization follows measured contention data.

## 2. Current QLever constraints

The current server has no global `TaskScheduler`. Query planning runs on `Server::queryThreadPool_` through `computeInNewThread`.

The HTTP entry point is `Server::handleHttpRequest`, not `Server::handleRequest`. Streamed result generation begins in `Server::sendStreamableResponse`.

`QueryRegistry` already exposes start and end callbacks. Registration occurs before query planning enters `queryThreadPool_`.

QLever does not currently contain `moodycamel::ConcurrentQueue` or another shared lock-free MPMC queue dependency.

These facts invalidate a direct implementation of the original `TaskScheduler` sketch. They also provide a smaller integration seam.

## 3. Non-negotiable invariants

1. V2 helper work never enters `queryThreadPool_`.
2. The primary export coordinator always makes progress without helper threads.
3. Revocation is cooperative and occurs only between morsels.
4. Query cancellation and helper revocation remain separate state transitions.
5. Every helper task owns the state it accesses until that task returns.
6. No helper task stores raw references into request-owned or coroutine-owned state.
7. A stale lease cannot execute work for a later export job.
8. Socket writes, completion waits, and other blocking operations never run on leased helper threads.
9. Queued and completed morsels cannot be emitted twice.
10. Compile-time exclusion removes every WP7 production symbol and source file.

The implementation must encode these invariants inside one scheduler module. Callers must not manage epochs, counters, tokens, or result-slot bookkeeping.

## 4. Design alternatives

### 4.1 Replace or prioritize `queryThreadPool_`

This design would share existing query workers with V2 exports. It would require priority scheduling across planning, updates, rebuilds, and export work.

This option has the largest blast radius. A queued export morsel could delay interactive planning before cooperative revocation runs.

WP7 rejects this option for its first implementation.

### 4.2 Create a process-wide V2 helper pool

This design creates a bounded pool used only by V2 export morsels. Workers sleep when no eligible export job exists.

New foreground demand invalidates active helper leases. The primary export coordinator continues on its existing execution path.

This option isolates queueing from `queryThreadPool_`. WP7 selects it for the first implementation.

### 4.3 Create threads per export request

This design creates and joins helper threads for each export. It avoids a process-wide scheduler but adds request-time thread lifecycle costs.

Concurrent exports would also create competing thread groups. WP7 rejects this option.

## 5. Selected module boundary

The production module is named `ElasticExportScheduler`. The name limits its authority to export helper work.

The scheduler owns all helper threads, the bounded work queue, admission state, lease epochs, and shutdown coordination.

Its public interface exposes export work sessions and owned morsels. It does not expose worker handles, queue objects, or lease tokens.

An `ExportWorkSession` is move-only and owns one export job's scheduler relationship. Destruction closes admission without blocking the request coroutine.

An `OwnedMorsel` pairs executable work with a `shared_ptr` to its `ExportJobState`. Construction must reject missing ownership.

The scheduler assigns each session a monotonically increasing job identifier and lease epoch. Both values remain private implementation details.

The coordinator submits CPU-only morsels through its session. Rejected or revoked work remains available to the coordinator's single-core path.

This fallback is internal to `ExportWorkSession`. The caller never requeues a morsel or reconciles helper results.

## 6. Foreground demand without a global scheduler

When runtime V2 support is enabled, `Server` registers callbacks through the existing `QueryRegistry` API.

The start callback increments an atomic foreground count and demand epoch. It then wakes sleeping V2 helper workers.

The end callback decrements the foreground count. It may make helper capacity available again.

The running V2 export contributes one registered query. Helper admission therefore requires exactly one active query and an eligible V2 session.

A second query or update raises the count above one. This invalidates all current helper leases before its planning work enters `queryThreadPool_`.

V1 execution never submits work to `ElasticExportScheduler`. Its existing query registration event only supplies foreground demand when runtime V2 is enabled.

When runtime V2 is disabled, `Server` creates no scheduler and registers no V2 callbacks.

## 7. Lease and revocation state machine

Each export session has four states: `PrimaryOnly`, `HelpersEligible`, `Revoking`, and `Closed`.

| Current state | Event | Next state | Required action |
|---|---|---|---|
| `PrimaryOnly` | Foreground count becomes one | `HelpersEligible` | Admit bounded helper work. |
| `HelpersEligible` | Foreground count exceeds one | `Revoking` | Increment the demand epoch and reject new helper claims. |
| `Revoking` | Last active morsel returns | `PrimaryOnly` | Make remaining morsels available to the coordinator. |
| `PrimaryOnly` | Foreground count returns to one | `HelpersEligible` | Permit new helper claims under a new epoch. |
| Any open state | Query cancellation | `Closed` | Stop admission and complete normal cancellation cleanup. |
| Any open state | Export completion | `Closed` | Stop admission, drain active tasks, and join session state. |

A worker snapshots the current epoch when it claims a morsel. It validates that epoch immediately before beginning CPU work.

Once a morsel begins, another thread never stops it. The worker observes revocation after the morsel completes and then returns its lease.

Pending work from an invalid epoch cannot start. The session transfers that work to the coordinator without duplicating completed output.

## 8. Ownership and result ordering

`ExportJobState` owns cancellation state, immutable export configuration, morsel status, and ordered result slots.

Every helper closure captures `shared_ptr<ExportJobState>` by value. No closure captures a request object, coroutine frame, or local reference.

Morsel status uses an explicit enum such as `Pending`, `Running`, and `Completed`. Empty output is valid and never acts as a sentinel.

Only the session changes morsel status. Result publication performs one checked transition from `Running` to `Completed`.

The output coordinator consumes completed slots in sequence order. Helpers never write to the socket or mutate the HTTP response.

Normal completion uses one consuming finalization operation that awaits active helpers and drains ordered results.

Cancellation closes publication without waiting. Active helper tasks keep job state alive until their current CPU morsels return.

## 9. Queue backend decision

The first implementation uses a bounded mutex and condition-variable queue. QLever already supports the required C++17 primitives.

This choice minimizes concurrent algorithm risk. The queue remains private, so later backend replacement does not affect callers.

The earlier DuckDB-style MPMC recommendation is deferred. QLever does not currently vendor DuckDB's queue dependency.

After correctness verification, a benchmark must compare the initial queue against an MPMC queue and a Chase-Lev design.

The comparison must report submission rate, contention time, CPU consumption, and p99.9 revocation latency.

## 10. Compile-time and runtime isolation

`QLEVER_ENABLE_EXPORT_V2=OFF` excludes the WP7 sources, tests, and scheduler member from their build targets.

Runtime disablement performs one startup policy decision. It constructs no scheduler and registers no `QueryRegistry` callbacks.

Portable C++ cannot guarantee that runtime disablement adds literally zero instructions. The specification must not repeat that claim.

Instead, the isolation benchmark compares disabled V2 builds against compile-time exclusion. It reports request-handler latency and instruction counts.

Any observed delta remains a measured result, not a predefined zero.

## 11. Morsel contract

A leased morsel contains bounded CPU work. It may scan memory, classify values, format terms, or assemble owned output fragments.

A leased morsel must not perform socket I/O, wait for `io_uring`, acquire an unbounded external lock, or await another executor.

Potential major page faults remain possible. Instrumentation must therefore measure wall time and CPU time for every morsel kind.

The initial morsel size is an experimental parameter. The design assigns no duration guarantee before measurement.

If a morsel exceeds the selected budget, the implementation splits its CPU work or moves blocking work outside the helper pool.

## 12. Required instrumentation

Each morsel records its kind, queue delay, wall duration, CPU duration, result size, and completion state.

The scheduler records foreground arrival time, demand epoch, active helper count, and the last helper-release time.

Revocation latency starts at the `QueryRegistry` start callback. It ends when the active helper count reaches zero.

Interactive latency starts at query registration. Time to first response byte ends at the first successful response write.

Reports must include p50, p99, and p99.9 values. They must separate each morsel kind and injected stall type.

## 13. Verification prerequisites

### 13.1 Deterministic unit tests

Use barriers and a fake demand source to test every state transition. Tests must not depend on scheduler timing.

Cover stale epochs, empty jobs, one morsel, revoked pending work, completion during revocation, cancellation, and scheduler shutdown.

Verify that each morsel result becomes visible exactly once and in sequence order.

### 13.2 ThreadSanitizer stress test

Run concurrent session creation, admission, revocation, cancellation, completion, and scheduler destruction under ThreadSanitizer.

Use deterministic seeds and publish the seed for every failure. A passing run has zero data-race and lifetime reports.

### 13.3 Runtime isolation test

Start a runtime-disabled server and execute the existing V1 query suite. Assert that no scheduler instance or V2 callback exists.

Build with V2 excluded and repeat the same suite. Compare outputs, request latency, and retired instruction counts.

### 13.4 Chaos preemption benchmark

Run one V2 export, then inject concurrent queries while helper morsels execute. Repeat with CPU pressure, page faults, and delayed output.

Delayed output occurs on the coordinator or asynchronous I/O path. It must never block a leased helper.

Publish distributions before choosing an acceptance threshold. The measured threshold then becomes WP7's performance Definition of Done.

## 14. Delivery sequence

1. Land the scheduler core with a fake demand source and deterministic tests.
2. Land the `QueryRegistry` callback adapter and compile-time gate in a separate change.
3. Land TSAN stress coverage before enabling more than one helper.
4. Land instrumentation and the chaos benchmark before claiming any preemption bound.
5. Compare queue backends only after the correctness baseline passes.

WP8 must not depend on elastic helpers. It integrates the primary single-core path first and treats WP7 capacity as optional.

## 15. Exit criteria for prerequisite design

Implementation may begin after reviewers accept the module boundary, foreground signal, state machine, and ownership model.

The review must also accept the removal of blocking I/O from leased morsels and the replacement of unmeasured latency claims.

No throughput or preemption number is considered validated by this design document.

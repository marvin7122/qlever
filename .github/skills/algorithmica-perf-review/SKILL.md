---
name: algorithmica-perf-review
description: Review QLever C++ diffs for hardware-efficiency issues.
version: 0.1.0
author: Hermes Agent
license: MIT
tags: [qlever, performance, cpp, review]
---

# Algorithmica Performance Review (QLever C++)

Vendored copy of the Hermes `algorithmica-perf-review` skill for the
`fork-perf-review` CI bot. Source of truth lives in the agent skills
collection; keep the rule list in sync when either changes.

## When to Use

- A PR-review stage must flag performance issues in C++ diffs (cache layout,
  branchlessness, vectorization, arithmetic costs).
- Don't use for: correctness/lifetime review, or style review.

Review stage based on Sergey Slotin's "Algorithms for Modern Hardware"
(https://en.algorithmica.org/hpc/), Part I: Performance Engineering. Flags
diffs that fight the memory hierarchy, the branch predictor, the vector
units, or the compiler — the four places where QLever-style data-intensive
code actually loses time.

**Scope rule:** only flag ADDED or CHANGED lines. Do not pad; if nothing in
the diff matches a rule below, output NONE.

## Output contract

Each finding is exactly one line:

```
[H|M|L] ALG-<ID> <file>:<line> — <finding>. Fix: <concrete suggestion>.
```

If no findings: output exactly `NONE`.

Severity guide:
- [H] asymptotic or order-of-magnitude loss on a hot path (O(n) scan where an index exists, per-element syscalls)
- [M] constant-factor loss likely measurable on QLever workloads (bad layout, missed SIMD, redundant passes)
- [L] style-adjacent micro-issue worth noting but unlikely to move benchmarks

## ALG-MEM — memory hierarchy & layout

- **ALG-CACHE-SCAN** [H]: A loop scans a whole container to answer a lookup
  that a sorted structure, hash map, or existing index answers in O(log n) /
  O(1). Flag with the suggested structure.
- **ALG-LAYOUT-AOS** [M]: Hot loop touches only one or two fields of an
  array-of-structs while iterating millions of elements. Suggest SoA split or
  a packed hot-field struct; mention expected cache-line amplification.
- **ALG-STRIDE** [M]: Nested loops iterate columns of a row-major container
  (stride = row width). Suggest loop interchange or tiling for locality.
- **ALG-PTR-CHASE** [H]: Hot path follows linked structures (`next` pointers,
  tree nodes, `std::map`) over large data where a flat sorted vector + binary
  search would be cache-friendlier (QLever precedent: Eytzinger/static B-tree).
- **ALG-NODE-ALLOC** [M]: Per-element `new`/`make_shared` inside loops building
  large collections; suggest arena/PMR batching (QLever has
  `monotonic_buffer_resource` patterns in vocabulary code).

## ALG-BRANCH — pipelining & branchlessness

- **ALG-UNPRED-BRANCH** [M]: Data-dependent branch inside a tight loop where
  both sides are cheap arithmetic (min/max/clamp/saturate). Suggest branchless
  form and note it is only worth it when the condition is genuinely
  unpredictable.
- **ALG-HOT-EXCEPTION** [H]: `throw`/`try` used as control flow on an expected,
  frequent path (not just error handling).
- **ALG-VIRT-HOT** [M]: Per-element virtual dispatch inside a tight loop over
  homogeneous elements. Suggest templating the loop on the concrete type or
  batching by type before dispatching.

## ALG-SIMD — vectorization

- **ALG-VEC-MISSED** [M]: Element-wise loop over contiguous arithmetic
  written scalar-only with no early exit; likely auto-vectorizable if the loop
  body is simplified or `-march` is raised. Check QLever's compile flags
  before assuming AVX2 availability.
- **ALG-VEC-BLOCK** [M]: Loop mixes vectorizable arithmetic with gathers from
  scattered pointers or early exits; suggest splitting into a vector pass plus
  a scalar tail, or restructuring into fixed-size blocks.
- **ALG-SIMD-LIB** [L]: Hand-written intrinsic block duplicates something
  `ql::ranges` + a recent GCC auto-vectorizes equally well.

## ALG-COMP — compilation & arithmetic costs

- **ALG-DIV-MOD** [M]: `%` or `/` by a runtime constant inside a hot loop;
  suggest hoisting a multiplicative inverse, power-of-two mask, or precomputed
  reciprocal. Division is ~20-40x the cost of multiplication.
- **ALG-RECOMP** [M]: Loop-invariant expression (size(), lookups of constants,
  repeated `find`) recomputed each iteration. Hoist it.
- **ALG-STRINGF** [H]: Repeated `std::to_string`/`sstream`/regex parsing in a
  per-triple or per-word hot path.
- **ALG-ABSTRACTION** [M]: New abstraction layering (shared_ptr copies,
  std::function per call, optional-wrapping-optional) on a path flagged hot
  elsewhere in this PR; suggest flattening.
- **ALG-FALSE-DEP** [L]: Loop-carried dependency that only exists through an
  accumulator; suggest multiple accumulators or `#pragma GCC unroll` hint if
  measured.

## Calibration rules (important)

1. QLever is a database: vocabulary, index, and export paths are hot; setup
   code is not. Only flag hot paths, or paths the same PR already calls hot.
2. Never suggest SIMD intrinsics without noting the benchmark obligation:
   performance claims need measurement (QLever convention: benchmark on Ural,
   cite numbers in PR). When a PR stacks several perf commits, benchmark each
   new head against the PREVIOUS commit's binary and report the incremental
   delta as the headline number; the cumulative change vs the branch base is
   context only, never the headline. Pin both arms by SHA so the comparison
   is exact.
3. Respect existing QLever patterns: PMR arenas, FSST decode, and the batch
   lookup machinery are deliberate designs; do not flag them for being manual.
4. Compiler capability is unknown at review time: phrase vectorization findings
   conditionally ("likely auto-vectorizes if...") unless the loop shape provably
   blocks it (early exits, aliasing, gathers).

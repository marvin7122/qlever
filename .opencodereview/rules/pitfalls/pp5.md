---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, pitfall]
last_verified: 2026-09-22
---
# PP5: Copy-paste duplication instead of reusing/extracting

## Statement
No copied code blocks in the diff; repeated logic is factored into a helper or an existing function is called instead.

## Sources
- review: `.ai-review/personal_pitfalls.md#PP5` Mined from maintainer review comments, file dated 2026-09-22.
- review: `QLever PR #102` "The build logic for `std::unordered_set` is duplicated in the `Probe set` measurement; factor it into a helper."
- review: `QLever PR #107` "This hand-rolled hash duplicates the hash already defined for `Id`/`ValueId`; reuse the existing hash implementation."
- review: `QLever PR #55` "The `RawCqe` struct is defined locally; extract it to a reusable private type."
- review: `QLever PR #94` "The `ChunkEscapeMask32::isAllClean()` method is logically equivalent to `!hasEscape()`; simplify the redundant implementation."
- review: `QLever PR #78` "Significant code duplication exists between the split vocabulary end-to-end and micro benchmarks; extract shared components."
- review: `QLever PR #79` "Identical `parseEnvironmentSize` lambda appears in both benchmark files and other benchmark files; move it to a shared utility."
- review: `QLever PR #2633` "don't copy and paste the code, but use the function that is already present."
- review: `QLever PR #2763` "This beginning is exactly `tryGet`, call this function here to remove code duplication."
- review: `QLever PR #3057` "There is a lot of code duplication between the two oracles ... work something out there."
- review: `QLever PR #4` "small questions about code duplication + one answered question."
- review: `QLever PR #86` "Significant code duplication between runMemcpyBenchmark and runStreamingWriterBenchmark: both allocate identical buffers, vocab tables, probe indices, warm cache, and set up perf counters."
- review: `QLever PR #99` "Significant code duplication in `describeDecision` method — it reimplements the exact same parameter/header parsing logic already present in `selectEngine`."
- review: `.opencodereview/rule.json#144` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Frequency in the mined comments: ~124 (of 639 review comments by joka921/hannahbast).
Fan-out rule wording: Copy-paste duplication instead of reusing/extracting: No copied code blocks in the diff; repeated logic is factored into a helper or an existing function is called instead.

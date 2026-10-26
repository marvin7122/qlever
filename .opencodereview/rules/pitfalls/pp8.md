---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, pitfall]
last_verified: 2026-09-22
---
# PP8: Poor or non-conventional naming

## Statement
New identifiers follow codebase conventions: data members end with `_`, camelCase (no `kBla`), names describe external meaning (e.g. `batchOffset`, `rowIdxInIdTable`), and are used consistently across files.

## Sources
- review: `.ai-review/personal_pitfalls.md#PP8` Mined from maintainer review comments, file dated 2026-09-22.
- review: `QLever review comment` "`data` is a vague name for this vector of generated `Id` values; use a specific name such as `inputIds` or `buildSideIds`. (#102)"
- review: `QLever PR #107` "Template parameter `RadixBits` uses PascalCase; use the project's camelCase convention."
- review: `QLever PR #162` "LCG constants in test helper: makeConcurrentTestBatches uses standard 64-bit LCG constants (multiplier 6364136223846793005ull, increment 1442695040888963407ull). Consider adding a comment citing the source (e.g., 'PCG / Knuth LCG constants') for maintainability."
- review: `QLever PR #78` "The magic hash constant appears without a named constant or explanatory comment."
- review: `QLever PR #55` "The redundant cast adds visual noise; `enumerate` already supplies a `size_t`."
- review: `QLever PR #78` "Multiple lines exceed reasonable length due to verbose namespace qualification; consider a namespace alias."
- review: `QLever PR #78` "Line exceeds reasonable length due to verbose namespace qualification; consider a namespace alias."
- review: `QLever PR #2824` "Data members should end with an `_` so `rdfTermString_`"
- review: `QLever review comment` "The test literal suffix `4u` is inconsistent with the surrounding test style and should be removed. (#70)"
- review: `QLever PR #102` "The `BlockedBloomFilter` class uses a fixed number of hash functions (8) determined by the `SALTS` array."
- review: `QLever PR #91` "Magic numbers used as configuration defaults appear as raw literals in multiple benchmark functions."
- review: `QLever PR #91` "Multiple tests use magic numbers for `zeroCopyThresholdBytes` and `maxChunkBytes` without named constants or explanatory comments."
- review: `QLever PR #118` "Magic number `1024` is used as a default queue capacity in several places ... Consider defining a named constant."
- review: `QLever PR #118` "Inconsistent naming: the member variable `maxForegroundQueriesForHelperAdmission_` ... align the terminology."
- review: `QLever PR #3035` "we don't use the `kBla` convention (just `numIoManagers` is fine)."
- review: `QLever PR #2938` "Okay, in the header file, always call this `rowIdxInIdTable` (currently called `absoluteRow` in some places."
- review: `QLever PR #114` "The test name still refers to a \"DirectTypedTuple\" although this PR uses MonomorphicRowSerializer terminology; this makes the test suite harder to search/navigate consistently."
- review: `QLever PR #84` "Buffer sizes (32 and 64) and prefix length (31) are used as magic numbers without explanatory constants or comments."
- review: `QLever PR #86` "Cache hit ratio estimation uses architecture-specific magic constants (3 ns L1 hit, 60 ns DRAM miss)."
- review: `QLever PR #86` "Chunk sizes (64 KB, 1 MB, 16 MB, 256 MB) are hardcoded as class constants."
- review: `QLever PR #90` "Magic numbers for URI/literal suffix lengths (45, 44, 46, 52, etc.) are hardcoded in `memcpy` calls."
- review: `QLever PR #87` "The lambda captures an unused second parameter (size_t) without marking it `[[maybe_unused]]` or naming it _."
- review: `QLever PR #84` "The magic constants 52429 and the right shift by 3 in the SSE2 path ... lack explanatory comments."
- review: `.opencodereview/rule.json#147` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Frequency in the mined comments: ~135 (of 639 review comments by joka921/hannahbast).
Fan-out rule wording: Poor or non-conventional naming: New identifiers follow codebase conventions: data members end with `_`, camelCase (no `kBla`), names describe external meaning (e.g. `batchOffset`, `rowIdxInIdTable`), and are used consistently across files.

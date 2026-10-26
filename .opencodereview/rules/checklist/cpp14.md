---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP14: PMR allocators

## Statement
PMR allocators: when using PMR (polymorphic memory resource) buffers, wrap in `ql::pmr::polymorphic_allocator` and use `ql::ranges::copy`, container constructors with allocator params, or allocator-aware algorithms — never raw `allocate()` + `memcpy`/manual transfer. The allocator-based interface provides safety (abstracts `void*` casts, clarifies lifetime), clarity (self-documenting intent), and zero runtime cost (`ql::ranges::copy` compiles to `memcpy`). Applies to all PMR-backed result construction: `CompressedVocabulary`, `VocabularyTypes`, `IndexBuilder`, `NamedCachedQueryBlobManager`.

## Sources
- review: `.opencodereview/rule.json#6` QLever OCR fan-out project rule, file dated 2026-09-23.
- doc: `QLever PMR patterns; Abseil PMR guidelines` Cited for CPP14 by REVIEW_CHECKLIST.md.

## Notes
Fan-out review trigger words: `memory_resource`, `pmr`, `polymorphic_allocator`.

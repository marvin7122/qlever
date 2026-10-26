---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP38: Provide custom `swap`/operators as hidden friends in the type's namespace (ADL-friendly)

## Statement
Provide custom `swap`/operators as hidden friends in the type's namespace (ADL-friendly); use the `using std::swap; swap(a,b);` pattern, never an unqualified `std::swap` for user types.

## Sources
- review: `.opencodereview/rule.json#32` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `friend`, `swap`.

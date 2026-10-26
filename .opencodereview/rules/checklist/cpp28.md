---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP28: Use `std::atomic<T>` (not `volatile`) for cross-thread flags/counters

## Statement
Use `std::atomic<T>` (not `volatile`) for cross-thread flags/counters; `volatile` gives no inter-thread ordering in C++.

## Sources
- review: `.opencodereview/rule.json#21` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `atomic`, `volatile`.

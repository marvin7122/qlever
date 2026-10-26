---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP24: Lock multiple mutexes via `std::lock` / `std::scoped_lock` to avoid deadlock

## Statement
Lock multiple mutexes via `std::lock` / `std::scoped_lock` to avoid deadlock; never lock the same pair of mutexes in inconsistent order on different code paths.

## Sources
- review: `.opencodereview/rule.json#17` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `mutex`, `scoped_lock`, `std::lock`.

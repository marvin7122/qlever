---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP23: Use `std::scoped_lock` / `std::lock_guard`, never manual `mutex.lock()`/`unlock()` that …

## Statement
Use `std::scoped_lock` / `std::lock_guard`, never manual `mutex.lock()`/`unlock()` that an early return or throw can skip.

## Sources
- review: `.opencodereview/rule.json#16` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `[.>]lock\(\)`, `[.>]unlock\(\)`, `lock_guard`, `mutex`, `scoped_lock`, `unique_lock`.

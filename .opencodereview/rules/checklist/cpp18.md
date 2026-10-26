---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP18: A `noexcept` function's body must not call potentially-throwing code without handling it

## Statement
A `noexcept` function's body must not call potentially-throwing code without handling it; a throw inside `noexcept` calls `std::terminate`.

## Sources
- review: `.opencodereview/rule.json#10` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `noexcept`.

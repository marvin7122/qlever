---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP72: Use `steady_clock` for elapsed-time measurement (monotonic) and `system_clock` for …

## Statement
Use `steady_clock` for elapsed-time measurement (monotonic) and `system_clock` for wall-clock; never measure elapsed time with `system_clock` (it can jump backward).

## Sources
- review: `.opencodereview/rule.json#71` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `steady_clock`, `system_clock`.

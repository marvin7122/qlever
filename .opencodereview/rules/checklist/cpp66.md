---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP66: `auto` drops reference/cv from the initializer unless `auto&`/`const auto&`

## Statement
`auto` drops reference/cv from the initializer unless `auto&`/`const auto&`; confirm the deduced type (e.g. `auto n = v.size()` is `size_t`, not `int`).

## Sources
- review: `.opencodereview/rule.json#64` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `auto`.

---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP62: Containers of polymorphic objects hold (smart) pointers, not base-by-value, which would …

## Statement
Containers of polymorphic objects hold (smart) pointers, not base-by-value, which would slice off derived parts.

## Sources
- review: `.opencodereview/rule.json#60` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `shared_ptr`, `unique_ptr`, `virtual`.

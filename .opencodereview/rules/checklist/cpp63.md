---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP63: Prefer brace-init `T x{...}` to avoid narrowing and the most-vexing-parse

## Statement
Prefer brace-init `T x{...}` to avoid narrowing and the most-vexing-parse; be aware it can preferentially select an `initializer_list` constructor.

## Sources
- review: `.opencodereview/rule.json#61` QLever OCR fan-out project rule, file dated 2026-09-23.

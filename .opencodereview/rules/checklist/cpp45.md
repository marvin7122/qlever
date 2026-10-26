---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP45: Do not compare `float`/`double` with `==`

## Statement
Do not compare `float`/`double` with `==`; use a tolerance (`std::abs(a-b) < eps`) and watch accumulation order for precision loss.

## Sources
- review: `.opencodereview/rule.json#40` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `double`, `float`.

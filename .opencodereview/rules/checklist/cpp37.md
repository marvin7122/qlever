---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP37: Values/functions used as compile-time constants are marked `constexpr`

## Statement
Values/functions used as compile-time constants are marked `constexpr`; a `constexpr` function that is never actually const-evaluable at its call site signals a contract mismatch (e.g. it touches non-const globals).

## Sources
- review: `.opencodereview/rule.json#31` QLever OCR fan-out project rule, file dated 2026-09-23.

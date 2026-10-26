---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP44: Guard division/modulo against a zero divisor and the `INT_MIN / -1` overflow case

## Statement
Guard division/modulo against a zero divisor and the `INT_MIN / -1` overflow case; verify the divisor's domain before the operation.

## Sources
- review: `.opencodereview/rule.json#39` QLever OCR fan-out project rule, file dated 2026-09-23.

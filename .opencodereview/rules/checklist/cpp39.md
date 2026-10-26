---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP39: Single-argument constructors (and conversion operators) are `explicit` unless an …

## Statement
Single-argument constructors (and conversion operators) are `explicit` unless an intentional, cheap, lossless conversion; implicit conversions hide overload-resolution bugs.

## Sources
- review: `.opencodereview/rule.json#33` QLever OCR fan-out project rule, file dated 2026-09-23.

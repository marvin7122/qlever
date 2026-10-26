---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP41: No signed integer overflow (UB)

## Statement
No signed integer overflow (UB); index/size arithmetic that can overflow uses a wider type or a checked operation (`gsl::narrow`).

## Sources
- review: `.opencodereview/rule.json#36` QLever OCR fan-out project rule, file dated 2026-09-23.

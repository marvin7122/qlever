---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP43: No shift by ≥ the type's width, or by a negative amount (UB)

## Statement
No shift by ≥ the type's width, or by a negative amount (UB); clamp shift counts to a valid range before shifting.

## Sources
- review: `.opencodereview/rule.json#38` QLever OCR fan-out project rule, file dated 2026-09-23.

---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP60: Do not redefine an inherited non-virtual function

## Statement
Do not redefine an inherited non-virtual function — it hides the base function and breaks polymorphism; use `override` or rename.

## Sources
- review: `.opencodereview/rule.json#58` QLever OCR fan-out project rule, file dated 2026-09-23.

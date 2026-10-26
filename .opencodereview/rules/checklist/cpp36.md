---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP36: A template specialization must honor the primary template's contract

## Statement
A template specialization must honor the primary template's contract; silent behavioral divergence (e.g. different `noexcept` or semantics) between specialization and primary is a trap.

## Sources
- review: `.opencodereview/rule.json#30` QLever OCR fan-out project rule, file dated 2026-09-23.

---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, qc]
last_verified: 2026-09-23
---
# QC1: Precondition violations (caller's fault) use `AD_CONTRACT_CHECK(cond)`

## Statement
Precondition violations (caller's fault) use `AD_CONTRACT_CHECK(cond)`; internal logic errors (our fault) use `AD_CORRECTNESS_CHECK(cond)` — the split is semantically correct, not interchangeable.

## Sources
- review: `.opencodereview/rule.json#149` QLever OCR fan-out project rule, file dated 2026-09-23.

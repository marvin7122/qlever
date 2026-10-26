---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, qc]
last_verified: 2026-09-23
---
# QC2: Unreachable code paths use `AD_FAIL()`

## Statement
Unreachable code paths use `AD_FAIL()`; runtime errors thrown via `AD_THROW("...")` (from `src/util/Exception.h`), not bare `throw std::runtime_error`.

## Sources
- review: `.opencodereview/rule.json#159` QLever OCR fan-out project rule, file dated 2026-09-23.

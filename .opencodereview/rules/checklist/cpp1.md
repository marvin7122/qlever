---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP1: Parameter passing

## Statement
Parameter passing: by value for cheap/sink, const& for expensive in-params, avoid non-const& out-params (prefer return values/structs).

## Sources
- review: `.opencodereview/rule.json#1` QLever OCR fan-out project rule, file dated 2026-09-23.
- doc: `Core Guidelines F.15, F.16, F.20` Cited for CPP1 by REVIEW_CHECKLIST.md.

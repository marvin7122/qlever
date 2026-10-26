---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP9: Errors via exceptions/expected, not sentinels

## Statement
Errors via exceptions/expected, not sentinels; no empty catch; dtors/move ops noexcept.

## Sources
- review: `.opencodereview/rule.json#73` QLever OCR fan-out project rule, file dated 2026-09-23.
- doc: `Core Guidelines E.2, E.14, C.66` Cited for CPP9 by REVIEW_CHECKLIST.md.

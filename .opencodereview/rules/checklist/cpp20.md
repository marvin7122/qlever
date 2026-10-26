---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP20: Constructors acquire every resource via RAII and throw on failure rather than leaving a …

## Statement
Constructors acquire every resource via RAII and throw on failure rather than leaving a half-built "isValid" object that callers must remember to check.

## Sources
- review: `.opencodereview/rule.json#13` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `IsValid`, `isValid`, `is_valid`.

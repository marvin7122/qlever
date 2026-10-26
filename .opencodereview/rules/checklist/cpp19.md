---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP19: Catch clauses must be ordered most-derived-first

## Statement
Catch clauses must be ordered most-derived-first; catching a base by value or reference before a derived handler slices away the derived information.

## Sources
- review: `.opencodereview/rule.json#11` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `catch`.

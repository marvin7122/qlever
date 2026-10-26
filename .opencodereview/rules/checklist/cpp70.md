---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP70: Prefer a `= default`ed `operator<=>`

## Statement
Prefer a `= default`ed `operator<=>`; do not define pairwise comparison operators inconsistently with one another.

## Sources
- review: `.opencodereview/rule.json#69` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `<=>`, `operator<`, `operator==`, `operator>`.

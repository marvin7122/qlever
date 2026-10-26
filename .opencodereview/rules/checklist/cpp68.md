---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP68: Symmetric binary operators (`==`, `<`, `+`) should be non-member hidden friends so …

## Statement
Symmetric binary operators (`==`, `<`, `+`) should be non-member hidden friends so conversions apply to the left operand too; C++20 generates `!=` from `==` and `==`/`<` from `operator<=>`.

## Sources
- review: `.opencodereview/rule.json#66` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `friend`, `operator`.

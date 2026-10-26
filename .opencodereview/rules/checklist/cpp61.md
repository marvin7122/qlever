---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP61: Do not pass or return polymorphic objects by value of the base type (slicing)

## Statement
Do not pass or return polymorphic objects by value of the base type (slicing); use `const` reference, `Base*`, or `unique_ptr<Base>`.

## Sources
- review: `.opencodereview/rule.json#59` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `: public`, `virtual`.

---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP51: Use `enum class` (scoped, no implicit `int` conversion) not a plain `enum`

## Statement
Use `enum class` (scoped, no implicit `int` conversion) not a plain `enum`; unrelated enums then cannot be mixed or implicitly compared as integers.

## Sources
- review: `.opencodereview/rule.json#47` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `enum`.

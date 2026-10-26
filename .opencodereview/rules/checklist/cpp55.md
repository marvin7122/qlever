---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP55: Do not chain adaptors over a temporary container and store the result without …

## Statement
Do not chain adaptors over a temporary container and store the result without materializing it (e.g. into a `vector`); the temporary dies at the end of the full expression.

## Sources
- review: `.opencodereview/rule.json#52` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `ranges::`, `views::`.

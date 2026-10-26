---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP40: Do not mix signed and unsigned in comparisons/arithmetic

## Statement
Do not mix signed and unsigned in comparisons/arithmetic — unsigned promotion silently swallows negative values; compare like types or cast deliberately.

## Sources
- review: `.opencodereview/rule.json#35` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `.size()`, `size_t`, `uint`, `unsigned`.

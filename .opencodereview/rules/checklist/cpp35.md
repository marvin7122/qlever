---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP35: Template code that could hard-error for some types must be guarded (`if constexpr` / …

## Statement
Template code that could hard-error for some types must be guarded (`if constexpr` / SFINAE) so substitution failure is soft; do not let the primary template instantiate with an invalid type.

## Sources
- review: `.opencodereview/rule.json#29` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `SFINAE`, `concept`, `enable_if`, `if constexpr`.

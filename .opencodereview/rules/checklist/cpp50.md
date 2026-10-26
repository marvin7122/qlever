---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP50: Do not store or use the pointer from `.c_str()`/`.data()` after the `std::string` is …

## Statement
Do not store or use the pointer from `.c_str()`/`.data()` after the `std::string` is modified, moved, or destroyed — it dangles.

## Sources
- review: `.opencodereview/rule.json#46` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `\.data\(\)`, `c_str`.

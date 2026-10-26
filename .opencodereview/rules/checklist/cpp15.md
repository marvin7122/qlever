---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP15: Do NOT `std::move` a function-local variable that is the function's return value

## Statement
Do NOT `std::move` a function-local variable that is the function's return value — it suppresses copy elision (RVO/NRVO). Return the local by name.

## Sources
- review: `.opencodereview/rule.json#7` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `std::move`.

---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP71: Represent time with `std::chrono` duration/clock types, not a raw `int` "milliseconds"

## Statement
Represent time with `std::chrono` duration/clock types, not a raw `int` "milliseconds"; encoding units in the type prevents silent unit swaps.

## Sources
- review: `.opencodereview/rule.json#70` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `chrono`, `clock`, `duration`, `milliseconds`, `seconds`.

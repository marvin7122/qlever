---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP16: Perfect-forwarding functions must forward with `std::forward<T>(t)` using the exact …

## Statement
Perfect-forwarding functions must forward with `std::forward<T>(t)` using the exact deduced template parameter type; do not forward a forwarding reference as a value or to the wrong overload (that silently turns an rvalue into an lvalue or vice-versa).

## Sources
- review: `.opencodereview/rule.json#8` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `std::forward`.

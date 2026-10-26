---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP42: Store sizes/indices in `size_t`/`ptrdiff_t` consistently

## Statement
Store sizes/indices in `size_t`/`ptrdiff_t` consistently; do not keep them in `int` and then compare with `container.size()`.

## Sources
- review: `.opencodereview/rule.json#37` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `.size()`, `size_t`.

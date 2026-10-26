---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP13: A move operation must leave the source in a valid (destructible and reassignable) state

## Statement
A move operation must leave the source in a valid (destructible and reassignable) state; the diff must not read a moved-from object except to assign to or destroy it, and must not assume the source is now empty.

## Sources
- review: `.opencodereview/rule.json#5` QLever OCR fan-out project rule, file dated 2026-09-23.

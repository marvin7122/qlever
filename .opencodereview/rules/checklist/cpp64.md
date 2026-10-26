---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP64: Avoid the most-vexing-parse

## Statement
Avoid the most-vexing-parse: `Widget w(foo());` declares a function, not a variable; use `Widget w{foo()}` or `= foo()`.

## Sources
- review: `.opencodereview/rule.json#62` QLever OCR fan-out project rule, file dated 2026-09-23.

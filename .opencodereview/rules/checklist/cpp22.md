---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP22: Every manually-acquired resource (file, socket, raw lock, allocation outside a smart …

## Statement
Every manually-acquired resource (file, socket, raw lock, allocation outside a smart pointer) is owned by an RAII object with deterministic release on all paths including early returns and throws.

## Sources
- review: `.opencodereview/rule.json#15` QLever OCR fan-out project rule, file dated 2026-09-23.

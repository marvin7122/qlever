---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP52: Structured bindings (`auto& [a,b] = ...`) are references into the source

## Statement
Structured bindings (`auto& [a,b] = ...`) are references into the source; ensure the source outlives the bindings, or copy explicitly when ownership is needed.

## Sources
- review: `.opencodereview/rule.json#48` QLever OCR fan-out project rule, file dated 2026-09-23.

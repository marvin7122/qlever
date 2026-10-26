---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP56: Wide-contract functions that never throw are marked `noexcept` to document the contract …

## Statement
Wide-contract functions that never throw are marked `noexcept` to document the contract and enable optimization; do not mark a function `noexcept` merely to satisfy a constraint if it can realistically throw.

## Sources
- review: `.opencodereview/rule.json#53` QLever OCR fan-out project rule, file dated 2026-09-23.

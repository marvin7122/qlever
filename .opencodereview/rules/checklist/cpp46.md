---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP46: Choose the container by access pattern

## Statement
Choose the container by access pattern: `vector` (contiguous/random), `deque` (cheap front/back), `map`/`unordered_map` (keyed); do not use `list`/`map` where `vector` suffices.

## Sources
- review: `.opencodereview/rule.json#41` QLever OCR fan-out project rule, file dated 2026-09-23.

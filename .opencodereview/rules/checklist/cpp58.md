---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP58: Never call virtual functions from constructors or destructors

## Statement
Never call virtual functions from constructors or destructors — dispatch does not reach the most-derived override while the object is half-built/destroyed.

## Sources
- review: `.opencodereview/rule.json#55` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `virtual`.

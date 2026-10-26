---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP27: A `std::thread`/`std::jthread` must be joined or detached (or its stop requested) before …

## Statement
A `std::thread`/`std::jthread` must be joined or detached (or its stop requested) before the owning scope ends; a detached thread that still references stack locals is UB.

## Sources
- review: `.opencodereview/rule.json#20` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `jthread`, `thread`.

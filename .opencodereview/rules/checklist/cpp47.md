---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP47: Do not hold iterators/references/pointers into a `vector` across a mutating …

## Statement
Do not hold iterators/references/pointers into a `vector` across a mutating `push_back`/`insert`/`emplace` that may reallocate; a range/view over it dangles on modification.

## Sources
- review: `.opencodereview/rule.json#42` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `emplace`, `insert`, `push_back`, `reserve`, `resize`.

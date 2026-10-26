---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP48: When erasing from `vector`/`deque` while iterating, use the iterator `erase()` returns …

## Statement
When erasing from `vector`/`deque` while iterating, use the iterator `erase()` returns (or the erase–remove idiom), not a blindly incremented index.

## Sources
- review: `.opencodereview/rule.json#43` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `erase`, `remove`.

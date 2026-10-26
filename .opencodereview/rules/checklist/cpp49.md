---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP49: References/pointers into `unordered_map`/`unordered_set` do not survive a rehash

## Statement
References/pointers into `unordered_map`/`unordered_set` do not survive a rehash; `reserve()` up front for many inserts and do not keep element pointers across unknown rehashes.

## Sources
- review: `.opencodereview/rule.json#44` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `rehash`, `reserve`, `unordered_map`, `unordered_set`.

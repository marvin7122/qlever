---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP53: A lazy view pipeline (`views::filter`/`transform`/...) is non-owning

## Statement
A lazy view pipeline (`views::filter`/`transform`/...) is non-owning; storing or returning the view from a function that owns the underlying range dangles once that range dies.

## Sources
- review: `.opencodereview/rule.json#49` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `generator`, `ranges::`, `views::`.

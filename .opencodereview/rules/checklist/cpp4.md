---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP4: std::move on last use of sink params

## Statement
std::move on last use of sink params; std::forward only for forwarding refs; no move on const or on returned locals.

## Sources
- review: `.opencodereview/rule.json#34` QLever OCR fan-out project rule, file dated 2026-09-23.
- doc: `Effective Modern C++ Items 23-25` Cited for CPP4 by REVIEW_CHECKLIST.md.

## Notes
Fan-out review trigger words: `std::forward`, `std::move`.

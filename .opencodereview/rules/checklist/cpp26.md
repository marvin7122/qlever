---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP26: `std::shared_ptr` is thread-safe only for its control block

## Statement
`std::shared_ptr` is thread-safe only for its control block; the pointee is NOT automatically thread-safe — protect shared data behind a mutex.

## Sources
- review: `.opencodereview/rule.json#19` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `shared_ptr`.

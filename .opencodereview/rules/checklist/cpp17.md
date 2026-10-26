---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP17: A destructor (or anything it calls) must never let an exception escape

## Statement
A destructor (or anything it calls) must never let an exception escape; a throw during stack unwinding calls `std::terminate`.

## Sources
- review: `.opencodereview/rule.json#9` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `catch`, `noexcept`, `terminate`, `throw`, `~`.

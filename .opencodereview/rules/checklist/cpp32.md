---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP32: A generator/range returned by a coroutine that borrows from its own or the caller's …

## Statement
A generator/range returned by a coroutine that borrows from its own or the caller's locals must not be iterated after that frame is destroyed (the classic dangling-generator bug).

## Sources
- review: `.opencodereview/rule.json#26` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `co_await`, `co_return`, `co_yield`, `coroutine`, `generator`.

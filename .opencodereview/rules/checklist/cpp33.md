---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP33: Do not resume a coroutine after it has reached its final suspension / been destroyed

## Statement
Do not resume a coroutine after it has reached its final suspension / been destroyed; `co_await` on a destroyed frame is UB.

## Sources
- review: `.opencodereview/rule.json#27` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `co_await`, `coroutine`.

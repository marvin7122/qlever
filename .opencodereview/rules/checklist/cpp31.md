---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP31: A coroutine (generator / Task) that captures references or stack locals across an await …

## Statement
A coroutine (generator / Task) that captures references or stack locals across an await point must keep the frame alive for the entire suspension; dangling captures after the producer frame is destroyed are UB.

## Sources
- review: `.opencodereview/rule.json#25` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `co_await`, `co_return`, `co_yield`, `coroutine`, `generator`.

---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP29: A condition-variable `wait` must use a predicate (spurious wakeups) and avoid lost-wakeup

## Statement
A condition-variable `wait` must use a predicate (spurious wakeups) and avoid lost-wakeup; notify only after the guarded state change, with correct lock discipline.

## Sources
- review: `.opencodereview/rule.json#22` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `condition_variable`.

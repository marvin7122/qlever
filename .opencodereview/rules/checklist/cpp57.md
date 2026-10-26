---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP57: Overrides use the `override` keyword

## Statement
Overrides use the `override` keyword; base virtuals meant to forbid further overriding use `final`; a missing `override` silently defines a new (non-virtual) function.

## Sources
- review: `.opencodereview/rule.json#54` QLever OCR fan-out project rule, file dated 2026-09-23.

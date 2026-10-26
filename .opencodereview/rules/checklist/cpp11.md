---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP11: Lifetime-by-design for returned lazy views/generators

## Statement
Lifetime-by-design for returned lazy views/generators: never capture a reference parameter, stack local, or temporary *by reference* into a lambda/view/coroutine that outlives the current statement. The returned pipeline must be self-contained: value-capture cheap handles, or own data via value/`shared_ptr` when ownership is shared. "Safe under current join consumption" is not enough — correctness must not depend on an implicit iteration order. Applies to `views::transform`/`filter`/`join`, coroutines, and any deferred callback. Prefer by-value of a small non-owning handle (`TableWithRange`, `TableConstRefWithVocab`, `reference_wrapper`) over `shared_ptr` unless true shared ownership of the *resource* is required; `shared_ptr` of a non-owning handle does not extend the underlying `IdTable`/`LocalVocab` lifetime.

## Sources
- review: `.opencodereview/rule.json#3` QLever OCR fan-out project rule, file dated 2026-09-23.
- doc: `Core Guidelines F.53, ES.65; expanded CPP31–32, CPP54` Cited for CPP11 by REVIEW_CHECKLIST.md.

## Notes
Fan-out review trigger words: `generator`, `ranges::`, `views::`.

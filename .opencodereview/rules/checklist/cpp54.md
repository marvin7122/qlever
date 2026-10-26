---
kind: rule
severity: MUST
language: cpp
tags: [qlever, cpp]
last_verified: 2026-09-23
---
# CPP54: `views::transform` with a lambda capturing locals, or `views::filter` whose predicate …

## Statement
`views::transform` with a lambda capturing locals, or `views::filter` whose predicate references locals, dangles if the view outlives those captures.

b [MUST] **Lifetime-by-design (hard rule):** A function that *returns* a lazy range/view/generator must not leave that object holding a reference (or pointer) to any of the function's parameters or locals. Capture only by value (cheap handles / `reference_wrapper` copies / `shared_ptr` when true shared ownership of the resource is required). Do not rely on "the caller fully drains this before the outer iterator advances" as the sole safety argument — that is an implicit contract the type system cannot check and a later refactor can break. Pattern to reject: `auto f(const T& x) { return views::transform([&x](...){...}); }`. Pattern to accept: take `T` by value (if cheap) or copy the needed fields into the lambda before building the view. Note: `shared_ptr` of a non-owning *handle* does not extend the lifetime of the underlying buffer/`IdTable`/`LocalVocab` — only value-owning or true shared ownership of those resources does.

## Sources
- review: `.opencodereview/rule.json#50` QLever OCR fan-out project rule, file dated 2026-09-23.
- review: `.opencodereview/rule.json#51` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Fan-out review trigger words: `generator`, `ranges::`, `views::`.
rule.json carries this id 2 times; both wordings are kept above as separate paragraphs.

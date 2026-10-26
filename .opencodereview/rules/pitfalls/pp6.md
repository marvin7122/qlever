---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, pitfall]
last_verified: 2026-09-22
---
# PP6: Manual loops/indices instead of ranges pipelines

## Statement
New loops use `ql::views`/range-v3 pipelines (`transform`, `filter`, `zip`, `join`, `to_vector`, `chunk`) instead of index-based iteration or manual accumulation where applicable.

## Sources
- review: `.ai-review/personal_pitfalls.md#PP6` Mined from maintainer review comments, file dated 2026-09-22.
- review: `QLever PR #102` "The initialization loop is an index-based hand-rolled loop; use `std::generate` or another named algorithm where applicable."
- review: `QLever PR #107` "This new row traversal is an index-based manual loop; prefer `ql::views::iota` or a table row view."
- review: `QLever PR #55` "Prefer `std::copy` or `std::copy_n` over raw-pointer `memcpy` for this copy."
- review: `QLever PR #2652` "can you use the piped syntax `return constructTriples_ | ql::views::transform(...) | ql::views::filter(...)` please, for readability."
- review: `QLever PR #2710` "(no need for indices, range-v3 has `zip`"
- review: `QLever PR #2837` "Isn't this just `::ranges::to_vector(range)`?"
- review: `QLever PR #16` "I think when you clean up this pr (merge master in, reduce unnecessary diff, simplify ranges) and then open a PR against the QLever master, we should be able to get good progress."
- review: `.opencodereview/rule.json#145` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Frequency in the mined comments: ~409 (of 639 review comments by joka921/hannahbast).
Fan-out rule wording: Manual loops/indices instead of ranges pipelines: New loops use `ql::views`/range-v3 pipelines (`transform`, `filter`, `zip`, `join`, `to_vector`, `chunk`) instead of index-based iteration or manual accumulation where applicable.

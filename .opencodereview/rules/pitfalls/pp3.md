---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, pitfall]
last_verified: 2026-09-22
---
# PP3: Accidental reformatting / unrelated diff noise

## Statement
The diff contains no reformatting, reordering, or edits of lines whose logic is unchanged by the PR.

## Sources
- review: `.ai-review/personal_pitfalls.md#PP3` Mined from maintainer review comments, file dated 2026-09-22.
- review: `QLever PR #137` "The parameter list formatting is inconsistent with the surrounding function; keep the diff focused and consistent."
- review: `QLever PR #79` "Formatting inconsistency in the new overload should match the existing overload."
- review: `QLever PR #79` "Minor indentation inconsistency in the new call should match surrounding style."
- review: `QLever PR #79` "Formatting inconsistency: the new two-argument overload should use the existing multiline layout."
- review: `QLever PR #2633` "no accidental formatting changes."
- review: `QLever PR #2955` "Those reformattings seem accidental and unrelated, please undo them (sometimes they even break code examples)."
- review: `QLever PR #3036` "This is all completely unchanged, but accidentally reformatted, plese revert that part."
- review: `QLever PR #16` "I think when you clean up this pr (merge master in, reduce unnecessary diff, simplify ranges) and then open a PR against the QLever master, we should be able to get good progress."
- review: `.opencodereview/rule.json#142` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Frequency in the mined comments: ~25 (of 639 review comments by joka921/hannahbast).
Fan-out rule wording: Accidental reformatting / unrelated diff noise: The diff contains no reformatting, reordering, or edits of lines whose logic is unchanged by the PR.

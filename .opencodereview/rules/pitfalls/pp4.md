---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, pitfall]
last_verified: 2026-09-22
---
# PP4: Deleting pre-existing comments

## Statement
No pre-existing explanatory comment is removed by the diff unless the code it documents is removed.

## Sources
- review: `.ai-review/personal_pitfalls.md#PP4` Mined from maintainer review comments, file dated 2026-09-22.
- review: `QLever PR #107` "The pre-existing explanatory comment was replaced entirely; restore it unless the documented code is removed."
- review: `QLever PR #2654` "don't delete comments, but move them alognside the declaration?"
- review: `QLever PR #78` "The closing `}  // namespace` comment was removed ... removing the closing namespace comment makes it harder to track which namespace a particular section belongs to. The comment should be restored for clarity."
- review: `QLever PR #2710` "please readd those comments (was that your AI?)"
- review: `QLever PR #2824` "This comment still has value, was the AI over eager again here?"
- review: `.opencodereview/rule.json#143` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Frequency in the mined comments: ~17 (of 639 review comments by joka921/hannahbast).
Fan-out rule wording: Deleting pre-existing comments: No pre-existing explanatory comment is removed by the diff unless the code it documents is removed.

---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, pitfall]
last_verified: 2026-09-22
---
# PP1: Comment style violations

## Statement
Every comment in the diff is a full English sentence ending with a full stop, imperative mood, with C++ identifiers/types in backticks; every function/test has at least a dummy `// ____...` separator comment.

## Sources
- review: `.ai-review/personal_pitfalls.md#PP1` Mined from maintainer review comments, file dated 2026-09-22.
- review: `QLever PR #102` "The comment does not follow the project's comment style; use a full imperative sentence ending with a full stop and add the required separator."
- review: `QLever PR #107` "The TODO comment should use imperative mood, end with a full stop, and wrap code elements such as `vector` in backticks."
- review: `QLever PR #2654` "all comments are full sentences (fulfilled) and end with a full stop (not fulfilled)."
- review: `QLever PR #2824` "Add at least the empty dummy comments around ALL functions and test cases for consistency (many occurences in multiple files)."
- review: `QLever PR #2936` "(have a prompt unifying to the imperative style in all your comments)."
- review: `QLever PR #103` "The TODO text has grammatical issues (capitalization and sentence split), which makes it harder to read."
- review: `QLever PR #105` "The new TODO comment is grammatically incomplete and reads like a note-to-self rather than an actionable TODO."
- review: `QLever PR #109` "The TODO comment has grammar/punctuation issues (capitalization, missing punctuation, and \"cache friendly\" → \"cache-friendly\"), which makes it harder to read in a header comment."
- review: `.opencodereview/rule.json#124` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Frequency in the mined comments: ~92 (of 639 review comments by joka921/hannahbast).
Fan-out rule wording: Comment style violations: Every comment in the diff is a full English sentence ending with a full stop, imperative mood, with C++ identifiers/types in backticks; every function/test has at least a dummy `// ____...` separator comment.

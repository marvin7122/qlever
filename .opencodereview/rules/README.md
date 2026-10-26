# Review rules

The rules the OCR fan-out review and the human reviewers apply to this fork, one
Markdown file per rule, readable here and editable like any other source file.
`rule.json` next to this directory is the runner's prompt set; these files are the
same rules in a form a person can read, with their sources and the review quotes
that motivated them.

| Directory | Origin | Contents |
|---|---|---|
| `checklist/` | `rule.json` | CPP (language), QC (QLever conventions), CQ (code quality) rules, one file per id |
| `pitfalls/` | review comments by the maintainers on this fork's PRs | PP checks, each with the quotes that motivated it |
| `review-categories/` | `rule.json` | the finding categories the fan-out runner classifies into |

Ids such as `CPP15` or `PP3` are the file names, so references between rules stay
valid. The general C++ rules these project rules instantiate live in
[cpp-idioms](https://github.com/marvin7122/cpp-idioms) and link back here.

## File format

```markdown
---
kind: rule                  # rule | guideline
severity: SHOULD            # rule only: MUST | SHOULD
language: cpp
tags: [qlever, cpp]
last_verified: YYYY-MM-DD   # when the claim and its sources were last checked
---
# CPP15: One-line title

## Statement
What to do and when. Required.

## Sources
- review: `.opencodereview/rule.json#7` Where this rule comes from, dated.
- review: `QLever PR #137` "The maintainer comment that motivated it."
- doc: `Core Guidelines F.15, F.16` Cited references.

## Notes
Fan-out trigger words, duplicate ids, anything for readers only.
```

Each source is one bullet: a kind (`doc`, `review`, `standard`, `paper`, `blog`,
`discussion`, `human`), a Markdown link or a backticked plain reference, then what
that source establishes.

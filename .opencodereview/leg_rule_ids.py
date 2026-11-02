#!/usr/bin/env python3
"""Print the rule ids of one leg's rule chunk, space-separated.

Usage: leg_rule_ids.py RULE_CHUNK_JSON [STATEMENTS_OUT_JSON]

Run on the chunk before the rules are bundled. A rule's id is the first word
of its text after the preamble that all rules of rule.json share (e.g. `QC18`
or `rule:includes`); rules without such an id (the built-in checks) have none.
Every finding must name one of these ids. With STATEMENTS_OUT_JSON, also
write {id: statement} (the rule text after its id and severity, shortened)
for the review filter's rule-fit check.
"""

import json
import os
import re
import sys

RULE_ID = re.compile(r"^(?:[A-Z]{2,4}\d+[a-z]?|rule:[a-z-]+)$")
STATEMENT_CHARS = 400


def rule_ids(rules, preamble_length):
    return list(rule_statements(rules, preamble_length))


def rule_statements(rules, preamble_length):
    """{id: statement} in the order of the rules, first occurrence of an id."""
    statements = {}
    for rule in rules:
        text = " ".join(rule["rule"][preamble_length:].split())
        first_word, _, rest = text.partition(" ")
        if not RULE_ID.match(first_word) or first_word in statements:
            continue
        rest = re.sub(r"^\[[A-Z]+\]\s*|^—\s*", "", rest)
        statements[first_word] = rest[:STATEMENT_CHARS] + ("…" if len(rest) > STATEMENT_CHARS else "")
    return statements


def preamble_length(texts):
    common = os.path.commonprefix(texts) if len(texts) > 1 else ""
    return common.rfind("\n\n") + 2 if "\n\n" in common else 0


def main():
    with open(sys.argv[1], encoding="utf-8") as f:
        rules = json.load(f)["rules"]
    # The preamble is shared by every rule of rule.json; a chunk of one rule
    # cannot show it, so take it from rule.json next to this script.
    full = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rule.json")
    texts = [r["rule"] for r in json.load(open(full, encoding="utf-8"))["rules"]] if os.path.exists(full) else []
    statements = rule_statements(rules, preamble_length(texts or [r["rule"] for r in rules]))
    if len(sys.argv) > 2:
        with open(sys.argv[2], "w", encoding="utf-8") as f:
            json.dump(statements, f, indent=1, ensure_ascii=False)
    print(" ".join(statements))


if __name__ == "__main__":
    main()

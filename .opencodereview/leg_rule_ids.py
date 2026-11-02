#!/usr/bin/env python3
"""Print the rule ids of one leg's rule chunk, space-separated.

Usage: leg_rule_ids.py RULE_CHUNK_JSON

Run on the chunk before the rules are bundled. A rule's id is the first word
of its text after the preamble that all rules of rule.json share (e.g. `QC18`
or `rule:includes`); rules without such an id (the built-in checks) have none.
The poster gives each finding one of these ids (or "unknown").
"""

import json
import os
import re
import sys

RULE_ID = re.compile(r"^(?:[A-Z]{2,4}\d+[a-z]?|rule:[a-z-]+)$")


def rule_ids(rules, preamble_length):
    ids = []
    for rule in rules:
        first_word = " ".join(rule["rule"][preamble_length:].split()).split(" ", 1)[0]
        if RULE_ID.match(first_word) and first_word not in ids:
            ids.append(first_word)
    return ids


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
    print(" ".join(rule_ids(rules, preamble_length(texts or [r["rule"] for r in rules]))))


if __name__ == "__main__":
    main()

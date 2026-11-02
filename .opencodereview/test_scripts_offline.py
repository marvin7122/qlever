#!/usr/bin/env python3
"""Checks of the review pipeline scripts without any model call.

Usage: python3 .opencodereview/test_scripts_offline.py

The link check against GitHub (cards answer 200, the rule-set commit exists)
runs only when `gh` is logged in with access to the card repositories;
everything else is offline.
"""

import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import leg_rule_ids  # noqa: E402
import rule_links  # noqa: E402

RULE_TEXT = open(os.path.join(HERE, "rule.json"), encoding="utf-8").read()
CARDS = json.load(open(os.path.join(HERE, "rule_cards.json"), encoding="utf-8"))


def test_leg_rule_ids():
    rules = json.loads(RULE_TEXT)["rules"]
    length = leg_rule_ids.preamble_length([r["rule"] for r in rules])
    ids = leg_rule_ids.rule_ids(rules, length)
    assert "QC18" in ids and "PP7" in ids and "rule:includes" in ids, ids[:10]
    assert len(ids) == len(set(ids)), "ids are unique"
    assert all(leg_rule_ids.RULE_ID.match(i) for i in ids)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump({"rules": rules[100:103]}, f)
    out = subprocess.run([sys.executable, os.path.join(HERE, "leg_rule_ids.py"), f.name],
                         check=True, capture_output=True, text=True).stdout.split()
    os.unlink(f.name)
    assert out == leg_rule_ids.rule_ids(rules[100:103], length), out


def test_every_rule_id_has_a_line_anchor():
    """(a) for the whole rule.json: every rule id resolves to the line that
    starts its own entry."""
    rules = json.loads(RULE_TEXT)["rules"]
    lines, length = rule_links.rule_lines(RULE_TEXT)
    ids = leg_rule_ids.rule_ids(rules, length)
    assert sorted(lines) == sorted(ids), set(ids) ^ set(lines)
    broken = [i for i, n in lines.items() if not rule_links.anchor_holds(RULE_TEXT, n, i, length)]
    assert not broken, f"anchors that miss their entry: {broken}"
    # A wrong line does not pass.
    some_id, some_line = next(iter(lines.items()))
    assert not rule_links.anchor_holds(RULE_TEXT, some_line + 1, some_id, length)


def test_link_check_logic():
    """The verified set, the rules without a card, and broken links, with a
    fake GitHub API."""
    lines, _ = rule_links.rule_lines(RULE_TEXT)
    broken_card = CARDS["PP7"]["url"]

    def status(path, token):
        if "/commits/" in path:
            return 200 if path.endswith("/good") else 404
        return 404 if rule_links.card_api_path(broken_card) == path else 200

    verified, no_card, broken = rule_links.check(RULE_TEXT, CARDS, "o/r", "good", "t", "c", status)
    assert "QC18" in verified and verified["QC18"]["rule_url"] == f"https://github.com/o/r/blob/good/.opencodereview/rule.json#L{lines['QC18']}"
    assert "PP2" in no_card and "rule:includes" in no_card and "PP2" not in verified
    assert {rule_id for rule_id, _ in broken} >= {"PP7"} and "PP7" not in verified
    _, _, broken_sha = rule_links.check(RULE_TEXT, CARDS, "o/r", "bad", "t", "c", status)
    assert broken_sha[0][0] == "(rule set)"


def test_links_against_github():
    """(a)-(c) for the whole rule set against GitHub, if `gh` can read the
    card repositories; reports the broken links."""
    token = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True).stdout.strip()
    sha = subprocess.run(["git", "-C", HERE, "rev-parse", "origin/master"], capture_output=True, text=True).stdout.strip()
    if not token or not sha:
        print("skipped: the link check against GitHub (no gh login or no origin/master)")
        return
    verified, no_card, broken = rule_links.check(RULE_TEXT, CARDS, "marvin7122/qlever", sha, token, token)
    print(f"links against GitHub at {sha[:9]}: {len(verified)} verified, no card: {' '.join(no_card)}")
    assert not broken, f"broken links: {broken}"


def test_review_spend_counts_rejections():
    with tempfile.TemporaryDirectory() as d:
        for leg, (usage, stats) in enumerate([
            ({"calls": 43, "input_tokens": 500000, "cache_read_tokens": 450000, "output_tokens": 4000,
              "cost_usd": 0.0095, "truncated": True, "truncated_reason": "model-call cap 200 reached"},
             {"findings": 3, "rejected_no_rule": 1, "rejected_unverified_link": 0}),
            ({"calls": 12, "input_tokens": 100000, "cache_read_tokens": 90000, "output_tokens": 1000,
              "cost_usd": 0.0020, "truncated": False},
             {"findings": 0, "rejected_no_rule": 0, "rejected_unverified_link": 2}),
        ]):
            os.makedirs(os.path.join(d, f"ocr-usage-{leg}"))
            json.dump(usage, open(os.path.join(d, f"ocr-usage-{leg}", "usage.json"), "w"))
            json.dump(stats, open(os.path.join(d, f"ocr-usage-{leg}", "rule-stats.json"), "w"))
        report = os.path.join(d, "spend.md")
        env = dict(os.environ, SELECTION_COST_USD="0.0028", OCR_REVIEW_BUDGET_USD="1", LEG_BUDGET_USD="0.33",
                   NO_CARD_RULES="PP20 rule:includes")
        subprocess.run([sys.executable, os.path.join(HERE, "review_spend.py"), d, "3", report],
                       check=True, capture_output=True, text=True, env=env)
        text = open(report).read()
    assert "$0.0143 of the $1.00 review budget" in text, text
    assert "A cap cut leg 0 short" in text and "No usage from leg 2" in text
    assert "Findings posted: 3; rejected: no valid rule 1, unverified link 2 (leg 0: 1 / 0; leg 1: 0 / 2)" in text, text
    assert "Left out of this review (no knowledge card): PP20, rule:includes." in text, text


if __name__ == "__main__":
    test_leg_rule_ids()
    test_every_rule_id_has_a_line_anchor()
    test_link_check_logic()
    test_review_spend_counts_rejections()
    test_links_against_github()
    print("checks passed")

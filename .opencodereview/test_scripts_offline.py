#!/usr/bin/env python3
"""Offline checks of the review pipeline scripts (no network, no model).

Usage: python3 .opencodereview/test_scripts_offline.py
"""

import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import leg_rule_ids  # noqa: E402


def test_leg_rule_ids():
    rules = json.load(open(os.path.join(HERE, "rule.json"), encoding="utf-8"))["rules"]
    length = leg_rule_ids.preamble_length([r["rule"] for r in rules])
    ids = leg_rule_ids.rule_ids(rules, length)
    assert "QC18" in ids and "PP7" in ids and "rule:includes" in ids, ids[:10]
    assert len(ids) == len(set(ids)), "ids are unique"
    # Every id of the full file is a well-formed id; the built-in-checks rule
    # (no id) is skipped.
    assert all(leg_rule_ids.RULE_ID.match(i) for i in ids)
    assert len(ids) < len(rules)
    # On a chunk, through the command line, the preamble comes from rule.json.
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump({"rules": rules[100:103]}, f)
    out = subprocess.run([sys.executable, os.path.join(HERE, "leg_rule_ids.py"), f.name],
                         check=True, capture_output=True, text=True).stdout.split()
    os.unlink(f.name)
    assert out == leg_rule_ids.rule_ids(rules[100:103], length), out


def test_review_spend_counts_unknown_rules():
    with tempfile.TemporaryDirectory() as d:
        for leg, (usage, stats) in enumerate([
            ({"calls": 43, "input_tokens": 500000, "cache_read_tokens": 450000, "output_tokens": 4000,
              "cost_usd": 0.0095, "truncated": True, "truncated_reason": "model-call cap 40 reached"},
             {"findings": 3, "unknown": 1}),
            ({"calls": 12, "input_tokens": 100000, "cache_read_tokens": 90000, "output_tokens": 1000,
              "cost_usd": 0.0020, "truncated": False}, {"findings": 0, "unknown": 0}),
        ]):
            os.makedirs(os.path.join(d, f"ocr-usage-{leg}"))
            json.dump(usage, open(os.path.join(d, f"ocr-usage-{leg}", "usage.json"), "w"))
            json.dump(stats, open(os.path.join(d, f"ocr-usage-{leg}", "rule-stats.json"), "w"))
        report = os.path.join(d, "spend.md")
        env = dict(os.environ, SELECTION_COST_USD="0.0028", OCR_REVIEW_BUDGET_USD="1", LEG_BUDGET_USD="0.33")
        subprocess.run([sys.executable, os.path.join(HERE, "review_spend.py"), d, "3", report],
                       check=True, capture_output=True, text=True, env=env)
        text = open(report).read()
    assert "$0.0143 of the $1.00 review budget" in text, text
    assert "A cap cut leg 0 short" in text
    assert "No usage from leg 2" in text
    assert "Findings: 3; without a rule id (posted as `rule: unknown`): 1." in text, text


if __name__ == "__main__":
    test_leg_rule_ids()
    test_review_spend_counts_unknown_rules()
    print("offline checks passed")

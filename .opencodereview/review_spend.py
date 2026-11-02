#!/usr/bin/env python3
"""Sum up the spend of one fan-out review for its summary comment.

Usage: review_spend.py USAGE_DIR NUM_LEGS REPORT_MD

USAGE_DIR holds one usage snapshot per leg (`ocr-usage-<leg>/usage.json`,
written by OCR after every model call, see OCR_USAGE_FILE). Environment:
SELECTION_COST_USD, OCR_REVIEW_BUDGET_USD, LEG_BUDGET_USD and RUN_URL.
Writes the comment to REPORT_MD: spend per stage and per leg against the
budget, and which legs a cap cut short or that left no usage (failed before
their first model call or were killed before the first snapshot).
"""

import glob
import json
import os
import re
import sys


# The poster's finding counts (rule-stats.json) and their labels.
FINDING_COUNTS = [
    ("findings", "posted"),
    ("rejected_no_rule", "rejected, no valid rule"),
    ("rejected_unverified_link", "rejected, unverified link"),
    ("rejected_invalid_citation", "rejected, invalid citation"),
    ("rejected_rule_does_not_fit", "rejected, rule does not fit"),
    ("reassigned_rule", "re-assigned to a fitting rule"),
    ("unverified", "posted unverified"),
]


def number(name):
    try:
        return float(os.environ.get(name, ""))
    except ValueError:
        return 0.0


def main():
    usage_dir, num_legs, report_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    legs = {}
    for path in glob.glob(os.path.join(usage_dir, "ocr-usage-*", "usage.json")):
        leg = int(re.search(r"ocr-usage-(\d+)", path).group(1))
        with open(path, encoding="utf-8") as f:
            legs[leg] = json.load(f)
    # Posted, rejected and changed findings per leg (rule-stats.json, written
    # by the poster).
    totals = {key: 0 for key, _ in FINDING_COUNTS}
    per_leg = {}
    # One-click suggestions (the poster's "suggestions" entry): findings with a
    # code change, how many of them are suggestions on every place, and why
    # the other places are not.
    suggestions = {"findings_with_change": 0, "one_click": 0}
    fallback = {}
    for path in glob.glob(os.path.join(usage_dir, "ocr-usage-*", "rule-stats.json")):
        leg = int(re.search(r"ocr-usage-(\d+)", path).group(1))
        with open(path, encoding="utf-8") as f:
            stats = json.load(f)
        for key, _ in FINDING_COUNTS:
            totals[key] += stats.get(key, 0)
        sugg = stats.get("suggestions") or {}
        for key in suggestions:
            suggestions[key] += sugg.get(key, 0)
        for reason, count in (sugg.get("fallback") or {}).items():
            fallback[reason] = fallback.get(reason, 0) + count
        notes = [f"{label} {stats[key]}" for key, label in FINDING_COUNTS[1:] if stats.get(key)]
        if notes:
            per_leg[leg] = ", ".join(notes)

    selection = number("SELECTION_COST_USD")
    budget = number("OCR_REVIEW_BUDGET_USD")
    leg_budget = os.environ.get("LEG_BUDGET_USD", "")
    total = selection + sum(u.get("cost_usd", 0.0) for u in legs.values())
    truncated = sorted(leg for leg, u in legs.items() if u.get("truncated"))
    missing = [leg for leg in range(num_legs) if leg not in legs]

    lines = [
        f"**OpenCodeReview spend:** ${total:.4f}"
        + (f" of the ${budget:.2f} review budget" if budget > 0 else "")
        + (f" ({num_legs} legs, at most ${float(leg_budget):.4f} each)" if leg_budget else ""),
        "",
        "| Stage / leg | Calls | Input | of which cache hits | Output | USD | Note |",
        "|---|---|---|---|---|---|---|",
        f"| 2: rule selection | 1 | | | | {selection:.4f} | |",
    ]
    for leg in range(num_legs):
        u = legs.get(leg)
        if u is None:
            lines.append(f"| 3+4: leg {leg} | | | | | | no usage recorded |")
            continue
        note = "cut: " + u.get("truncated_reason", "") if u.get("truncated") else ""
        lines.append(
            f"| 3+4: leg {leg} | {u.get('calls', 0)} | {u.get('input_tokens', 0):,} | "
            f"{u.get('cache_read_tokens', 0):,} | {u.get('output_tokens', 0):,} | "
            f"{u.get('cost_usd', 0.0):.4f} | {note} |"
        )
    def legs_text(numbers):
        return ("leg " if len(numbers) == 1 else "legs ") + ", ".join(map(str, numbers))

    lines.append("")
    if truncated:
        lines.append(f"A cap cut {legs_text(truncated)} short; the findings so far were posted, "
                     "unverified where the filter could not run.")
    else:
        lines.append("No cap cut any leg.")
    if missing:
        lines.append(f"No usage from {legs_text(missing)} (failed before the first model call, or killed).")
    lines.append(f"Findings posted: {totals['findings']}. "
                 + "; ".join(f"{label}: {totals[key]}" for key, label in FINDING_COUNTS[1:]) + ".")
    if suggestions["findings_with_change"]:
        line = (f"One-click suggestions: {suggestions['one_click']} of {suggestions['findings_with_change']} "
                "findings with a code change.")
        if fallback:
            line += " Places without one: " + "; ".join(
                f"{reason.replace('_', ' ')}: {count}" for reason, count in sorted(fallback.items())) + "."
        lines.append(line)
    if per_leg:
        lines.append("Per leg: " + "; ".join(f"leg {leg}: {text}" for leg, text in sorted(per_leg.items()))
                     + ". Rejected findings are in the legs' logs.")
    if os.environ.get("NO_CARD_RULES", "").strip():
        lines.append(f"Left out of this review (no knowledge card): {', '.join(os.environ['NO_CARD_RULES'].split())}.")
    if os.environ.get("RUN_URL"):
        lines.append(f"\n[Actions run]({os.environ['RUN_URL']}); every leg logs `[ocr] usage:` after each model call.")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()

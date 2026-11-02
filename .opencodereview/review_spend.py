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
    # Posted and rejected findings per leg (rule-stats.json, written by the
    # poster's rule gate).
    rejected = {}
    findings = no_rule = unverified = 0
    for path in glob.glob(os.path.join(usage_dir, "ocr-usage-*", "rule-stats.json")):
        leg = int(re.search(r"ocr-usage-(\d+)", path).group(1))
        with open(path, encoding="utf-8") as f:
            stats = json.load(f)
        findings += stats.get("findings", 0)
        no_rule += stats.get("rejected_no_rule", 0)
        unverified += stats.get("rejected_unverified_link", 0)
        if stats.get("rejected_no_rule", 0) or stats.get("rejected_unverified_link", 0):
            rejected[leg] = (stats.get("rejected_no_rule", 0), stats.get("rejected_unverified_link", 0))

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
    lines.append(f"Findings posted: {findings}; rejected: no valid rule {no_rule}, unverified link {unverified}"
                 + (" (" + "; ".join(f"leg {leg}: {a} / {b}" for leg, (a, b) in sorted(rejected.items())) + ")"
                    if rejected else "") + ". Rejected findings are in the legs' logs.")
    if os.environ.get("NO_CARD_RULES", "").strip():
        lines.append(f"Left out of this review (no knowledge card): {', '.join(os.environ['NO_CARD_RULES'].split())}.")
    if os.environ.get("RUN_URL"):
        lines.append(f"\n[Actions run]({os.environ['RUN_URL']}); every leg logs `[ocr] usage:` after each model call.")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()

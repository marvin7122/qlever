#!/usr/bin/env python3
"""Select the project rules that apply to a pull request, before the fan-out.

Usage: select_rules.py RULE_JSON OUT_JSON

Environment: GH_TOKEN, REPO (owner/name), PR_NUMBER, OCR_LLM_URL,
OCR_LLM_AUTH_TOKEN, OCR_LLM_MODEL, OCR_RULES_PER_LEG (default 14), and
GITHUB_OUTPUT (the step outputs `count`, `num_chunks`, `chunks`,
`selection_cost_usd` and `leg_budget_usd`). With OCR_REVIEW_BUDGET_USD and
the OCR_PRICE_*_PER_M prices, each leg gets an equal share of what the
review budget leaves after this call (`leg_budget_usd`; empty = unlimited).

Two passes, both conservative:
1. Triggers: a rule with triggers is kept only if one of its trigger regexes
   matches the diff or the new content of a changed file it applies to. This
   is the check OCR itself does per file, which the per-leg rule bundling
   switched off.
2. Relevance: one model call (thinking disabled) sees the rule ids and titles,
   the changed files and the diff, and returns the ids of the rules that could
   apply. Any failure keeps every rule of pass 1.

Writes the selected rules (same include/exclude as RULE_JSON) to OUT_JSON.
"""

import fnmatch
import json
import math
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stage1_deterministic import SCRIPTED_RULE_IDS  # noqa: E402

# Usage of the selection call, as the API reported it (empty if none ran).
SELECTION_USAGE = {}

# Diff characters sent to the selection call; the rest is listed by name only.
MAX_DIFF_CHARS = 60_000
# Characters of each rule's own text shown to the selection call.
RULE_SUMMARY_CHARS = 240


def log(message):
    print(f"[select-rules] {message}", file=sys.stderr)


def gh_api(path, accept=None):
    command = ["gh", "api", path]
    if accept:
        command += ["-H", f"Accept: {accept}"]
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def changed_files(repo, number):
    files = []
    for page in range(1, 31):
        batch = json.loads(gh_api(f"repos/{repo}/pulls/{number}/files?per_page=100&page={page}"))
        files += batch
        if len(batch) < 100:
            break
    return files


def expand_braces(glob):
    """Expand one `{a,b}` group, as in `**/*.{h,cpp}`."""
    match = re.search(r"\{([^{}]*)\}", glob)
    if not match:
        return [glob]
    return [
        expanded
        for option in match.group(1).split(",")
        for expanded in expand_braces(glob[: match.start()] + option + glob[match.end():])
    ]


def path_matches(path, glob):
    for pattern in expand_braces(glob):
        if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(path, pattern.replace("**/", "")):
            return True
    return False


def rule_summary(text, preamble_length):
    return " ".join(text[preamble_length:].split())[:RULE_SUMMARY_CHARS]


def triggers_hit(rule, texts):
    triggers = rule.get("trigger") or []
    if not triggers:
        return True
    for pattern in triggers:
        try:
            compiled = re.compile(pattern)
        except re.error:
            return True
        if any(compiled.search(text) for text in texts):
            return True
    return False


# Answers of the model endpoint that every leg would get as well (key rejected,
# account empty, rate limit): the review stops instead of falling back to all
# rules, which would start legs that are bound to fail.
ACCOUNT_ERRORS = {401, 402, 403, 429}


def stop_for_account_error(error, data, out_path):
    """Write empty outputs and the stop report for an account error."""
    detail = error.read().decode(errors="replace")[:300]
    log(f"pass 2 stopped: the model endpoint answered {error.code} ({detail})")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({**data, "rules": []}, f, indent=1)
        f.write("\n")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write("count=0\nnum_chunks=0\nchunks=[]\nselection_cost_usd=0\nleg_budget_usd=\nstopped=true\n")
    with open(os.environ["OCR_STOP_REPORT"], "w", encoding="utf-8") as f:
        f.write(f"**Review stopped at stage 2 (rule selection):** the model endpoint answered "
                f"`{error.code} {error.reason}`; no review legs were started.\n\n"
                f"```\n{detail}\n```\n\nFix the account, then trigger the review again.\n")


def select_with_model(labels, summaries, files, diff_text):
    """Return the labels (`R<index>`) of the rules the model selects."""
    url = os.environ["OCR_LLM_URL"]
    rule_lines = "\n".join(f"{label}: {summary}" for label, summary in zip(labels, summaries))
    file_lines = "\n".join(f"- {f['filename']} (+{f.get('additions', 0)}/-{f.get('deletions', 0)})" for f in files)
    prompt = (
        "You select which code review rules can apply to a pull request. "
        "A rule applies if the changed code could violate it: the rule's topic "
        "(a construct, an API, a convention, a file kind) occurs in the added "
        "or changed lines, or the change could plausibly need what the rule "
        "asks for. Be inclusive: when unsure, select the rule. Only leave out "
        "rules about topics that the diff clearly does not touch.\n\n"
        f"Rules (label: rule id, severity, summary):\n{rule_lines}\n\n"
        f"Changed files:\n{file_lines}\n\n"
        f"Diff (possibly truncated):\n{diff_text}\n\n"
        'Answer with JSON only, listing the labels: {"rule_ids": ["R0", ...]}'
    )
    body = {
        "model": os.environ["OCR_LLM_MODEL"],
        "messages": [{"role": "user", "content": prompt}],
        "thinking": {"type": "disabled"},
        "response_format": {"type": "json_object"},
        "max_tokens": 4096,
        "temperature": 0,
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {os.environ['OCR_LLM_AUTH_TOKEN']}",
        },
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        answer = json.load(response)
    usage = answer.get("usage", {})
    SELECTION_USAGE.update(usage)
    log(f"selection call model: {answer.get('model')}, usage: {json.dumps(usage)}")
    content = answer["choices"][0]["message"]["content"]
    selected = json.loads(content)["rule_ids"]
    if not isinstance(selected, list):
        raise ValueError(f"rule_ids is not a list: {content[:200]}")
    return {str(rule_id_) for rule_id_ in selected}


def price(name):
    try:
        return max(float(os.environ.get(name, "")), 0.0)
    except ValueError:
        return 0.0


def selection_cost():
    """USD of the selection call (DeepSeek reports cache hits and misses)."""
    hits = SELECTION_USAGE.get("prompt_cache_hit_tokens", 0)
    misses = SELECTION_USAGE.get("prompt_cache_miss_tokens", SELECTION_USAGE.get("prompt_tokens", 0) - hits)
    output = SELECTION_USAGE.get("completion_tokens", 0)
    return (misses * price("OCR_PRICE_INPUT_PER_M") + hits * price("OCR_PRICE_CACHE_HIT_PER_M")
            + output * price("OCR_PRICE_OUTPUT_PER_M")) / 1e6


def main():
    rule_path, out_path = sys.argv[1], sys.argv[2]
    repo, number = os.environ["REPO"], os.environ["PR_NUMBER"]
    per_leg = int(os.environ.get("OCR_RULES_PER_LEG", "14"))

    with open(rule_path, encoding="utf-8") as f:
        data = json.load(f)
    rules = data["rules"]
    preamble = os.path.commonprefix([r["rule"] for r in rules]) if len(rules) > 1 else ""
    preamble_length = preamble.rfind("\n\n") + 2 if "\n\n" in preamble else 0
    summaries = [rule_summary(r["rule"], preamble_length) for r in rules]

    head_sha = json.loads(gh_api(f"repos/{repo}/pulls/{number}"))["head"]["sha"]
    files = changed_files(repo, number)
    log(f"{len(files)} changed files, {len(rules)} rules")

    # Pass 1: triggers, per file the rule applies to (diff + new content).
    texts_by_file = {}
    for f in files:
        texts = [f.get("patch") or ""]
        if f.get("status") != "removed":
            try:
                texts.append(gh_api(f"repos/{repo}/contents/{f['filename']}?ref={head_sha}", "application/vnd.github.raw"))
            except subprocess.CalledProcessError:
                # Unknown content: keep every triggered rule for this file.
                texts.append(None)
        texts_by_file[f["filename"]] = texts
    kept = []
    scripted = 0
    # Rules whose links could not be given (no knowledge card, see
    # rule_links.py) are left out of this review.
    excluded = set(os.environ.get("OCR_EXCLUDE_RULE_IDS", "").split())
    left_out = 0
    for index, rule in enumerate(rules):
        # Stage 1 checks these rules deterministically; no model sees them.
        if summaries[index].split(" ", 1)[0] in SCRIPTED_RULE_IDS:
            scripted += 1
            continue
        if summaries[index].split(" ", 1)[0] in excluded:
            left_out += 1
            continue
        applicable = [name for name in texts_by_file if path_matches(name, rule["path"])]
        if not applicable:
            continue
        texts = [t for name in applicable for t in texts_by_file[name]]
        if None in texts or triggers_hit(rule, [t for t in texts if t]):
            kept.append(index)
    log(f"left out {left_out} rules without a knowledge card")
    log(f"pass 1: {scripted} rules left to the stage 1 script; triggers and paths kept {len(kept)} of the other {len(rules) - scripted}")

    # Pass 2: relevance, one model call; any failure keeps pass 1's rules.
    # Rules are labelled by position (`R<index>`): not every rule has an id,
    # and some ids repeat. The rule that merges OCR's built-in checks is
    # always kept.
    selected = kept
    ids = {index: f"R{index}" for index in kept}
    always = {index for index in kept if rules[index].get("merge_system_rule")}
    if kept:
        diff_text = ""
        for f in files:
            if f.get("patch"):
                diff_text += f"--- {f['filename']}\n{f['patch']}\n"
        if len(diff_text) > MAX_DIFF_CHARS:
            diff_text = diff_text[:MAX_DIFF_CHARS] + "\n[diff truncated]\n"
        try:
            chosen = select_with_model([ids[i] for i in kept], [summaries[i] for i in kept], files, diff_text)
            selected = [i for i in kept if ids[i] in chosen or i in always]
            unknown = chosen - set(ids.values())
            if unknown:
                log(f"ignoring unknown rule labels from the model: {sorted(unknown)}")

            def name(i):
                return f"{ids[i]} {summaries[i][:40]}"

            log(f"pass 2 (model): selected {len(selected)} of {len(kept)} rules: {[name(i) for i in selected]}")
            log(f"pass 2 left out: {[name(i) for i in kept if i not in selected]}")
        except Exception as error:  # noqa: BLE001 - any other failure falls back
            if isinstance(error, urllib.error.HTTPError) and error.code in ACCOUNT_ERRORS:
                stop_for_account_error(error, data, out_path)
                return
            log(f"pass 2 failed ({error!r}); keeping all {len(kept)} rules of pass 1")
            selected = kept
    else:
        log("pass 2 skipped (no rules left after pass 1)")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({**data, "rules": [rules[i] for i in selected]}, f, indent=1)
        f.write("\n")

    num_chunks = math.ceil(len(selected) / per_leg)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        cost = selection_cost()
        budget = price("OCR_REVIEW_BUDGET_USD")
        leg_budget = f"{(budget - cost) / num_chunks:.4f}" if budget > 0 and num_chunks else ""
        out.write(f"selection_cost_usd={cost:.6f}\n")
        out.write(f"leg_budget_usd={leg_budget}\n")
        log(f"selection cost ${cost:.6f}; per-leg budget {leg_budget or 'unlimited'}")
        out.write(f"count={len(selected)}\n")
        out.write(f"num_chunks={num_chunks}\n")
        out.write(f"chunks={json.dumps(list(range(num_chunks)))}\n")
    log(f"selected {len(selected)} rules -> {num_chunks} legs of at most {per_leg} rules")


if __name__ == "__main__":
    main()

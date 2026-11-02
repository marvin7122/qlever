#!/usr/bin/env python3
"""Compute and verify the two links of every rule, before any model call.

Usage: rule_links.py RULE_JSON RULE_CARDS_JSON OUT_VERIFIED_JSON REPORT_MD

Environment: REPO (owner/name of the reviewed repository), RULE_SHA (the commit
whose rule.json the review uses), GH_TOKEN (reads REPO), CARDS_TOKEN (reads the
private card repositories; may be empty), GITHUB_OUTPUT (optional).

Per rule id of RULE_JSON:
(a) the rule's permalink `https://github.com/<REPO>/blob/<RULE_SHA>/
    .opencodereview/rule.json#L<n>`, where line n starts the rule's entry and
    the entry's text has that id;
(b) its card from RULE_CARDS_JSON, which must answer 200 through the contents
    API (with CARDS_TOKEN for the private repositories);
(c) RULE_SHA must exist in REPO.
A rule without a card URL is left out of the review (listed in `no_card`). Any
other failure is a broken link: the review stops before tokens are spent.
Writes {id: {rule_url, card_url, title}} of the verified rules to
OUT_VERIFIED_JSON, the outcome to REPORT_MD, and the step outputs `ok`,
`no_card` (space-separated ids) and `broken` (count).
"""

import json
import os
import re
import subprocess
import sys

RULE_ID = re.compile(r"^(?:[A-Z]{2,4}\d+[a-z]?|rule:[a-z-]+)$")
BLOB = re.compile(r"^https://github\.com/([^/]+/[^/]+)/blob/([^/]+)/(.+)$")


def first_word(text, preamble_length):
    return " ".join(text[preamble_length:].split()).split(" ", 1)[0]


def preamble_length(texts):
    common = os.path.commonprefix(texts) if len(texts) > 1 else ""
    return common.rfind("\n\n") + 2 if "\n\n" in common else 0


def rule_lines(text):
    """{rule id: 1-based line of the `{` that starts its entry} for a rule.json
    text, plus the preamble length. The k-th `"rule":` line of the file belongs
    to the k-th entry of `rules`; its entry starts at the closest `{` above."""
    rules = json.loads(text)["rules"]
    length = preamble_length([r["rule"] for r in rules])
    lines = text.split("\n")
    rule_value_lines = [i for i, line in enumerate(lines) if re.match(r'^\s*"rule":\s*"', line)]
    if len(rule_value_lines) != len(rules):
        raise ValueError(f"{len(rule_value_lines)} \"rule\" lines for {len(rules)} rules; is rule.json one key per line?")
    result = {}
    for rule, value_line in zip(rules, rule_value_lines):
        rule_id = first_word(rule["rule"], length)
        if not RULE_ID.match(rule_id) or rule_id in result:
            continue
        start = value_line
        while start > 0 and not lines[start].strip().startswith("{"):
            start -= 1
        result[rule_id] = start + 1
    return result, length


def anchor_holds(text, line, rule_id, length):
    """(a): the entry that starts at `line` has the rule text of `rule_id`."""
    lines = text.split("\n")
    if not 1 <= line <= len(lines) or not lines[line - 1].strip().startswith("{"):
        return False
    for candidate in lines[line:]:
        match = re.match(r'^\s*"rule":\s*(".*")\s*,?\s*$', candidate)
        if match:
            return first_word(json.loads(match.group(1)), length) == rule_id
        if candidate.strip().startswith("{"):
            return False
    return False


def api_status(path, token):
    """HTTP status of a GitHub API GET (gh exits 1 on 4xx/5xx)."""
    env = dict(os.environ, GH_TOKEN=token) if token else dict(os.environ)
    out = subprocess.run(["gh", "api", "-i", "--silent", path], capture_output=True, text=True, env=env)
    match = re.search(r"HTTP/[\d.]+ (\d{3})", out.stdout + out.stderr)
    return int(match.group(1)) if match else 0


def card_api_path(url):
    match = BLOB.match(url)
    if not match:
        return None
    repo, ref, path = match.groups()
    return f"repos/{repo}/contents/{path}?ref={ref}"


def check(rule_text, cards, repo, sha, gh_token, cards_token, status=api_status):
    lines, length = rule_lines(rule_text)
    verified, no_card, broken = {}, [], []
    sha_ok = status(f"repos/{repo}/commits/{sha}", gh_token) == 200
    if not sha_ok:
        broken.append(("(rule set)", f"commit {sha} not found in {repo}"))
    card_status = {}
    for rule_id, line in sorted(lines.items(), key=lambda kv: kv[1]):
        rule_url = f"https://github.com/{repo}/blob/{sha}/.opencodereview/rule.json#L{line}"
        if not anchor_holds(rule_text, line, rule_id, length):
            broken.append((rule_id, f"{rule_url} does not start the entry of {rule_id}"))
            continue
        card = cards.get(rule_id) or {}
        if not card.get("url"):
            no_card.append(rule_id)
            continue
        api_path = card_api_path(card["url"])
        if api_path is None:
            broken.append((rule_id, f"card {card['url']} is not a GitHub blob URL"))
            continue
        if api_path not in card_status:
            card_status[api_path] = status(api_path, cards_token or gh_token)
        if card_status[api_path] != 200:
            broken.append((rule_id, f"card {card['url']} answered {card_status[api_path] or 'no response'}"))
            continue
        if sha_ok:
            verified[rule_id] = {"rule_url": rule_url, "card_url": card["url"], "title": card.get("title", "")}
    return verified, no_card, broken


def main():
    rule_path, cards_path, out_path, report_path = sys.argv[1:5]
    repo, sha = os.environ["REPO"], os.environ["RULE_SHA"]
    cards_token = os.environ.get("CARDS_TOKEN", "")
    rule_text = open(rule_path, encoding="utf-8").read()
    cards = json.load(open(cards_path, encoding="utf-8"))
    verified, no_card, broken = check(rule_text, cards, repo, sha, os.environ.get("GH_TOKEN", ""), cards_token)
    if not cards_token and any("answered" in reason for _, reason in broken):
        broken.append(("(token)", "no token reads the private card repositories marvin7122/cpp-idioms and "
                       "marvin7122/ocr-runners: install the OCR fan-out app on them or add the secret "
                       "OCR_CARDS_READ_TOKEN (fine-grained, contents: read)"))
    json.dump(verified, open(out_path, "w", encoding="utf-8"), indent=1)
    lines = []
    if broken:
        lines += [f"**Review stopped before the model stages: {len(broken)} broken rule link(s)**; no model was called.", ""]
        lines += [f"- `{rule_id}`: {reason}" for rule_id, reason in broken]
    else:
        lines.append(f"Rule links verified: {len(verified)} rules link their entry at `{sha[:9]}` and their card.")
    if no_card:
        lines += ["", f"Left out of this review (no knowledge card): {', '.join(no_card)}."]
    open(report_path, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    print("\n".join(lines))
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
            out.write(f"ok={'false' if broken else 'true'}\n")
            out.write(f"no_card={' '.join(no_card)}\n")
            out.write(f"broken={len(broken)}\n")


if __name__ == "__main__":
    main()

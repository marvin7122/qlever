#!/usr/bin/env python3
"""Act on the PR owner's replies to OpenCodeReview findings.

The owner reacts to finding comments or replies in their threads (the legend
under every finding lists the reactions and keywords), then posts a signal:

  /apply-replies  (PR comment)    handle every finding thread with an owner
                                  signal that was not handled since then
  /approve        (thread reply)  apply the proposal of this thread
  /reject         (thread reply)  drop the proposal of this thread
  /approve-all    (PR comment)    apply every pending proposal

What a thread's owner signal means (the latest owner reply wins over
reactions; threads without one are left alone):
  👍 ❤️, a free-text reply, `/improve <text>`  implement (guided by the text)
  😕 confusing, 😄 too long, `/not-actionable`  rewrite the finding: a TL;DR,
                                  a concrete code proposal, at most three
                                  bullets, the original collapsed below
  👎 `/wrong` 🎉 `/skip <reason>`               no code change, resolve
  `/duplicate <link>`, 👀                       duplicate: resolve with a link to
                                  the original and fix only the original (👀
                                  without a link: the one finding anchored on
                                  overlapping lines; if that is ambiguous, ask)
  🚀                                            already fixed: resolve

Code changes are split by kind. A mechanical change (rule listed in
apply_kinds.json, or a free-text instruction the model classifies as
mechanical, and an edit that passes `is_mechanical_edit`) is committed to the
PR branch directly. Everything else becomes a proposal: a commit on the side
branch `apply/<pr>-<thread>` and a reply with the change (a GitHub suggestion
if it replaces exactly the anchored lines), applied only on `/approve`.
A rewrite's reply records the original finding and the rewrite in its marker,
so the feedback collection (marvin7122/ocr-runners) can learn a better
comment format per rule from them.

The model backend is configurable: OCR_APPLY_LLM_URL / _MODEL / _AUTH_TOKEN,
falling back to the review fan-out's OCR_LLM_URL / _MODEL / _AUTH_TOKEN. The
edit harness is chosen by OCR_APPLY_AGENT (see HARNESSES). The run stops
before its first model call if the account cannot pay OCR_APPLY_BUDGET_USD
(stage0_balance.py), and never spends more than that budget.

Usage: apply_replies.py --mode replies|decisions|approve-all --repo-dir <checkout>
Environment: GH_TOKEN, REPO (owner/name), PR_NUMBER, optional OCR_APPLY_OWNER,
OCR_APPLY_ALLOWED_REPO (default marvin7122/qlever), OCR_APPLY_FINDING_AUTHORS,
CLANG_FORMAT, the prices OCR_PRICE_INPUT_PER_M / _CACHE_HIT_PER_M / _OUTPUT_PER_M.
"""
import argparse
import dataclasses
import difflib
import enum
import json
import os
import re
import subprocess
import sys
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from stage0_balance import check_balance  # noqa: E402

# The marker every reply of this flow carries; its JSON records the state.
APPLY_MARKER = re.compile(r"<!-- ocr-apply (\{.*?\}) -->")
FINDING_MARKER = re.compile(r"<!-- ocr-\d")
META_MARKER = re.compile(r"<!-- ocr-meta (\{.*?\}) -->")
# Lines of the file shown to the model around the anchored lines.
EXCERPT_CONTEXT = 60
# Characters of one bullet of a rewrite.
BULLET_CHARS = 220


def log(message):
    print(f"[apply] {message}", file=sys.stderr, flush=True)


# _____________________________________________________________________________
# What the owner asked for.

class Action(enum.Enum):
    IMPLEMENT = "implement"
    REWRITE = "rewrite"
    ACKNOWLEDGE = "acknowledge"
    DUPLICATE = "duplicate"
    RESOLVE = "resolve"
    APPROVE = "approve"
    REJECT = "reject"


class Kind(enum.Enum):
    MECHANICAL = "mechanical"
    SIGNOFF = "signoff"


@dataclasses.dataclass(frozen=True)
class Signal:
    action: Action
    text: str  # the owner's instruction or reason ("" for reactions)
    what: str  # the reaction or keyword, for the reply
    at: str  # ISO time of the signal


# Reactions in the order they decide a thread when there is no owner reply.
REACTION_ACTIONS = [
    ("ROCKET", Action.RESOLVE, "🚀 fixed"),
    ("THUMBS_DOWN", Action.ACKNOWLEDGE, "👎 wrong"),
    ("THUMBS_UP", Action.IMPLEMENT, "👍 actionable"),
    ("HEART", Action.IMPLEMENT, "❤️ valuable"),
    ("EYES", Action.DUPLICATE, "👀 duplicate"),
    ("CONFUSED", Action.REWRITE, "😕 confusing"),
    ("LAUGH", Action.REWRITE, "😄 too long"),
    ("HOORAY", Action.ACKNOWLEDGE, "🎉 improvement potential"),
]

KEYWORD_ACTIONS = {
    "/approve": Action.APPROVE,
    "/reject": Action.REJECT,
    "/improve": Action.IMPLEMENT,
    "/not-actionable": Action.REWRITE,
    "/wrong": Action.ACKNOWLEDGE,
    "/skip": Action.ACKNOWLEDGE,
    "/duplicate": Action.DUPLICATE,
}
# Keywords that are PR comments, not thread replies; ignored inside threads.
PR_KEYWORDS = {"/apply-replies", "/approve-all"}

# What each kind of rewrite focuses on, by the reaction or keyword that asked
# for it, and the bullets it shows (at most three).
REWRITE_FOCUS = {
    "😕 confusing": ("The owner found the finding confusing. Rewrite it so it is readable and "
                     "actionable: say plainly what is wrong and what to do.", ("why", "where", "risk")),
    "😄 too long": ("The owner found the finding too long to read. Rewrite it as a short TL;DR "
                    "with a concrete proposal; drop everything that is not needed to act.", ("why", "where", "risk")),
    "/not-actionable": ("The owner found the finding not actionable as written. Rewrite it as a concrete, "
                        "executable proposal: the exact code change, the file and lines, and how to verify it "
                        "(which test to add or run).", ("where", "verify", "why")),
}


def reply_signal(body, at):
    """The signal of one owner reply, or None if it carries none."""
    text = body.strip()
    if not text:
        return None
    first = text.split(None, 1)[0]
    if first in PR_KEYWORDS:
        return None
    if first in KEYWORD_ACTIONS:
        return Signal(KEYWORD_ACTIONS[first], text[len(first):].strip(), first, at)
    if first.startswith("/"):
        return None  # an unknown command is not an instruction
    return Signal(Action.IMPLEMENT, text, "reply", at)


def owner_signal(thread, owner):
    """The owner's signal on a finding thread: the latest owner reply with a
    signal, else the reaction that decides first in REACTION_ACTIONS, else
    None."""
    replies = [c for c in thread["comments"][1:] if c["author"] == owner and not APPLY_MARKER.search(c["body"])]
    for comment in sorted(replies, key=lambda c: c["createdAt"], reverse=True):
        signal = reply_signal(comment["body"], comment["createdAt"])
        if signal is not None:
            return signal
    owned = {r["content"]: r["createdAt"] for r in thread["comments"][0]["reactions"] if r["user"] == owner}
    for content, action, what in REACTION_ACTIONS:
        if content in owned:
            return Signal(action, "", what, owned[content])
    return None


def last_apply_state(thread):
    """(state dict, time) of the latest reply of this flow, or (None, "")."""
    for comment in reversed(thread["comments"]):
        match = APPLY_MARKER.search(comment["body"])
        if match:
            return json.loads(match.group(1)), comment["createdAt"]
    return None, ""


# _____________________________________________________________________________
# Which changes may be applied without sign-off.

def load_kinds(path=os.path.join(SCRIPT_DIR, "apply_kinds.json")):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def rule_kind(rule_id, kinds):
    """The kind from the table alone: MECHANICAL, SIGNOFF, or None (unknown)."""
    if rule_id in kinds["signoff"]:
        return Kind.SIGNOFF
    if rule_id in kinds["mechanical"] or any(rule_id.startswith(p) for p in kinds["mechanical_prefixes"]):
        return Kind.MECHANICAL
    return None


def decide_kind(rule_id, kinds, has_text, model_kind):
    """Mechanical only if the table says so for a reaction, or, for a
    free-text instruction, if the model says so and the table does not
    require sign-off. When unsure: sign-off."""
    table = rule_kind(rule_id, kinds)
    if has_text:
        return Kind.MECHANICAL if model_kind == "mechanical" and table is not Kind.SIGNOFF else Kind.SIGNOFF
    return Kind.MECHANICAL if table is Kind.MECHANICAL else Kind.SIGNOFF


_COMMENT = re.compile(r"//[^\n]*|/\*.*?\*/", re.S)
_STRING = re.compile(r'"(?:[^"\\\n]|\\.)*"')


def _code_only(text, blank_strings):
    text = _COMMENT.sub("", text)
    if blank_strings:
        text = _STRING.sub('""', text)
    return "".join(text.split())


def is_mechanical_edit(path, old, new):
    """True if the edit cannot change behavior: only comments and whitespace
    change, or only `#include` lines, or (in test files) only string
    literals such as assertion messages."""
    if _code_only(old, False) == _code_only(new, False):
        return True

    def non_include(text):
        return [l for l in text.splitlines() if l.strip() and not l.lstrip().startswith("#include")]
    if non_include(old) == non_include(new):
        return True
    is_test = path.startswith("test/") or "/test/" in path
    return is_test and _code_only(old, True) == _code_only(new, True)


# _____________________________________________________________________________
# GitHub.

def gh(*args):
    result = subprocess.run(["gh", *args], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args[:3])} failed: {result.stderr.strip()[:500]}")
    return result.stdout


THREADS_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 50, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id isResolved path line startLine
          comments(first: 50) {
            nodes {
              databaseId url body createdAt author { login }
              reactions(first: 50) { nodes { content createdAt user { login } } }
            }
          }
        }
      }
    }
  }
}"""


def fetch_threads(repo, number):
    """The PR's review threads, flattened (author and user are logins)."""
    owner, name = repo.split("/")
    threads, after = [], None
    while True:
        args = ["api", "graphql", "-f", f"query={THREADS_QUERY}", "-F", f"owner={owner}",
                "-F", f"name={name}", "-F", f"number={number}"]
        if after:
            args += ["-F", f"after={after}"]
        page = json.loads(gh(*args))["data"]["repository"]["pullRequest"]["reviewThreads"]
        for node in page["nodes"]:
            threads.append({
                "id": node["id"], "isResolved": node["isResolved"], "path": node["path"],
                "line": node["line"], "startLine": node["startLine"],
                "comments": [{
                    "id": c["databaseId"], "url": c["url"], "body": c["body"], "createdAt": c["createdAt"],
                    "author": (c["author"] or {}).get("login", ""),
                    "reactions": [{"content": r["content"], "createdAt": r["createdAt"],
                                   "user": (r["user"] or {}).get("login", "")}
                                  for r in c["reactions"]["nodes"]],
                } for c in node["comments"]["nodes"]],
            })
        if not page["pageInfo"]["hasNextPage"]:
            return threads
        after = page["pageInfo"]["endCursor"]


def is_finding_thread(thread, finding_authors):
    first = thread["comments"][0] if thread["comments"] else None
    return bool(first and FINDING_MARKER.search(first["body"]) and first["author"] in finding_authors)


def finding_rule(thread):
    match = META_MARKER.search(thread["comments"][0]["body"])
    rule = json.loads(match.group(1)).get("rule") if match else None
    return rule or "unknown"


def finding_text(body):
    """The finding's own text: without markers, badges, legend and excerpts."""
    body = re.sub(r"<!--.*?-->", "", body, flags=re.S)
    body = re.sub(r"<details>.*?</details>", "", body, flags=re.S)
    lines = [l for l in body.splitlines()
             if not l.startswith(("![", "React:", "Rule: ")) and l.strip()]
    return "\n".join(lines).strip()


# _____________________________________________________________________________
# The tasks of one run.

@dataclasses.dataclass
class Task:
    thread: dict
    action: Action
    signal: Signal
    rule: str
    proposal: dict | None = None  # the pending proposal, for APPROVE/REJECT
    original: dict | None = None  # the original's thread, for DUPLICATE
    problem: str = ""  # why the original of a DUPLICATE is unknown


# A link to a finding comment: its URL or `#discussion_r<id>`.
DISCUSSION_LINK = re.compile(
    r"^(?:https://github\.com/(?P<repo>[\w.-]+/[\w.-]+)/pull/(?P<pr>\d+)[^#\s]*)?#?discussion_r(?P<id>\d+)$")


def parse_duplicate_link(text):
    """(repo or None, PR number or None, comment id) of the link `text`
    starts with, or None."""
    words = text.split()
    match = DISCUSSION_LINK.match(words[0]) if words else None
    if not match:
        return None
    return match["repo"], int(match["pr"]) if match["pr"] else None, int(match["id"])


def resolve_duplicate_link(text, thread, threads, finding_authors, repo, number):
    """(the original's thread, "") for a valid link, or (None, why not)."""
    parsed = parse_duplicate_link(text)
    if parsed is None:
        return None, "the reply has no link to the original finding (its URL or `#discussion_r<id>`)"
    link_repo, link_pr, comment_id = parsed
    if link_repo is not None and (link_repo != repo or link_pr != number):
        return None, "the link points to another pull request"
    for candidate in threads:
        if candidate["comments"] and candidate["comments"][0]["id"] == comment_id:
            if candidate is thread:
                return None, "the link points to this finding itself"
            if not is_finding_thread(candidate, finding_authors):
                return None, "the linked comment is not an OpenCodeReview finding"
            return candidate, ""
    return None, "the linked comment is not a finding of this pull request"


def overlaps(a, b):
    """True if two threads are anchored on overlapping lines of one file."""
    if a["path"] != b["path"] or not a["line"] or not b["line"]:
        return False
    return (a["startLine"] or a["line"]) <= b["line"] and (b["startLine"] or b["line"]) <= a["line"]


def eyes_original(thread, threads, owner, finding_authors):
    """The original of a finding marked 👀 without a link: the one unmarked
    finding on overlapping lines of the same file; with none unmarked, the
    earliest 👀-marked one. (None, why) if there is none, or if it is
    ambiguous (several unmarked, or this finding is the earliest)."""
    def marked(t):
        return any(r["content"] == "EYES" and r["user"] == owner for r in t["comments"][0]["reactions"])
    candidates = [t for t in threads if t is not thread and is_finding_thread(t, finding_authors)
                  and overlaps(t, thread)]
    unmarked = [t for t in candidates if not marked(t)]
    ask = " Reply `/duplicate <link>` to name the original."
    if len(unmarked) == 1:
        return unmarked[0], ""
    if len(unmarked) > 1:
        links = ", ".join(t["comments"][0]["url"] for t in unmarked)
        return None, f"several findings on overlapping lines could be the original ({links}).{ask}"
    if not candidates:
        return None, f"no other finding is anchored on overlapping lines of this file.{ask}"
    earliest = min(candidates + [thread], key=lambda t: t["comments"][0]["createdAt"])
    if earliest is thread:
        return None, f"this finding is the earliest of the 👀-marked ones on these lines.{ask}"
    return earliest, ""


def plan_tasks(threads, owner, mode, finding_authors, repo=None, number=None):
    """One task per finding thread that needs one in this mode. A duplicate
    gets its original resolved; an original that has no task of its own and
    is not handled yet is implemented."""
    tasks = []
    for thread in threads:
        if not is_finding_thread(thread, finding_authors):
            continue
        state, state_at = last_apply_state(thread)
        pending = state if state and state.get("state") == "proposed" else None
        signal = owner_signal(thread, owner)
        if mode == "approve-all":
            if pending:
                tasks.append(Task(thread, Action.APPROVE, Signal(Action.APPROVE, "", "/approve-all", ""),
                                  finding_rule(thread), pending))
            continue
        if signal is None:
            continue
        if signal.action in (Action.APPROVE, Action.REJECT):
            if pending and signal.at > state_at:
                tasks.append(Task(thread, signal.action, signal, finding_rule(thread), pending))
            continue
        if mode == "decisions" or thread["isResolved"]:
            continue
        if state is not None and state_at >= signal.at:
            continue  # handled since the owner's last signal
        tasks.append(Task(thread, signal.action, signal, finding_rule(thread)))
    for task in [t for t in tasks if t.action is Action.DUPLICATE]:
        if task.signal.what == "/duplicate":
            task.original, task.problem = resolve_duplicate_link(
                task.signal.text, task.thread, threads, finding_authors, repo, number)
        else:
            task.original, task.problem = eyes_original(task.thread, threads, owner, finding_authors)
        original = task.original
        if original is None or original["isResolved"] or any(t.thread is original for t in tasks):
            continue
        if owner_signal(original, owner) is None and last_apply_state(original)[0] is None:
            tasks.append(Task(original, Action.IMPLEMENT,
                              Signal(Action.IMPLEMENT, "", "original of a duplicate", task.signal.at),
                              finding_rule(original)))
    return tasks


# _____________________________________________________________________________
# The model.

class Spend:
    """The run's model spend; stops before a call that the budget cannot pay."""

    def __init__(self, budget):
        self.budget = budget
        self.usd = 0.0
        self.calls = 0

    @staticmethod
    def _price(name):
        try:
            return max(float(os.environ.get(name, "")), 0.0)
        except ValueError:
            return 0.0

    def add(self, usage):
        hits = usage.get("prompt_cache_hit_tokens", 0)
        misses = usage.get("prompt_cache_miss_tokens", usage.get("prompt_tokens", 0) - hits)
        output = usage.get("completion_tokens", 0)
        self.usd += (misses * self._price("OCR_PRICE_INPUT_PER_M") + hits * self._price("OCR_PRICE_CACHE_HIT_PER_M")
                     + output * self._price("OCR_PRICE_OUTPUT_PER_M")) / 1e6
        self.calls += 1
        log(f"usage: call {self.calls}, {json.dumps(usage)}, total ${self.usd:.4f} of ${self.budget:.2f}")

    def exhausted(self):
        return self.budget > 0 and self.usd >= self.budget * 0.98


class BudgetExhausted(Exception):
    pass


def backend():
    """(url, model, token) of the model: OCR_APPLY_LLM_* over OCR_LLM_*."""
    def pick(name):
        return os.environ.get(f"OCR_APPLY_LLM_{name}") or os.environ.get(f"OCR_LLM_{name}", "")
    return pick("URL"), pick("MODEL"), pick("AUTH_TOKEN")


def chat(messages, spend):
    if spend.exhausted():
        raise BudgetExhausted()
    url, model, token = backend()
    body = {"model": model, "messages": messages, "response_format": {"type": "json_object"},
            "max_tokens": 8192, "temperature": 0}
    if (os.environ.get("OCR_APPLY_THINKING") or "disabled") == "disabled":
        body["thinking"] = {"type": "disabled"}
    request = urllib.request.Request(url, data=json.dumps(body).encode(), headers={
        "Content-Type": "application/json", "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=600) as response:
        answer = json.load(response)
    spend.add(answer.get("usage", {}))
    return json.loads(answer["choices"][0]["message"]["content"])


EDIT_PROMPT = """You fix one code review finding in a C++ repository by editing one file.

File: {path} (the lines shown are numbered; the finding is anchored at lines {start}-{end})
```
{excerpt}
```

Rule {rule}: {statement}

Finding:
{finding}

{instruction}

Answer with JSON only:
{{"edits": [{{"old": "<exact text from the file, unique in it>", "new": "<replacement>"}}],
  "kind": "mechanical" or "behavior",
  "summary": "<one sentence: what the change does>"}}
{edit_rules}"""

REWRITE_PROMPT = """You rewrite one code review finding on a C++ repository so the reader can act on it at once.

File: {path} (the lines shown are numbered; the finding is anchored at lines {start}-{end})
```
{excerpt}
```

Rule {rule}: {statement}

Finding as posted:
{finding}

{focus}

Answer with JSON only:
{{"tldr": "<one sentence: what is wrong and what to do>",
  "edits": [{{"old": "<exact text from the file, unique in it>", "new": "<replacement>"}}],
  "kind": "mechanical" or "behavior",
  "summary": "<one sentence: what the change does>",
  "why": "<one short sentence>",
  "where": ["{path}:<line>", ...],
  "verify": "<which test to add or run, one short sentence>",
  "risk": "<one short sentence>"}}
The edits are the concrete code proposal for this file. {edit_rules}"""

EDIT_RULES = ("`old` must be copied exactly (without the line numbers) and occur exactly once in the file. Keep the "
              "edit minimal; change nothing the finding does not ask for. `kind` is \"mechanical\" only if the change "
              "cannot change behavior (comments, docstrings, test assertion messages, includes, formatting); otherwise "
              "\"behavior\". If no change is needed, give no edits and say why in `summary`.")


@dataclasses.dataclass
class Edit:
    path: str
    old: str
    new: str


@dataclasses.dataclass
class EditResult:
    edits: list
    model_kind: str
    summary: str
    extra: dict  # the other fields of the answer (the rewrite's tldr, why, ...)


def numbered_excerpt(content, start, end, context=EXCERPT_CONTEXT):
    lines = content.splitlines()
    first, last = max(1, start - context), min(len(lines), end + context)
    return "\n".join(f"{n:5d}  {lines[n - 1]}" for n in range(first, last + 1))


def anchored_range(thread):
    end = thread["line"] or 1
    return (thread["startLine"] or end), end


def request_edits(prompt, path, content, spend):
    """Ask the model; one retry with the error if an `old` text is not unique
    in the file."""
    messages = [{"role": "user", "content": prompt}]
    for _ in range(2):
        answer = chat(messages, spend)
        edits = [Edit(path, e["old"], e["new"]) for e in answer.get("edits") or []]
        problems = [e.old[:80] for e in edits if content.count(e.old) != 1]
        if not problems:
            extra = {k: v for k, v in answer.items() if k not in ("edits", "kind", "summary")}
            return EditResult(edits, answer.get("kind", "behavior"), answer.get("summary", ""), extra)
        messages += [{"role": "assistant", "content": json.dumps(answer)},
                     {"role": "user", "content": "These `old` texts do not occur exactly once in the file: "
                      + json.dumps(problems) + ". Answer again with exact, unique `old` texts."}]
    raise RuntimeError(f"the edit does not match the file: {problems}")


def search_replace_harness(task, content, statement, spend):
    """Exact search/replace edits of the finding's file, for IMPLEMENT."""
    thread = task.thread
    start, end = anchored_range(thread)
    instruction = (f"The repository owner asks: {task.signal.text}" if task.signal.text
                   else "The repository owner agrees with the finding; fix it as the finding suggests.")
    prompt = EDIT_PROMPT.format(
        path=thread["path"], start=start, end=end, excerpt=numbered_excerpt(content, start, end),
        rule=task.rule, statement=statement or "(no statement)",
        finding=finding_text(thread["comments"][0]["body"]), instruction=instruction, edit_rules=EDIT_RULES)
    return request_edits(prompt, thread["path"], content, spend)


def search_replace_rewrite(task, content, statement, spend):
    """The rewrite of a finding plus its code proposal as exact edits, for REWRITE."""
    thread = task.thread
    start, end = anchored_range(thread)
    focus, _ = REWRITE_FOCUS[task.signal.what]
    prompt = REWRITE_PROMPT.format(
        path=thread["path"], start=start, end=end, excerpt=numbered_excerpt(content, start, end),
        rule=task.rule, statement=statement or "(no statement)",
        finding=finding_text(thread["comments"][0]["body"]), focus=focus, edit_rules=EDIT_RULES)
    return request_edits(prompt, thread["path"], content, spend)


# The harnesses, chosen by OCR_APPLY_AGENT: for each, the function for
# IMPLEMENT and the one for REWRITE. Each takes (task, file content, rule
# statement, spend) and returns an EditResult.
HARNESSES = {"search-replace": (search_replace_harness, search_replace_rewrite)}


def harness():
    # An unset repository variable reaches the script as an empty string.
    name = os.environ.get("OCR_APPLY_AGENT") or "search-replace"
    if name not in HARNESSES:
        raise SystemExit(f"OCR_APPLY_AGENT={name!r} is not one of {sorted(HARNESSES)}")
    return HARNESSES[name]


# _____________________________________________________________________________
# Edits and their rendering.

def apply_edits(content, edits):
    for edit in edits:
        if content.count(edit.old) != 1:
            raise RuntimeError(f"`{edit.old[:60]}` does not occur exactly once")
        content = content.replace(edit.old, edit.new, 1)
    return content


def changed_lines(old_content, new_content):
    """1-based line numbers of new_content that differ from old_content."""
    lines = []
    matcher = difflib.SequenceMatcher(a=old_content.splitlines(), b=new_content.splitlines(), autojunk=False)
    for tag, _, _, j1, j2 in matcher.get_opcodes():
        if tag in ("replace", "insert"):
            lines += range(j1 + 1, j2 + 1)
    return lines


def clang_format_lines(repo_dir, path, lines):
    if not lines or not path.endswith((".h", ".hpp", ".cpp", ".cc", ".cxx")):
        return
    clang_format = os.environ.get("CLANG_FORMAT", "clang-format-18")
    ranges = [f"--lines={n}:{n}" for n in sorted(set(lines))]
    subprocess.run([clang_format, "-i", "--style=file", *ranges, path], cwd=repo_dir, check=True)


def unified_diff(path, old_content, new_content):
    return "".join(difflib.unified_diff(old_content.splitlines(True), new_content.splitlines(True),
                                        f"a/{path}", f"b/{path}", n=3))


def suggestion_block(thread, old_content, new_content):
    """The replacement of exactly the anchored lines as a GitHub suggestion,
    or None if the change reaches outside them."""
    end = thread["line"]
    if not end:
        return None
    start = thread["startLine"] or end
    changed = changed_lines(old_content, new_content)
    old_lines, new_lines = old_content.splitlines(), new_content.splitlines()
    delta = len(new_lines) - len(old_lines)
    if not changed or min(changed) < start or max(changed) > end + max(delta, 0):
        return None
    if old_lines[:start - 1] != new_lines[:start - 1] or old_lines[end:] != new_lines[end + delta:]:
        return None
    replacement = "\n".join(new_lines[start - 1:end + delta])
    return f"```suggestion\n{replacement}\n```"


def code_proposal(thread, old_content, new_content):
    """The suggestion block if the change fits the anchored lines, and the
    diff in a collapsible block."""
    suggestion = suggestion_block(thread, old_content, new_content)
    diff = unified_diff(thread["path"], old_content, new_content)
    return ((f"{suggestion}\n\n" if suggestion else "")
            + f"<details><summary>Diff ({thread['path']})</summary>\n\n```diff\n{diff}```\n</details>")


def where_links(where, repo, sha, repo_dir):
    """`path:line` or `path:a-b` entries as permalinks at `sha`; entries whose
    file or lines do not exist in the checkout are dropped."""
    links = []
    for entry in where or []:
        match = re.fullmatch(r"\s*([^:\s]+):(\d+)(?:-(\d+))?\s*", str(entry))
        if not match:
            continue
        path, first, last = match.group(1), int(match.group(2)), int(match.group(3) or match.group(2))
        try:
            with open(os.path.join(repo_dir, path), encoding="utf-8") as f:
                count = len(f.read().splitlines())
        except OSError:
            continue
        if not 1 <= first <= last <= count:
            continue
        anchor = f"L{first}" + (f"-L{last}" if last != first else "")
        links.append(f"[{path}:{first}{'-' + str(last) if last != first else ''}]"
                     f"(https://github.com/{repo}/blob/{sha}/{path}#{anchor})")
    return links


def short(text, limit=BULLET_CHARS):
    text = " ".join(str(text or "").split())
    return text if len(text) <= limit else text[:limit - 1] + "…"


def render_rewrite(rewrite, bullets, code, status, original):
    """A rewritten finding: TL;DR, the code proposal, at most three bullets,
    the status line, and the original collapsed."""
    labels = {"why": "Why", "where": "Where", "verify": "Verify", "risk": "Risk"}
    lines = [f"**TL;DR:** {short(rewrite.get('tldr'), 300)}", ""]
    if code:
        lines += [code, ""]
    for key in bullets[:3]:
        value = rewrite.get(key)
        if value:
            lines.append(f"- **{labels[key]}:** {value if key == 'where' else short(value)}")
    lines += ["", status, "", "<details><summary>Original finding</summary>", "", original, "", "</details>"]
    return "\n".join(lines)


# _____________________________________________________________________________
# Replies.

def marker(state):
    return f"<!-- ocr-apply {json.dumps(state, sort_keys=True, ensure_ascii=False)} -->"


def reply(repo, number, thread, text, state):
    gh("api", f"repos/{repo}/pulls/{number}/comments/{thread['comments'][0]['id']}/replies",
       "-f", f"body={marker(state)}\n{text}")


def resolve(thread):
    gh("api", "graphql", "-f",
       "query=mutation($id: ID!) { resolveReviewThread(input: {threadId: $id}) { thread { id } } }",
       "-F", f"id={thread['id']}")


def git(repo_dir, *args):
    result = subprocess.run(["git", *args], cwd=repo_dir, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args[:2])} failed: {result.stderr.strip()[:400]}")
    return result.stdout.strip()


# _____________________________________________________________________________
# The run.

@dataclasses.dataclass
class Applied:
    """A commit made on the PR branch in this run, and how to report it once
    it is pushed: `body(sha)` is the reply, `state` its marker."""
    task: Task
    sha: str
    body: object
    state: dict


class Run:
    def __init__(self, args):
        self.repo = os.environ["REPO"]
        self.number = int(os.environ["PR_NUMBER"])
        self.repo_dir = args.repo_dir
        self.mode = args.mode
        self.kinds = load_kinds()
        self.statements = rule_statements()
        self.spend = Spend(float(os.environ.get("OCR_APPLY_BUDGET_USD") or 2.0))
        self.results = []  # (outcome, thread url, rule, detail)
        self.applied = []
        pr = json.loads(gh("api", f"repos/{self.repo}/pulls/{self.number}"))
        self.pr = pr
        self.owner = os.environ.get("OCR_APPLY_OWNER") or pr["user"]["login"]
        authors = os.environ.get("OCR_APPLY_FINDING_AUTHORS", "github-actions[bot],github-actions")
        self.finding_authors = {a.strip() for a in authors.split(",") if a.strip()}
        self.head_ref = pr["head"]["ref"]
        self.head_sha = pr["head"]["sha"]

    def check_target(self):
        allowed = os.environ.get("OCR_APPLY_ALLOWED_REPO", "marvin7122/qlever")
        head_repo = (self.pr["head"].get("repo") or {}).get("full_name")
        if self.repo != allowed or head_repo != allowed:
            raise SystemExit(f"refusing: the PR head {head_repo} / repository {self.repo} is not {allowed}")

    def record(self, outcome, task, detail):
        self.results.append((outcome, task.thread["comments"][0]["url"], task.rule, detail))

    def file_content(self, path):
        with open(os.path.join(self.repo_dir, path), encoding="utf-8") as f:
            return f.read()

    def write_file(self, path, content):
        with open(os.path.join(self.repo_dir, path), "w", encoding="utf-8") as f:
            f.write(content)

    def commit_change(self, task, old, new, message):
        path = task.thread["path"]
        self.write_file(path, new)
        clang_format_lines(self.repo_dir, path, changed_lines(old, new))
        git(self.repo_dir, "add", path)
        git(self.repo_dir, "commit", "-q", "-m",
            f"{message}\n\nFinding: {task.thread['comments'][0]['url']}\nRule: {task.rule}")
        return git(self.repo_dir, "rev-parse", "HEAD")

    def change_kind(self, task, result, has_text):
        kind = decide_kind(task.rule, self.kinds, has_text, result.model_kind)
        if kind is Kind.MECHANICAL and not all(is_mechanical_edit(e.path, e.old, e.new) for e in result.edits):
            log(f"{task.thread['path']}:{task.thread['line']}: the edit changes code, so it needs sign-off")
            return Kind.SIGNOFF
        return kind

    def new_branch(self, name):
        """`name`, or `name-2`, `name-3`, ... if a proposal branch of that name
        exists already (a new proposal never overwrites an old one)."""
        taken = set(git(self.repo_dir, "ls-remote", "--heads", "origin", f"{name}*").split())
        candidate, n = name, 1
        while f"refs/heads/{candidate}" in taken:
            n += 1
            candidate = f"{name}-{n}"
        return candidate

    def push_proposal(self, task, old, new, summary):
        """Commit the change on a new side branch and push it; return
        (sha, branch, the file as committed, the commit it is based on)."""
        head = git(self.repo_dir, "rev-parse", "HEAD")
        branch = self.new_branch(f"apply/{self.number}-{task.thread['comments'][0]['id']}")
        git(self.repo_dir, "switch", "-q", "-c", branch)
        try:
            sha = self.commit_change(task, old, new, f"apply-replies (proposal): {summary}")
            git(self.repo_dir, "push", "-q", "origin", f"{branch}:{branch}")
            committed = self.file_content(task.thread["path"])
        finally:
            git(self.repo_dir, "switch", "-q", self.head_ref)
            git(self.repo_dir, "reset", "-q", "--hard", head)
        return sha, branch, committed, head

    def commit_url(self, sha):
        return f"https://github.com/{self.repo}/commit/{sha}"

    def proposal_status(self, sha, branch):
        return (f"Commit [{sha[:9]}]({self.commit_url(sha)}) on `{branch}`. Reply `/approve` to apply it to "
                f"this PR, or `/reject` to drop it.")

    # Simple outcomes.
    def handle(self, task):
        thread, action = task.thread, task.action
        if action is Action.RESOLVE:
            reply(self.repo, self.number, thread, "Marked as fixed (🚀); resolving.", {"state": "resolved"})
            resolve(thread)
            self.record("resolved", task, "marked as fixed")
        elif action is Action.ACKNOWLEDGE:
            why = f": {task.signal.text}" if task.signal.text else ""
            reply(self.repo, self.number, thread,
                  f"Noted ({task.signal.what}{why}); no code change. It counts in the feedback data for {task.rule}.",
                  {"state": "acknowledged", "signal": task.signal.what})
            resolve(thread)
            self.record("acknowledged", task, task.signal.what)
        elif action is Action.DUPLICATE:
            if task.original is None:
                reply(self.repo, self.number, thread, f"Not marked as duplicate: {task.problem}",
                      {"state": "needs-input", "signal": task.signal.what})
                self.record("needs input", task, task.problem)
                return
            first = task.original["comments"][0]
            rule = finding_rule(task.original)
            reply(self.repo, self.number, thread,
                  f"Duplicate of [this finding]({first['url']}) ({rule}); only the original is fixed. Resolving.",
                  {"state": "duplicate", "signal": task.signal.what, "rule": task.rule,
                   "duplicate_of": {"comment_id": first["id"], "rule": rule, "url": first["url"]}})
            resolve(thread)
            self.record("duplicate", task, f"of {first['url']}")
        elif action is Action.IMPLEMENT:
            self.implement(task)
        elif action is Action.REWRITE:
            self.rewrite(task)
        elif action is Action.APPROVE:
            self.approve(task)
        elif action is Action.REJECT:
            branch = task.proposal["branch"]
            subprocess.run(["git", "push", "-q", "origin", "--delete", branch], cwd=self.repo_dir)
            reply(self.repo, self.number, thread, "Proposal dropped (`/reject`).", {"state": "rejected"})
            resolve(thread)
            self.record("rejected", task, branch)

    def outdated(self, task):
        if task.thread["line"]:
            return False
        reply(self.repo, self.number, task.thread,
              "Not changed: this finding is outdated (its lines no longer exist at the PR head).", {"state": "failed"})
        self.record("failed", task, "outdated")
        return True

    def implement(self, task):
        if self.outdated(task):
            return
        thread = task.thread
        old = self.file_content(thread["path"])
        result = harness()[0](task, old, self.statements.get(task.rule, ""), self.spend)
        if not result.edits:
            reply(self.repo, self.number, thread, f"No change made: {result.summary}", {"state": "no-change"})
            self.record("no change", task, result.summary)
            return
        new = apply_edits(old, result.edits)
        if self.change_kind(task, result, bool(task.signal.text)) is Kind.MECHANICAL:
            sha = self.commit_change(task, old, new, f"apply-replies: {result.summary}")
            self.applied.append(Applied(task, sha, lambda s, r=result: f"Applied in [{s[:9]}]({self.commit_url(s)}): "
                                        f"{r.summary}", {"state": "applied"}))
            return
        sha, branch, committed, base = self.push_proposal(task, old, new, result.summary)
        reason = "behavior change" if result.model_kind != "mechanical" else "not provably mechanical"
        body = (f"**Proposed change** (needs sign-off: {reason}): {result.summary}\n\n"
                f"{code_proposal(thread, old, committed)}\n\n{self.proposal_status(sha, branch)}")
        reply(self.repo, self.number, thread, body, {"state": "proposed", "branch": branch, "commit": sha, "base": base})
        self.record("proposed", task, f"{branch} {sha[:9]}")

    def rewrite(self, task):
        """Rewrite the finding with a concrete proposal (😕, 😄, /not-actionable)."""
        if self.outdated(task):
            return
        thread = task.thread
        old = self.file_content(thread["path"])
        result = harness()[1](task, old, self.statements.get(task.rule, ""), self.spend)
        _, bullets = REWRITE_FOCUS[task.signal.what]
        links = where_links(result.extra.get("where"), self.repo, self.head_sha, self.repo_dir)
        fields = {k: result.extra.get(k, "") for k in ("tldr", "why", "verify", "risk")}
        fields["where"] = ", ".join(links)
        original = finding_text(thread["comments"][0]["body"])
        record = {"signal": task.signal.what, "rule": task.rule, "original": original[:2000],
                  "tldr": short(fields["tldr"], 300), "why": short(fields["why"]), "where": links,
                  "verify": short(fields["verify"]), "risk": short(fields["risk"]), "summary": result.summary}
        if not result.edits:
            body = render_rewrite(fields, bullets, "", f"No code change proposed: {result.summary}", original)
            reply(self.repo, self.number, thread, body, {"state": "rewritten", "rewrite": record})
            self.record("rewritten", task, "no code change")
            return
        new = apply_edits(old, result.edits)
        if self.change_kind(task, result, False) is Kind.MECHANICAL:
            sha = self.commit_change(task, old, new, f"apply-replies: {result.summary}")
            formatted = self.file_content(thread["path"])
            code = code_proposal(thread, old, formatted)
            self.applied.append(Applied(
                task, sha,
                lambda s, f=fields, c=code: render_rewrite(f, bullets, c, f"Applied in [{s[:9]}]({self.commit_url(s)}).",
                                                           original),
                {"state": "applied", "rewrite": record}))
            return
        sha, branch, committed, base = self.push_proposal(task, old, new, result.summary)
        body = render_rewrite(fields, bullets, code_proposal(thread, old, committed),
                              self.proposal_status(sha, branch), original)
        reply(self.repo, self.number, thread, body,
              {"state": "proposed", "branch": branch, "commit": sha, "base": base, "rewrite": record})
        self.record("proposed", task, f"rewrite, {branch} {sha[:9]}")

    def approve(self, task):
        proposal = task.proposal
        git(self.repo_dir, "fetch", "-q", "origin", proposal["branch"])
        result = subprocess.run(["git", "cherry-pick", proposal["commit"]], cwd=self.repo_dir,
                                capture_output=True, text=True)
        if result.returncode != 0:
            subprocess.run(["git", "cherry-pick", "--abort"], cwd=self.repo_dir)
            reply(self.repo, self.number, task.thread,
                  "Could not apply the proposal: it conflicts with the current PR head. Reply with an "
                  "instruction and post `/apply-replies` to get a new proposal.", {"state": "failed"})
            self.record("failed", task, "conflict")
            return
        sha = git(self.repo_dir, "rev-parse", "HEAD")
        self.applied.append(Applied(task, sha, lambda s: f"Applied in [{s[:9]}]({self.commit_url(s)}) "
                                    f"(approved proposal {proposal['commit'][:9]}).", {"state": "applied"}))

    def push_and_report_applied(self):
        if not self.applied:
            return
        git(self.repo_dir, "push", "-q", "origin", f"HEAD:{self.head_ref}")
        for item in self.applied:
            reply(self.repo, self.number, item.task.thread, item.body(item.sha), {**item.state, "commit": item.sha})
            resolve(item.task.thread)
            if item.task.proposal:
                subprocess.run(["git", "push", "-q", "origin", "--delete", item.task.proposal["branch"]],
                               cwd=self.repo_dir)
            self.record("applied", item.task, item.sha[:9])

    def summary(self):
        counts = {}
        for outcome, *_ in self.results:
            counts[outcome] = counts.get(outcome, 0) + 1
        lines = [f"**OpenCodeReview apply-replies** (`{self.mode}`): "
                 + (", ".join(f"{n} {o}" for o, n in sorted(counts.items())) or "nothing to do")
                 + f"; model spend ${self.spend.usd:.4f} of ${self.spend.budget:.2f} ({self.spend.calls} calls).", ""]
        for outcome, url, rule, detail in self.results:
            lines.append(f"- {outcome}: [{rule}]({url}) {detail}")
        return "\n".join(lines) + "\n"

    def main(self):
        self.check_target()
        tasks = plan_tasks(fetch_threads(self.repo, self.number), self.owner, self.mode, self.finding_authors,
                           self.repo, self.number)
        # Duplicates first: their originals' implementations follow.
        tasks.sort(key=lambda t: t.action is not Action.DUPLICATE)
        log(f"{len(tasks)} task(s): " + ", ".join(f"{t.action.value}@{t.thread['path']}:{t.thread['line']}"
                                                   for t in tasks))
        needs_model = (Action.IMPLEMENT, Action.REWRITE)
        if any(t.action in needs_model for t in tasks):
            url, _, token = backend()
            ok, reason = check_balance(url, token, self.spend.budget)
            if not ok:
                for task in [t for t in tasks if t.action in needs_model]:
                    self.record("skipped", task, reason)
                tasks = [t for t in tasks if t.action not in needs_model]
        for task in tasks:
            try:
                self.handle(task)
            except BudgetExhausted:
                self.record("skipped", task, "the budget is used up")
            except Exception as error:  # noqa: BLE001 - one failed thread must not stop the others
                log(f"{task.thread['path']}:{task.thread['line']}: {error!r}")
                self.record("failed", task, str(error)[:200])
        self.push_and_report_applied()
        if self.results:
            gh("pr", "comment", str(self.number), "-R", self.repo, "--body", self.summary())


def rule_statements():
    from leg_rule_ids import preamble_length, rule_statements as statements
    with open(os.path.join(SCRIPT_DIR, "rule.json"), encoding="utf-8") as f:
        rules = json.load(f)["rules"]
    return statements(rules, preamble_length([r["rule"] for r in rules]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["replies", "decisions", "approve-all"], required=True)
    parser.add_argument("--repo-dir", required=True)
    Run(parser.parse_args()).main()


if __name__ == "__main__":
    main()

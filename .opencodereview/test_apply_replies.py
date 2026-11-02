#!/usr/bin/env python3
"""Offline tests of apply_replies.py: no network, no model, no git.

Run: python3 .opencodereview/test_apply_replies.py
The fixture fixtures/threads-323.json holds the real review threads of
marvin7122/qlever#323 (read-only, recorded with apply_replies.fetch_threads).
"""
import copy
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import apply_replies as a  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OWNER = "marvin7122"
BOT = {"github-actions[bot]", "github-actions"}


def load_fixture():
    with open(os.path.join(HERE, "fixtures", "threads-323.json"), encoding="utf-8") as f:
        return json.load(f)


def thread(rule="PP10", replies=(), reactions=(), resolved=False, line=10, start=None, path="src/a.cpp"):
    meta = json.dumps({"rule": rule})
    return {
        "id": "T1", "isResolved": resolved, "path": path, "line": line, "startLine": start,
        "comments": [{"id": 1, "url": "u", "author": "github-actions[bot]", "createdAt": "2026-11-02T10:00:00Z",
                      "body": f"<!-- ocr-1-0-x -->\n<!-- ocr-meta {meta} -->\n![x](y)\n{rule}: the finding.\nReact: ...",
                      "reactions": [{"content": c, "createdAt": t, "user": u} for c, t, u in reactions]}]
        + [{"id": 2 + i, "url": "u", "author": u, "createdAt": t, "body": b, "reactions": []}
           for i, (b, t, u) in enumerate(replies)],
    }


class KindTest(unittest.TestCase):
    kinds = a.load_kinds()

    def test_table(self):
        self.assertIs(a.rule_kind("PP10", self.kinds), a.Kind.MECHANICAL)
        self.assertIs(a.rule_kind("rule:comment-accuracy", self.kinds), a.Kind.MECHANICAL)
        self.assertIs(a.rule_kind("QC3", self.kinds), a.Kind.SIGNOFF)
        self.assertIsNone(a.rule_kind("CPP52", self.kinds))

    def test_decide(self):
        # A reaction: the table alone decides; unknown rules need sign-off.
        self.assertIs(a.decide_kind("PP10", self.kinds, False, "behavior"), a.Kind.MECHANICAL)
        self.assertIs(a.decide_kind("CPP52", self.kinds, False, "mechanical"), a.Kind.SIGNOFF)
        # A free-text instruction: the model decides, unless the table requires sign-off.
        self.assertIs(a.decide_kind("CPP52", self.kinds, True, "mechanical"), a.Kind.MECHANICAL)
        self.assertIs(a.decide_kind("PP10", self.kinds, True, "behavior"), a.Kind.SIGNOFF)
        self.assertIs(a.decide_kind("QC3", self.kinds, True, "mechanical"), a.Kind.SIGNOFF)


class SignalTest(unittest.TestCase):
    def test_reply_keywords(self):
        self.assertEqual(a.reply_signal("/improve shorter please", "t").action, a.Action.IMPLEMENT)
        self.assertEqual(a.reply_signal("/improve shorter please", "t").text, "shorter please")
        self.assertEqual(a.reply_signal("/wrong it is checked above", "t").action, a.Action.ACKNOWLEDGE)
        self.assertEqual(a.reply_signal("/not-actionable", "t").action, a.Action.REWRITE)
        skip = a.reply_signal("/skip not worth it here", "t")
        self.assertEqual((skip.action, skip.text), (a.Action.ACKNOWLEDGE, "not worth it here"))
        self.assertEqual(a.reply_signal("/approve", "t").action, a.Action.APPROVE)
        self.assertEqual(a.reply_signal("/reject", "t").action, a.Action.REJECT)
        self.assertEqual(a.reply_signal("Use a fixture instead.", "t").action, a.Action.IMPLEMENT)
        self.assertIsNone(a.reply_signal("/apply-replies", "t"))
        self.assertIsNone(a.reply_signal("/unknown x", "t"))
        self.assertIsNone(a.reply_signal("  ", "t"))

    def test_owner_only(self):
        t = thread(replies=[("Please fix it like X.", "2026-11-02T11:00:00Z", "someone")],
                   reactions=[("THUMBS_UP", "2026-11-02T11:00:00Z", "someone")])
        self.assertIsNone(a.owner_signal(t, OWNER))

    def test_latest_reply_wins_and_beats_reactions(self):
        t = thread(replies=[("/wrong no", "2026-11-02T11:00:00Z", OWNER),
                            ("Do it like X.", "2026-11-02T12:00:00Z", OWNER)],
                   reactions=[("ROCKET", "2026-11-02T13:00:00Z", OWNER)])
        signal = a.owner_signal(t, OWNER)
        self.assertEqual((signal.action, signal.text), (a.Action.IMPLEMENT, "Do it like X."))

    def test_reaction_priority(self):
        t = thread(reactions=[("THUMBS_UP", "2026-11-02T11:00:00Z", OWNER),
                              ("ROCKET", "2026-11-02T11:00:00Z", OWNER)])
        self.assertEqual(a.owner_signal(t, OWNER).action, a.Action.RESOLVE)
        t = thread(reactions=[("LAUGH", "2026-11-02T11:00:00Z", OWNER), ("HEART", "2026-11-02T11:00:00Z", OWNER)])
        self.assertEqual(a.owner_signal(t, OWNER).action, a.Action.IMPLEMENT)
        t = thread(reactions=[("LAUGH", "2026-11-02T11:00:00Z", OWNER), ("THUMBS_DOWN", "2026-11-02T11:00:00Z", OWNER)])
        self.assertEqual(a.owner_signal(t, OWNER).action, a.Action.ACKNOWLEDGE)
        t = thread(reactions=[("EYES", "2026-11-02T11:00:00Z", OWNER)])
        self.assertEqual(a.owner_signal(t, OWNER).action, a.Action.DUPLICATE)

    def test_own_replies_are_not_signals(self):
        t = thread(replies=[(a.marker({"state": "acknowledged"}) + "\nNoted.", "2026-11-02T11:00:00Z", OWNER)])
        self.assertIsNone(a.owner_signal(t, OWNER))


class PlanTest(unittest.TestCase):
    def test_fixture_323(self):
        threads = load_fixture()
        tasks = a.plan_tasks(threads, OWNER, "replies", BOT)
        self.assertEqual([(t.action, t.rule) for t in tasks], [
            (a.Action.IMPLEMENT, "unknown"), (a.Action.IMPLEMENT, "QC3"), (a.Action.REWRITE, "QC3"),
            (a.Action.IMPLEMENT, "IDM028"), (a.Action.IMPLEMENT, "IDM059")])
        # Resolved threads and threads without an owner signal are left alone.
        self.assertTrue(all(not t.thread["isResolved"] for t in tasks))
        self.assertEqual(a.plan_tasks(threads, OWNER, "decisions", BOT), [])
        self.assertEqual(a.plan_tasks(threads, OWNER, "approve-all", BOT), [])
        # Someone else's signals do not count.
        self.assertEqual(a.plan_tasks(threads, "someone", "replies", BOT), [])

    def test_rewrite_fixture(self):
        # Four real, untouched #323 findings with one owner signal each:
        # 😕, 😄, /not-actionable, /skip <reason>.
        with open(os.path.join(HERE, "fixtures", "threads-rewrite.json"), encoding="utf-8") as f:
            threads = json.load(f)
        tasks = a.plan_tasks(threads, OWNER, "replies", BOT)
        self.assertEqual([(t.action, t.signal.what) for t in tasks], [
            (a.Action.REWRITE, "😕 confusing"), (a.Action.REWRITE, "😄 too long"),
            (a.Action.REWRITE, "/not-actionable"), (a.Action.ACKNOWLEDGE, "/skip")])
        self.assertEqual(tasks[3].signal.text, "the extra check is not worth it here")
        self.assertTrue(all(t.signal.what in a.REWRITE_FOCUS for t in tasks[:3]))

    def test_handled_once_per_signal(self):
        t = thread(reactions=[("THUMBS_UP", "2026-11-02T11:00:00Z", OWNER)])
        self.assertEqual(len(a.plan_tasks([t], OWNER, "replies", BOT)), 1)
        t["comments"].append({"id": 9, "url": "u", "author": "github-actions[bot]", "createdAt": "2026-11-02T12:00:00Z",
                              "body": a.marker({"state": "applied"}) + "\nApplied.", "reactions": []})
        self.assertEqual(a.plan_tasks([t], OWNER, "replies", BOT), [])
        t["comments"].append({"id": 10, "url": "u", "author": OWNER, "createdAt": "2026-11-02T13:00:00Z",
                              "body": "Also rename it.", "reactions": []})
        self.assertEqual([x.action for x in a.plan_tasks([t], OWNER, "replies", BOT)], [a.Action.IMPLEMENT])

    def test_decisions_on_proposals(self):
        proposal = {"state": "proposed", "branch": "apply/1-1", "commit": "abc", "base": "def"}
        t = thread(replies=[("Fix it.", "2026-11-02T11:00:00Z", OWNER),
                            (a.marker(proposal) + "\nProposed.", "2026-11-02T12:00:00Z", "github-actions[bot]")])
        self.assertEqual(a.plan_tasks([t], OWNER, "replies", BOT), [])
        self.assertEqual([x.proposal for x in a.plan_tasks([t], OWNER, "approve-all", BOT)], [proposal])
        approved = copy.deepcopy(t)
        approved["comments"].append({"id": 9, "url": "u", "author": OWNER, "createdAt": "2026-11-02T13:00:00Z",
                                     "body": "/approve", "reactions": []})
        tasks = a.plan_tasks([approved], OWNER, "decisions", BOT)
        self.assertEqual([(x.action, x.proposal["branch"]) for x in tasks], [(a.Action.APPROVE, "apply/1-1")])
        rejected = copy.deepcopy(t)
        rejected["comments"].append({"id": 9, "url": "u", "author": OWNER, "createdAt": "2026-11-02T13:00:00Z",
                                     "body": "/reject", "reactions": []})
        self.assertEqual([x.action for x in a.plan_tasks([rejected], OWNER, "replies", BOT)], [a.Action.REJECT])

    def test_only_finding_threads(self):
        t = thread(reactions=[("THUMBS_UP", "2026-11-02T11:00:00Z", OWNER)])
        t["comments"][0]["author"] = "someone"
        self.assertEqual(a.plan_tasks([t], OWNER, "replies", BOT), [])


def finding(cid, created, path="src/a.cpp", line=10, start=None, rule="PP10", eyes=False, replies=()):
    t = thread(rule=rule, line=line, start=start, path=path, replies=replies,
               reactions=[("EYES", "2026-11-02T11:00:00Z", OWNER)] if eyes else ())
    t["id"] = f"T{cid}"
    first = t["comments"][0]
    first.update(id=cid, createdAt=created, url=f"https://github.com/marvin7122/qlever/pull/323#discussion_r{cid}")
    return t


class DuplicateTest(unittest.TestCase):
    def test_parse_link(self):
        p = a.parse_duplicate_link
        self.assertEqual(p("https://github.com/marvin7122/qlever/pull/323#discussion_r4444013643 same thing"),
                         ("marvin7122/qlever", 323, 4444013643))
        self.assertEqual(p("https://github.com/marvin7122/qlever/pull/323/files#discussion_r12"),
                         ("marvin7122/qlever", 323, 12))
        self.assertEqual(p("#discussion_r12"), (None, None, 12))
        self.assertEqual(p("discussion_r12"), (None, None, 12))
        self.assertIsNone(p("see the other one"))
        self.assertIsNone(p(""))
        self.assertIsNone(p("https://github.com/marvin7122/qlever/pull/323"))

    def test_validate_link(self):
        dup, orig = finding(2, "2026-11-02T10:01:00Z"), finding(1, "2026-11-02T10:00:00Z")
        other = finding(3, "2026-11-02T10:02:00Z")
        other["comments"][0]["author"] = "someone"
        threads = [dup, orig, other]
        check = lambda text: a.resolve_duplicate_link(text, dup, threads, BOT, "marvin7122/qlever", 323)  # noqa: E731
        self.assertIs(check("#discussion_r1")[0], orig)
        self.assertIs(check("https://github.com/marvin7122/qlever/pull/323#discussion_r1")[0], orig)
        self.assertIn("another pull request", check("https://github.com/marvin7122/qlever/pull/9#discussion_r1")[1])
        self.assertIn("this finding itself", check("#discussion_r2")[1])
        self.assertIn("not an OpenCodeReview finding", check("#discussion_r3")[1])
        self.assertIn("not a finding of this pull request", check("#discussion_r99")[1])
        self.assertIn("no link", check("the one above")[1])

    def test_link_in_plan(self):
        orig = finding(1, "2026-11-02T10:00:00Z")
        dup = finding(2, "2026-11-02T10:01:00Z", replies=[("/duplicate #discussion_r1", "2026-11-02T12:00:00Z", OWNER)])
        tasks = a.plan_tasks([orig, dup], OWNER, "replies", BOT, "marvin7122/qlever", 323)
        self.assertEqual([(t.action, t.thread["id"]) for t in tasks],
                         [(a.Action.DUPLICATE, "T2"), (a.Action.IMPLEMENT, "T1")])
        self.assertIs(tasks[0].original, orig)
        self.assertEqual(tasks[1].signal.what, "original of a duplicate")
        # An original with its own signal keeps it, and is not implemented twice.
        orig_wrong = finding(1, "2026-11-02T10:00:00Z", replies=[("/wrong no", "2026-11-02T12:00:00Z", OWNER)])
        tasks = a.plan_tasks([orig_wrong, dup], OWNER, "replies", BOT, "marvin7122/qlever", 323)
        self.assertEqual([(t.action, t.thread["id"]) for t in tasks],
                         [(a.Action.ACKNOWLEDGE, "T1"), (a.Action.DUPLICATE, "T2")])
        # An invalid link: no original, the reply asks for one.
        bad = finding(2, "2026-11-02T10:01:00Z", replies=[("/duplicate the other", "2026-11-02T12:00:00Z", OWNER)])
        tasks = a.plan_tasks([orig, bad], OWNER, "replies", BOT, "marvin7122/qlever", 323)
        self.assertEqual([(t.action, t.original, bool(t.problem)) for t in tasks], [(a.Action.DUPLICATE, None, True)])

    def test_eyes_fallback(self):
        dup = finding(5, "2026-11-02T10:05:00Z", line=12, start=10, eyes=True)
        one = finding(1, "2026-11-02T10:00:00Z", line=11)
        elsewhere = finding(2, "2026-11-02T10:00:00Z", line=40)
        other_file = finding(3, "2026-11-02T10:00:00Z", line=11, path="src/b.cpp")
        original, problem = a.eyes_original(dup, [dup, one, elsewhere, other_file], OWNER, BOT)
        self.assertIs(original, one)
        # Several unmarked candidates: ambiguous, ask.
        two = finding(4, "2026-11-02T10:01:00Z", line=10)
        original, problem = a.eyes_original(dup, [dup, one, two], OWNER, BOT)
        self.assertIsNone(original)
        self.assertIn("several findings", problem)
        self.assertIn("discussion_r1", problem)
        # No overlapping finding: ask.
        self.assertIn("no other finding", a.eyes_original(dup, [dup, elsewhere], OWNER, BOT)[1])
        # All marked: the earliest is the original, unless it is this one.
        early = finding(0, "2026-11-02T09:00:00Z", line=12, eyes=True)
        self.assertIs(a.eyes_original(dup, [dup, early], OWNER, BOT)[0], early)
        late = finding(9, "2026-11-02T11:00:00Z", line=12, eyes=True)
        self.assertIn("earliest", a.eyes_original(dup, [dup, late], OWNER, BOT)[1])
        # In the plan: the 👀 thread is a duplicate of the one overlapping finding.
        tasks = a.plan_tasks([dup, one, elsewhere], OWNER, "replies", BOT, "marvin7122/qlever", 323)
        self.assertEqual([(t.action, t.thread["id"]) for t in tasks],
                         [(a.Action.DUPLICATE, "T5"), (a.Action.IMPLEMENT, "T1")])


class EditTest(unittest.TestCase):
    def test_mechanical_check(self):
        self.assertTrue(a.is_mechanical_edit("src/a.cpp", "// old text\nint x = 1;", "// new text\n// more\nint x = 1;"))
        self.assertTrue(a.is_mechanical_edit("src/a.cpp", "#include <a>\nint x;", "#include <a>\n#include <b>\nint x;"))
        self.assertTrue(a.is_mechanical_edit("test/aT.cpp", 'EXPECT_THAT(f(), HasSubstr("old"));',
                                             'EXPECT_THAT(f(), HasSubstr("new message"));'))
        self.assertFalse(a.is_mechanical_edit("src/a.cpp", 'throw E("old");', 'throw E("new");'))
        self.assertFalse(a.is_mechanical_edit("src/a.cpp", "int x = 1;", "int x = 2;"))
        self.assertFalse(a.is_mechanical_edit("src/a.cpp", "// c\nf(x);", "// c\nAD_CONTRACT_CHECK(x);\nf(x);"))

    def test_apply_edits(self):
        content = "a\nb\nc\n"
        self.assertEqual(a.apply_edits(content, [a.Edit("p", "b\n", "B\nB2\n")]), "a\nB\nB2\nc\n")
        with self.assertRaises(RuntimeError):
            a.apply_edits("x\nx\n", [a.Edit("p", "x", "y")])

    def test_suggestion_and_diff(self):
        old = "".join(f"line{n}\n" for n in range(1, 21))
        inside = old.replace("line10\n", "line10 changed\nline10b\n")
        t = thread(line=11, start=10)
        self.assertEqual(a.suggestion_block(t, old, inside), "```suggestion\nline10 changed\nline10b\nline11\n```")
        outside = old.replace("line3\n", "line3 changed\n")
        self.assertIsNone(a.suggestion_block(t, old, outside))
        diff = a.unified_diff("src/a.cpp", old, outside)
        self.assertIn("-line3\n+line3 changed\n", diff)
        self.assertTrue(diff.startswith("--- a/src/a.cpp\n+++ b/src/a.cpp\n"))
        self.assertEqual(a.changed_lines(old, inside), [10, 11])

    def test_search_replace_harness_retries(self):
        answers = [{"edits": [{"old": "missing", "new": "x"}], "kind": "mechanical", "summary": "s"},
                   {"edits": [{"old": "// old", "new": "// new"}], "kind": "mechanical", "summary": "fix the comment"}]
        seen = []

        def fake_chat(messages, spend):
            seen.append(messages[-1]["content"])
            return answers[len(seen) - 1]
        original, a.chat = a.chat, fake_chat
        try:
            task = a.Task(thread(), a.Action.IMPLEMENT, a.Signal(a.Action.IMPLEMENT, "", "👍", "t"), "PP10")
            result = a.search_replace_harness(task, "int x;\n// old\n", "Comments are accurate.", a.Spend(1.0))
        finally:
            a.chat = original
        self.assertEqual((result.summary, result.model_kind, len(result.edits)), ("fix the comment", "mechanical", 1))
        self.assertIn("do not occur exactly once", seen[1])
        self.assertIn("Rule PP10: Comments are accurate.", seen[0])

    def test_rewrite_harness_focus(self):
        prompts = []

        def fake_chat(messages, spend):
            prompts.append(messages[-1]["content"])
            return {"tldr": "Name the check.", "edits": [{"old": "// old", "new": "// new"}], "kind": "mechanical",
                    "summary": "s", "why": "w", "where": ["src/a.cpp:2"], "verify": "run aTest", "risk": "none"}
        original, a.chat = a.chat, fake_chat
        try:
            for what, needle in [("😕 confusing", "readable and actionable"), ("😄 too long", "short TL;DR"),
                                 ("/not-actionable", "how to verify it")]:
                task = a.Task(thread(), a.Action.REWRITE, a.Signal(a.Action.REWRITE, "", what, "t"), "PP10")
                result = a.search_replace_rewrite(task, "int x;\n// old\n", "Comments are accurate.", a.Spend(1.0))
                self.assertIn(needle, prompts[-1])
                self.assertEqual((result.extra["tldr"], result.extra["verify"]), ("Name the check.", "run aTest"))
        finally:
            a.chat = original

    def test_render_rewrite(self):
        fields = {"tldr": "Assert it.", "why": "w " * 200, "where": "[a.cpp:3](u)", "verify": "run aTest", "risk": "r"}
        for what, expected in [("😄 too long", ["Why", "Where", "Risk"]), ("/not-actionable", ["Where", "Verify", "Why"])]:
            body = a.render_rewrite(fields, a.REWRITE_FOCUS[what][1], "```suggestion\nx\n```", "Status.", "orig text")
            self.assertTrue(body.startswith("**TL;DR:** Assert it."))
            self.assertLess(body.index("```suggestion"), body.index("- **"))
            bullets = [l for l in body.splitlines() if l.startswith("- **")]
            self.assertEqual([b.split("**")[1].rstrip(":") for b in bullets], expected)
            self.assertTrue(all(len(b) < a.BULLET_CHARS + 30 for b in bullets))
            self.assertTrue(body.rstrip().endswith("<details><summary>Original finding</summary>\n\norig text\n\n</details>"))

    def test_where_links(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "src"))
            with open(os.path.join(d, "src", "a.cpp"), "w") as f:
                f.write("1\n2\n3\n")
            links = a.where_links(["src/a.cpp:2", "src/a.cpp:2-3", "src/a.cpp:9", "missing.cpp:1", "nonsense"],
                                  "o/r", "abc", d)
        self.assertEqual(links, ["[src/a.cpp:2](https://github.com/o/r/blob/abc/src/a.cpp#L2)",
                                 "[src/a.cpp:2-3](https://github.com/o/r/blob/abc/src/a.cpp#L2-L3)"])

    def test_finding_text(self):
        body = "<!-- ocr-1 -->\n![b](c)\nPP10: stale.\n<details><summary>x</summary>code</details>\nRule: [PP10](u)\nReact: 👍"
        self.assertEqual(a.finding_text(body), "PP10: stale.")


class ConfigTest(unittest.TestCase):
    def test_backend_override(self):
        env = {"OCR_LLM_URL": "u", "OCR_LLM_MODEL": "m", "OCR_LLM_AUTH_TOKEN": "t", "OCR_APPLY_LLM_MODEL": "m2"}
        saved = {k: os.environ.get(k) for k in list(env) + ["OCR_APPLY_LLM_URL", "OCR_APPLY_LLM_AUTH_TOKEN"]}
        os.environ.update(env)
        for k in ("OCR_APPLY_LLM_URL", "OCR_APPLY_LLM_AUTH_TOKEN"):
            os.environ.pop(k, None)
        try:
            self.assertEqual(a.backend(), ("u", "m2", "t"))
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_unknown_harness(self):
        os.environ["OCR_APPLY_AGENT"] = "nope"
        try:
            with self.assertRaises(SystemExit):
                a.harness()
        finally:
            del os.environ["OCR_APPLY_AGENT"]
        self.assertEqual(a.harness(), (a.search_replace_harness, a.search_replace_rewrite))

    def test_spend(self):
        os.environ.update({"OCR_PRICE_INPUT_PER_M": "0.14", "OCR_PRICE_CACHE_HIT_PER_M": "0.0028",
                           "OCR_PRICE_OUTPUT_PER_M": "0.28"})
        spend = a.Spend(0.01)
        spend.add({"prompt_tokens": 50_000, "prompt_cache_hit_tokens": 0, "completion_tokens": 10_000})
        self.assertAlmostEqual(spend.usd, 0.0098)
        self.assertTrue(spend.exhausted())
        with self.assertRaises(a.BudgetExhausted):
            a.chat([], spend)


if __name__ == "__main__":
    unittest.main(verbosity=1)

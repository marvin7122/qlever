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
        skipped = []
        tasks = a.plan_tasks(threads, OWNER, "replies", BOT, skipped=skipped)
        self.assertEqual([(t.action, t.rule) for t in tasks], [
            (a.Action.IMPLEMENT, "unknown"), (a.Action.IMPLEMENT, "QC60"), (a.Action.IMPLEMENT, "QC3"),
            (a.Action.IMPLEMENT, "QC3"), (a.Action.REWRITE, "QC3"), (a.Action.IMPLEMENT, "IDM028"),
            (a.Action.IMPLEMENT, "IDM059")])
        # A resolved thread with an owner signal is acted on (resolving is not a signal): the 👍 threads
        # QC60 (outdated) and QC3 at line 102, which the run 41510928135 skipped silently.
        self.assertEqual([(t.rule, t.thread["line"]) for t in tasks if t.thread["isResolved"]],
                         [("QC60", None), ("QC3", 102)])
        # Owner reactions on other bots' threads are listed as skipped, with the reason.
        self.assertEqual([why for _, why in skipped], ["not an OpenCodeReview finding thread"] * 2)
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
        content = "a\nb\nc\nd\n"
        # Several ranges, applied bottom-up; an insertion repeats its anchor line; "" deletes.
        edits = [a.Edit("p", 2, 2, "B\nB2"), a.Edit("p", 4, 4, "")]
        self.assertEqual(a.apply_edits(content, edits), "a\nB\nB2\nc\n")
        self.assertEqual(a.apply_edits(content, [a.Edit("p", 1, 1, "a\n#include <x>\n")]), "a\n#include <x>\nb\nc\nd\n")
        self.assertEqual(a.edit_old_text(content, a.Edit("p", 2, 3, "")), "b\nc\n")
        with self.assertRaises(RuntimeError):
            a.apply_edits(content, [a.Edit("p", 4, 5, "x")])

    def test_edit_problems(self):
        content = "a\nb\nc\n"
        self.assertEqual(a.edit_problems(content, [a.Edit("p", 1, 1, ""), a.Edit("p", 2, 3, "")]), [])
        self.assertIn("not lines of the file", a.edit_problems(content, [a.Edit("p", 0, 1, "")])[0])
        self.assertIn("not lines of the file", a.edit_problems(content, [a.Edit("p", 3, 2, "")])[0])
        self.assertIn("overlap", a.edit_problems(content, [a.Edit("p", 1, 2, ""), a.Edit("p", 2, 3, "")])[0])
        self.assertEqual(a.parse_edits("p", [{"start": "2", "end": 2, "replacement": "x"}])[0],
                         [a.Edit("p", 2, 2, "x")])
        self.assertIn("malformed", a.parse_edits("p", [{"old": "x"}])[1])

    def test_relocate_edits(self):
        base = "a\nb\nc\n"
        edits = [a.Edit("p", 2, 2, "B")]
        self.assertEqual(a.relocate_edits(base, base, edits), edits)
        self.assertEqual(a.relocate_edits(base, "x\ny\na\nb\nc\n", edits), [a.Edit("p", 4, 4, "B")])
        self.assertIsNone(a.relocate_edits(base, "b\na\nb\n", edits), "ambiguous")
        self.assertIsNone(a.relocate_edits(base, "a\nc\n", edits), "gone")

    def test_diff(self):
        old = "".join(f"line{n}\n" for n in range(1, 21))
        outside = old.replace("line3\n", "line3 changed\n")
        diff = a.unified_diff("src/a.cpp", old, outside)
        self.assertIn("-line3\n+line3 changed\n", diff)
        self.assertTrue(diff.startswith("--- a/src/a.cpp\n+++ b/src/a.cpp\n"))
        self.assertIn("<details><summary>Diff (src/a.cpp)</summary>", a.diff_details("src/a.cpp", old, outside))
        self.assertEqual(a.changed_lines(old, old.replace("line10\n", "line10 changed\nline10b\n")), [10, 11])

    def test_search_replace_harness_retries(self):
        answers = [{"edits": [{"start": 2, "end": 9, "replacement": "x"}], "kind": "mechanical", "summary": "s"},
                   {"edits": [{"start": 2, "end": 2, "replacement": "// new"}], "kind": "mechanical",
                    "summary": "fix the comment"}]
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
        self.assertEqual((result.summary, result.model_kind, result.edits),
                         ("fix the comment", "mechanical", [a.Edit("src/a.cpp", 2, 2, "// new")]))
        self.assertIn("cannot be applied", seen[1])
        self.assertIn("not lines of the file (1-2)", seen[1])
        self.assertIn("Rule PP10: Comments are accurate.", seen[0])

    def test_rewrite_harness_focus(self):
        prompts = []

        def fake_chat(messages, spend):
            prompts.append(messages[-1]["content"])
            return {"tldr": "Name the check.", "edits": [{"start": 2, "end": 2, "replacement": "// new"}],
                    "kind": "mechanical",
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


REPO_ROOT = os.path.dirname(HERE)
FILE = "".join(f"  int x{n} = {n};\n" for n in range(1, 31))


class SuggestionTest(unittest.TestCase):
    def test_render(self):
        s = a.Suggestion("src/a.cpp", 3, 4, "old3\nold4\n", "new3\n")
        self.assertEqual(s.block(), "```suggestion\nnew3\n```")
        self.assertEqual(a.Suggestion("src/a.cpp", 3, 3, "old\n", "").block(), "```suggestion\n```", "deletes the line")
        self.assertEqual(a.Suggestion("src/a.cpp", 3, 3, "old\n", "\n").block(), "```suggestion\n\n```",
                         "one empty line, not a deletion")
        self.assertEqual(a.Suggestion("src/a.cpp", 9, 9, "x", "y").block(), "```suggestion\ny\n```",
                         "the last line of a file without a final newline")
        self.assertEqual(s.state(7)["comment_id"], 7)

    def test_diff_suggestions_and_round_trip(self):
        new = FILE.replace("  int x5 = 5;\n", "  int x5 = 50;\n")  # a replacement
        new = new.replace("  int x10 = 10;\n", "  int x10 = 10;\n  int y = 0;\n")  # an insertion
        new = new.replace("  int x20 = 20;\n", "")  # a deletion
        suggestions = a.diff_suggestions("src/a.cpp", FILE, new)
        self.assertEqual([(s.start, s.end, s.new) for s in suggestions],
                         [(5, 5, "  int x5 = 50;\n"), (10, 10, "  int x10 = 10;\n  int y = 0;\n"), (20, 20, "")])
        self.assertEqual(a.apply_suggestions(FILE, suggestions), new)
        # An insertion at the top of the file is anchored on the line below.
        top = "#include <x>\n" + FILE
        [s] = a.diff_suggestions("src/a.cpp", FILE, top)
        self.assertEqual((s.start, s.end, s.new), (1, 1, "#include <x>\n  int x1 = 1;\n"))
        # Regression (end-to-end run on #338): an insertion anchored on an empty
        # line that ends with an empty line keeps both empty lines.
        head = "#include <vector>\n\n// A comment.\nnamespace a {\n"
        committed = "#include <vector>\n\n#include \"util/Exception.h\"\n\n// A comment.\nnamespace a {\n"
        [s] = a.diff_suggestions("src/a.h", head, committed)
        self.assertEqual(s.end - s.start, 0, "one anchor line")
        self.assertEqual(a.apply_suggestions(head, [s]), committed)
        self.assertEqual(a.one_click_gate("src/a.h", head, committed, [(1, 4)], lambda *args: "")[1], "")
        # Blocks that touch are merged into one suggestion.
        touching = FILE.replace("  int x7 = 7;\n  int x8 = 8;\n", "  int x7 = 70;\n  int x8 = 80;\n  int z = 0;\n")
        self.assertEqual(len(a.diff_suggestions("src/a.cpp", FILE, touching)), 1)

    def test_diff_hunks(self):
        patch = "@@ -1,3 +1,4 @@\n a\n+b\n c\n d\n@@ -40,2 +41,5 @@ void f() {\n x\n@@ -60 +64 @@\n@@ -70,2 +75,0 @@\n"
        self.assertEqual(a.diff_hunks(patch), [(1, 4), (41, 45), (64, 64)])
        self.assertEqual(a.diff_hunks(None), [])

    def test_gate(self):
        new = FILE.replace("  int x5 = 5;\n", "  int x5 = 50;\n").replace("  int x25 = 25;\n", "  int x25 = 250;\n")
        ok = lambda *args: ""  # noqa: E731
        suggestions, why = a.one_click_gate("src/a.cpp", FILE, new, [(1, 30)], ok)
        self.assertEqual((len(suggestions), why), (2, ""))
        # A line outside the PR's diff hunks: no one-click suggestion.
        suggestions, why = a.one_click_gate("src/a.cpp", FILE, new, [(1, 10)], ok)
        self.assertEqual(suggestions, [])
        self.assertIn("line(s) 25 of `src/a.cpp` are not within one hunk", why)
        # A suggestion spanning two hunks cannot be one multi-line comment.
        two_lines = FILE.replace("  int x5 = 5;\n  int x6 = 6;\n", "  int x5 = 50;\n  int x6 = 60;\n")
        self.assertIn("not within one hunk", a.one_click_gate("src/a.cpp", FILE, two_lines, [(1, 5), (6, 30)], ok)[1])
        # Not formatted: the formatter's verdict decides.
        self.assertEqual(a.one_click_gate("src/a.cpp", FILE, new, [(1, 30)], lambda *args: "not formatted")[1],
                         "not formatted")
        # A file without a final newline round-trips too (the lines are kept exactly).
        self.assertEqual(a.one_click_gate("src/a.cpp", FILE, new[:-1], [(1, 30)], ok)[1], "")
        # Round trip: if committing the suggestions does not give the side-branch file, no one-click.
        original, a.apply_suggestions = a.apply_suggestions, lambda content, suggestions: content
        try:
            self.assertIn("would not give the proposed file", a.one_click_gate("src/a.cpp", FILE, new, [(1, 30)], ok)[1])
        finally:
            a.apply_suggestions = original
        self.assertEqual(a.one_click_gate("src/a.cpp", FILE, FILE, [(1, 30)], ok)[1], "the change is empty")

    def test_clang_format_check(self):
        # Under the file's real name, from the repository root (its .clang-format).
        saved_dir, saved = os.getcwd(), os.environ.get("CLANG_FORMAT")
        os.chdir(REPO_ROOT)
        os.environ["CLANG_FORMAT"] = "clang-format"
        try:
            formatted = "int f() { return 1; }\n"
            self.assertEqual(a.clang_format_problem("src/util/X.cpp", formatted, [1]), "")
            self.assertIn("would change", a.clang_format_problem("src/util/X.cpp", "int  f( ){return 1;}\n", [1]))
            self.assertEqual(a.clang_format_problem("docs/x.md", "anything", [1]), "", "not C++")
        finally:
            os.chdir(saved_dir)
            if saved is None:
                os.environ.pop("CLANG_FORMAT", None)
            else:
                os.environ["CLANG_FORMAT"] = saved

    def test_place_in_thread_or_own_comment(self):
        t = thread(path="src/a.cpp", line=6, start=4)
        new = FILE.replace("  int x5 = 5;\n", "  int x5 = 50;\n").replace("  int x20 = 20;\n", "  int x20 = 200;\n")
        suggestions = a.diff_suggestions("src/a.cpp", FILE, new)
        own, others = a.place_suggestions(t, FILE, suggestions)
        # Inside the anchor: one suggestion replacing exactly the anchored lines 4-6.
        self.assertEqual((own.start, own.end), (4, 6))
        self.assertEqual(own.new, "  int x4 = 4;\n  int x5 = 50;\n  int x6 = 6;\n")
        self.assertEqual(own.old, "  int x4 = 4;\n  int x5 = 5;\n  int x6 = 6;\n")
        # Outside: a review comment of its own on exactly its lines.
        self.assertEqual([(s.start, s.end) for s in others], [(20, 20)])
        # Committing the anchor suggestion and the other one gives the proposed file.
        self.assertEqual(a.apply_suggestions(FILE, [own] + others), new)
        # No suggestion inside the anchor: all get their own comment.
        own, others = a.place_suggestions(thread(path="src/a.cpp", line=12), FILE, suggestions)
        self.assertIsNone(own)
        self.assertEqual(len(others), 2)
        # Another file is never placed in the thread.
        own, _ = a.place_suggestions(thread(path="src/b.cpp", line=6, start=4), FILE, suggestions)
        self.assertIsNone(own)

    def test_committed_via_button(self):
        new = FILE.replace("  int x5 = 5;\n", "  int x5 = 50;\n").replace("  int x20 = 20;\n", "")
        recorded = [s.state() for s in a.diff_suggestions("src/a.cpp", FILE, new)]
        self.assertTrue(a.suggestions_applied({"src/a.cpp": new}, recorded))
        self.assertFalse(a.suggestions_applied({"src/a.cpp": FILE}, recorded), "nothing committed")
        half = FILE.replace("  int x5 = 5;\n", "  int x5 = 50;\n")
        self.assertFalse(a.suggestions_applied({"src/a.cpp": half}, recorded), "the deletion is missing")
        self.assertFalse(a.suggestions_applied({}, recorded), "the file is gone")
        self.assertFalse(a.suggestions_applied({"src/a.cpp": new}, []), "no suggestions recorded")
        inserted = FILE.replace("  int x9 = 9;\n", "  int x9 = 9;\n  int y = 1;\n")
        recorded = [s.state() for s in a.diff_suggestions("src/a.cpp", FILE, inserted)]
        self.assertTrue(a.suggestions_applied({"src/a.cpp": inserted}, recorded), "an insertion keeps its anchor line")

    def test_plan_notices_button_commits(self):
        proposal = {"state": "proposed", "branch": "apply/1-1", "commit": "abc", "base": "def",
                    "suggestions": [{"path": "src/a.cpp", "start": 5, "end": 5, "old": "o", "new": "n"}]}
        t = thread(replies=[("Fix it.", "2026-11-02T11:00:00Z", OWNER),
                            (a.marker(proposal) + "\nProposed.", "2026-11-02T12:00:00Z", "github-actions[bot]")])
        for mode in ("replies", "decisions", "approve-all"):
            tasks = a.plan_tasks([t], OWNER, mode, BOT, committed=lambda state: True)
            self.assertEqual([(x.action, x.proposal["branch"]) for x in tasks], [(a.Action.COMMITTED, "apply/1-1")])
        self.assertEqual(a.plan_tasks([t], OWNER, "replies", BOT, committed=lambda state: False), [])
        self.assertEqual([x.action for x in a.plan_tasks([t], OWNER, "approve-all", BOT, committed=lambda s: False)],
                         [a.Action.APPROVE])

    def test_summary_counts_one_click(self):
        run = a.Run.__new__(a.Run)
        run.mode, run.results, run.one_click, run.spend = "replies", [], [2, 3], a.Spend(2.0)
        run.handled_earlier = []
        self.assertIn("One-click: 2 of 3 proposals.", run.summary())
        run.one_click = [0, 0]
        self.assertNotIn("One-click", run.summary())


def signals_fixture():
    """The threads of #323 with owner signals (recorded read-only after run
    41510928135), plus the parents of the flow's suggestion threads."""
    with open(os.path.join(HERE, "fixtures", "threads-323-signals.json"), encoding="utf-8") as f:
        return json.load(f)


HEAD_323 = {"test/IoUringManagerTest.cpp": "IoUringManagerTest.cpp", "src/util/IoUringManager.cpp": "IoUringManager.cpp"}


def head_323(path):
    with open(os.path.join(HERE, "fixtures", "head-323", HEAD_323[path]), encoding="utf-8") as f:
        return f.read()


class SignalsOf323Test(unittest.TestCase):
    """The owner signals run 41510928135 missed on #323."""

    def plan(self, mode="replies"):
        skipped = []
        tasks = a.plan_tasks(signals_fixture(), OWNER, mode, BOT, "marvin7122/qlever", 323, skipped=skipped)
        return {t.thread["comments"][0]["id"]: t for t in tasks}, {t["comments"][0]["id"]: why for t, why in skipped}

    def test_resolved_thread_with_thumbs_up(self):
        # r4444039306 (QC3, PageCacheFirstPolicy.h:102): resolved, 👍 at 16:01 - acted on now.
        tasks, skipped = self.plan()
        task = tasks[4444039306]
        self.assertEqual((task.action, task.rule, task.thread["line"], task.thread["isResolved"]),
                         (a.Action.IMPLEMENT, "QC3", 102, True))
        self.assertNotIn(4444039306, skipped)

    def test_replies_in_suggestion_threads_go_to_the_finding(self):
        # r4446838755 and r4446839683 are this flow's suggestion comments; the
        # owner's replies in their threads count for findings 4444039334 and 4444053995.
        threads = {t["comments"][0]["id"]: t for t in signals_fixture()}
        self.assertEqual(a.suggestion_parent(threads[4446838755]), 4444039334)
        self.assertEqual(a.suggestion_parent(threads[4446839683]), 4444053995)
        tasks, skipped = self.plan()
        self.assertIn("tests only file", tasks[4444039334].signal.text)
        self.assertEqual(tasks[4444039334].action, a.Action.IMPLEMENT)
        self.assertEqual(tasks[4444053995].action, a.Action.IMPLEMENT)
        self.assertTrue(tasks[4444053995].signal.text)
        self.assertNotIn(4446838755, skipped, "a suggestion thread is not reported on its own")
        self.assertNotIn(4446838755, tasks)

    def test_every_signal_is_listed(self):
        tasks, skipped = self.plan()
        signalled = {t["comments"][0]["id"] for t in a.with_suggestion_replies(signals_fixture())
                     if a.suggestion_parent(t) is None and (a.owner_signal(t, OWNER) or a.unknown_command(t, OWNER))}
        self.assertEqual(signalled, set(tasks) | set(skipped), "no thread with an owner signal is left out")
        self.assertEqual(skipped[4432842062], "not an OpenCodeReview finding thread")
        self.assertEqual(skipped[4444039242], "already handled (applied)")

    def test_skip_reasons(self):
        approve = thread(replies=[("/approve", "2026-11-02T12:00:00Z", OWNER)])
        unknown = thread(replies=[("/apply please", "2026-11-02T12:00:00Z", OWNER)])
        skipped = []
        self.assertEqual(a.plan_tasks([approve, unknown], OWNER, "replies", BOT, skipped=skipped), [])
        self.assertEqual([why for _, why in skipped], ["`/approve` without a pending proposal", "unknown keyword `/apply`"])

    def test_outdated_threads_already_applied(self):
        # r4444034076 (QC60) and r4444039212 (QC3) became outdated by the owner's
        # button commits of the findings' own suggestions.
        threads = {t["comments"][0]["id"]: t for t in signals_fixture()}
        for cid in (4444034076, 4444039212):
            t = threads[cid]
            self.assertIsNone(t["line"])
            content = head_323(t["path"])
            self.assertTrue(a.own_suggestion_applied(t, content))
            self.assertIsNone(a.relocate_lines(a.anchored_original(t), content, t["comments"][0]["originalLine"]))
        self.assertEqual(a.anchored_original(threads[4444039212]),
                         "  AD_CONTRACT_CHECK(offsets.size() == numberOfBytesToRead.size() &&\n"
                         "                    buffers.size() == numberOfBytesToRead.size());\n")

    def test_relocate(self):
        block = "  int a = 1;\n  int b = 2;\n"
        content = "".join(f"// line {n}\n" for n in range(1, 41)) + block + "x\n" + block
        # `near` is the original anchor's last line.
        self.assertEqual(a.relocate_lines(block, content, 42), (41, 42), "exact, nearest to the original line")
        self.assertEqual(a.relocate_lines(block, content, 44), (44, 45))
        self.assertEqual(a.relocate_lines(block, content, 90), (44, 45))
        reformatted = "".join(f"// line {n}\n" for n in range(1, 41)) + "  int a  =  1;\n  int b = 2;\n"
        self.assertEqual(a.relocate_lines(block, reformatted, 38), (41, 42), "nearby, whitespace ignored")
        self.assertIsNone(a.relocate_lines(block, reformatted, 200), "too far away")
        self.assertIsNone(a.relocate_lines(block, "nothing here\n", 1))

    def run_outdated(self, t, files):
        """Run.outdated on a thread, with the files of a fake checkout and the replies recorded."""
        import tempfile
        posted = []
        run = a.Run.__new__(a.Run)
        run.repo, run.number, run.results = "marvin7122/qlever", 323, []
        saved = a.reply, a.resolve
        a.reply = lambda repo, number, thread, text, state: posted.append((text, state))
        a.resolve = lambda thread: posted.append(("resolved", {}))
        with tempfile.TemporaryDirectory() as d:
            for path, content in files.items():
                os.makedirs(os.path.dirname(os.path.join(d, path)), exist_ok=True)
                with open(os.path.join(d, path), "w", encoding="utf-8") as f:
                    f.write(content)
            run.repo_dir = d
            task = a.Task(t, a.Action.IMPLEMENT, a.Signal(a.Action.IMPLEMENT, "", "👍", "t"), a.finding_rule(t))
            try:
                skip = run.outdated(task)
            finally:
                a.reply, a.resolve = saved
        return skip, task, run.results, posted

    def test_run_outdated_paths(self):
        t = {x["comments"][0]["id"]: x for x in signals_fixture()}[4444039212]
        # The finding's own suggestion is on the branch: already applied (the thread is resolved already).
        skip, _, results, posted = self.run_outdated(t, {t["path"]: head_323(t["path"])})
        self.assertTrue(skip)
        self.assertEqual(results[0][0], "already applied")
        self.assertEqual(posted, [("Already on the branch: this finding's own suggestion was committed. Resolving.",
                                   {"state": "applied", "via": "finding suggestion"})])
        # Not applied, and the anchored lines moved 30 lines down: relocated, acted on there.
        moved = "".join(f"// {n}\n" for n in range(1, 395)) + a.anchored_original(t) + "}\n"
        skip, task, results, posted = self.run_outdated(t, {t["path"]: moved})
        self.assertFalse(skip)
        self.assertEqual((task.thread["startLine"], task.thread["line"], task.thread["relocated"]), (395, 396, True))
        self.assertEqual((results, posted), ([], []))
        # Neither: skipped, said in the thread and in the summary.
        skip, _, results, posted = self.run_outdated(t, {t["path"]: "int unrelated;\n"})
        self.assertTrue(skip)
        self.assertEqual(results[0][0], "skipped: outdated, could not relocate")
        self.assertIn("could not be found", posted[0][0])
        # A relocated thread gets no suggestion in its own reply (its anchor is gone).
        own, others = a.place_suggestions({**t, "line": 396, "startLine": 395, "relocated": True}, moved,
                                          [a.Suggestion(t["path"], 395, 396, "x", "y")])
        self.assertIsNone(own)
        self.assertEqual(len(others), 1)

    def test_summary_lists_skips(self):
        run = a.Run.__new__(a.Run)
        run.mode, run.one_click, run.spend = "replies", [0, 0], a.Spend(2.0)
        run.results = [("implement", "u1", "QC3", "x"), ("skipped: not an OpenCodeReview finding thread", "u2", "sourcery", "")]
        run.handled_earlier = [("skipped: already handled (applied)", "u3", "QC1", "")]
        text = run.summary()
        self.assertIn("- skipped: not an OpenCodeReview finding thread: [sourcery](u2)", text)
        self.assertIn("Owner signals already handled by an earlier run (1)", text)
        self.assertIn("- skipped: already handled (applied): [QC1](u3)", text)


class JsonAnswerTest(unittest.TestCase):
    # The shape of the failed #323 answer (run 41510928135): two objects of
    # the edit schema, separated by a blank line, which json.loads rejects
    # with "Extra data: line 3 column 1".
    TWO = ('{"edits": [{"start": 3, "end": 3, "replacement": "x"}], "kind": "behavior", "summary": "first"}\n\n'
           '{"edits": [], "kind": "mechanical", "summary": "second"}')

    def test_two_objects(self):
        with self.assertRaisesRegex(ValueError, "Extra data: line 3 column 1"):
            json.loads(self.TWO)
        self.assertEqual(a.parse_json_object(self.TWO)["summary"], "first")

    def test_fenced_and_surrounded(self):
        self.assertEqual(a.parse_json_object('```json\n{"summary": "s"}\n```'), {"summary": "s"})
        self.assertEqual(a.parse_json_object('```\n{"summary": "s"}\n```\n'), {"summary": "s"})
        self.assertEqual(a.parse_json_object('Here it is:\n{"summary": "s"}\nDone.'), {"summary": "s"})

    def test_garbage(self):
        for text in ("no json at all", '{"summary": ', "[1, 2]", "", None):
            with self.assertRaises(ValueError):
                a.parse_json_object(text)

    def test_chat_retries_once_then_fails(self):
        import io
        answers = []

        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        def fake_urlopen(request, timeout):
            content = answers.pop(0)
            requests.append(json.loads(request.data))
            return Response(json.dumps({"choices": [{"message": {"content": content}}],
                                        "usage": {"prompt_tokens": 10, "completion_tokens": 5}}).encode())
        original = a.urllib.request.urlopen
        a.urllib.request.urlopen = fake_urlopen
        saved_url = os.environ.get("OCR_APPLY_LLM_URL")
        os.environ["OCR_APPLY_LLM_URL"] = "https://llm.invalid/chat/completions"
        try:
            requests, answers[:] = [], ["sure, here: nothing", '```json\n{"summary": "ok"}\n```']
            spend = a.Spend(1.0)
            self.assertEqual(a.chat([{"role": "user", "content": "q"}], spend), {"summary": "ok"})
            self.assertEqual(spend.calls, 2)
            self.assertIn("exactly one JSON object", requests[1]["messages"][-1]["content"])
            requests, answers[:] = [], ["garbage", "still garbage"]
            with self.assertRaisesRegex(RuntimeError, "not after a retry"):
                a.chat([{"role": "user", "content": "q"}], a.Spend(1.0))
            requests, answers[:] = [], [self.TWO]
            self.assertEqual(a.chat([{"role": "user", "content": "q"}], a.Spend(1.0))["summary"], "first")
            self.assertEqual(len(requests), 1, "two objects need no retry")
        finally:
            a.urllib.request.urlopen = original
            if saved_url is None:
                os.environ.pop("OCR_APPLY_LLM_URL", None)
            else:
                os.environ["OCR_APPLY_LLM_URL"] = saved_url


class AlreadyAppliedTest(unittest.TestCase):
    """r4444039306 on #323 (run 41525959244): the finding's change was already
    on the branch (a suggestion-button commit), the edit was a no-op, and
    `git commit` failed with an empty stderr."""

    def setUp(self):
        import shutil
        import tempfile
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)
        shutil.copy(os.path.join(REPO_ROOT, ".clang-format"), self.dir)
        os.makedirs(os.path.join(self.dir, "src"))
        self.content = "int f(int x) {\n  validateSameLength(x);\n  return x;\n}\n"
        with open(os.path.join(self.dir, "src", "a.cpp"), "w") as f:
            f.write(self.content)
        for args in (["init", "-q"], ["config", "user.email", "t@t"], ["config", "user.name", "t"],
                     ["add", "-A"], ["commit", "-q", "-m", "base"]):
            a.git(self.dir, *args)
        self.run_ = a.Run.__new__(a.Run)
        self.run_.repo, self.run_.number, self.run_.repo_dir, self.run_.results = "o/r", 1, self.dir, []
        self.run_.applied, self.run_.proposals = [], []
        self.run_.kinds, self.run_.statements, self.run_.spend = a.load_kinds(), {}, a.Spend(1.0)
        self.task = a.Task(thread(rule="PP10", path="src/a.cpp", line=2), a.Action.IMPLEMENT,
                           a.Signal(a.Action.IMPLEMENT, "", "👍", "t"), "PP10")
        self.saved_format = os.environ.get("CLANG_FORMAT")
        os.environ["CLANG_FORMAT"] = "clang-format"

    def tearDown(self):
        if self.saved_format is None:
            os.environ.pop("CLANG_FORMAT", None)
        else:
            os.environ["CLANG_FORMAT"] = self.saved_format

    def head(self):
        return a.git(self.dir, "rev-parse", "HEAD")

    def test_no_op_edit(self):
        before = self.head()
        with self.assertRaises(a.AlreadyApplied):
            self.run_.commit_change(self.task, self.content, self.content, "m")
        self.assertEqual(self.head(), before, "no commit")

    def test_change_that_formatting_undoes(self):
        before = self.head()
        with self.assertRaises(a.AlreadyApplied):
            self.run_.commit_change(self.task, self.content, self.content.replace("return x;", "return  x ;"), "m")
        self.assertEqual(self.head(), before)
        self.assertEqual(a.git(self.dir, "status", "--porcelain"), "", "the file is as before")

    def test_real_change_is_committed(self):
        before = self.head()
        sha = self.run_.commit_change(self.task, self.content, self.content.replace("return x;", "return x + 1;"), "m")
        self.assertNotEqual(sha, before)

    def test_git_error_includes_stdout(self):
        with self.assertRaisesRegex(RuntimeError, "nothing to commit"):
            a.git(self.dir, "commit", "-q", "-m", "empty")

    def test_implement_no_op_is_reported_as_already_applied(self):
        # The model's edit reproduces what is already there (the #323 case).
        original_harness = a.harness
        a.harness = lambda: (lambda task, content, statement, spend: a.EditResult(
            [a.Edit("src/a.cpp", 2, 2, "  validateSameLength(x);")], "mechanical", "check the lengths", {}), None)
        posted = []
        saved = a.reply, a.resolve
        a.reply = lambda repo, number, thread, text, state: posted.append(state)
        a.resolve = lambda thread: posted.append("resolved")
        before = self.head()
        try:
            with self.assertRaises(a.AlreadyApplied):
                self.run_.implement(self.task)
            self.run_.already_applied(self.task)
        finally:
            a.harness = original_harness
            a.reply, a.resolve = saved
        self.assertEqual(self.head(), before)
        self.assertEqual(self.run_.applied, [])
        self.assertEqual(self.run_.results, [("already applied", "u", "PP10", "the change is already on the branch")])
        self.assertEqual(posted, [{"state": "applied", "via": "already on the branch"}, "resolved"])


A_BASE = "".join(f"int f{i}() {{ return {i}; }}\n" for i in range(1, 31))


def a_with(*changes):
    lines = A_BASE.splitlines(True)
    for i in changes:
        lines[i - 1] = f"int f{i}() {{ return {i} * 10; }}\n"
    return "".join(lines)


class SplitTest(unittest.TestCase):
    """`/split`: a PR (branch `feature`, PR #7 against `master`) changes
    src/a.cpp at two far-apart places (two hunks) and renames
    src/old_name.h to src/new_name.h without changing it, the IDM059 case of
    #323. A local bare repository stands in for marvin7122/qlever."""

    def setUp(self):
        import shutil
        import tempfile
        top = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, top)
        self.origin, self.dir = os.path.join(top, "origin.git"), os.path.join(top, "work")
        a.git(top, "init", "-q", "--bare", "-b", "master", self.origin)
        a.git(top, "clone", "-q", f"file://{self.origin}", self.dir)
        for args in (["config", "user.email", "t@t"], ["config", "user.name", "t"], ["switch", "-q", "-c", "master"]):
            a.git(self.dir, *args)
        shutil.copy(os.path.join(REPO_ROOT, ".clang-format"), self.dir)
        self.write("src/a.cpp", A_BASE)
        self.write("src/old_name.h", "#pragma once\nint g();\n")
        self.commit("base")
        a.git(self.dir, "push", "-q", "origin", "master")
        a.git(self.dir, "switch", "-q", "-c", "feature")
        self.write("src/a.cpp", a_with(3, 27))
        a.git(self.dir, "mv", "src/old_name.h", "src/new_name.h")
        self.commit("feature")
        a.git(self.dir, "push", "-q", "origin", "feature")
        self.files = self.pr_files()
        self.run_ = a.Run.__new__(a.Run)
        self.run_.repo, self.run_.number, self.run_.repo_dir, self.run_.results = "o/r", 7, self.dir, []
        self.run_.applied, self.run_.proposals, self.run_.splits = [], [], []
        self.run_.kinds, self.run_.statements, self.run_.spend = a.load_kinds(), {}, a.Spend(1.0)
        self.run_.head_ref = "feature"
        self.run_.pr = {"base": {"ref": "master", "repo": {"full_name": "o/r"}}}
        self.run_.fetch_pr_files = lambda: self.files
        self.opened = []
        self.run_.open_split_pr = lambda branch, title, body: self.opened.append((branch, title, body)) or (
            8, "https://github.com/o/r/pull/8")
        self.task = a.Task(thread(rule="IDM059", path="src/new_name.h", line=1), a.Action.SPLIT,
                           a.Signal(a.Action.SPLIT, "", "/split", "t"), "IDM059")
        self.posted = []
        self.saved = a.reply, a.resolve, a.chat, os.environ.get("CLANG_FORMAT")
        a.reply = lambda repo, number, thread, text, state: self.posted.append((text, state))
        a.resolve = lambda thread: self.posted.append("resolved")
        os.environ["CLANG_FORMAT"] = "clang-format"

    def tearDown(self):
        a.reply, a.resolve, a.chat, clang_format = self.saved
        if clang_format is None:
            os.environ.pop("CLANG_FORMAT", None)
        else:
            os.environ["CLANG_FORMAT"] = clang_format

    def write(self, path, content):
        os.makedirs(os.path.join(self.dir, os.path.dirname(path)), exist_ok=True)
        with open(os.path.join(self.dir, path), "w") as f:
            f.write(content)

    def read(self, path):
        full = os.path.join(self.dir, path)
        if not os.path.exists(full):
            return None
        with open(full) as f:
            return f.read()

    def commit(self, message):
        a.git(self.dir, "add", "-A")
        a.git(self.dir, "commit", "-q", "-m", message)

    def pr_files(self):
        """The PR's files as GitHub's API lists them (`patch` from the first `@@`)."""
        def patch(path):
            diff = a.git(self.dir, "diff", "master...feature", "--", path) + "\n"
            return diff[diff.index("@@"):].rstrip("\n")
        return [{"filename": "src/a.cpp", "status": "modified", "changes": 4, "patch": patch("src/a.cpp")},
                {"filename": "src/new_name.h", "previous_filename": "src/old_name.h", "status": "renamed",
                 "changes": 0}]

    def plan(self, files):
        a.chat = lambda messages, spend: {"files": files, "title": "Rename old_name.h", "summary": "An unrelated rename."}

    def apply_to(self, branch, patch, reverse=False):
        a.git(self.dir, "switch", "-q", "-f", branch)
        self.run_.git_apply(patch, reverse=reverse)

    # Extraction and validation.
    def test_hunks_of_a_patch(self):
        hunks = a.split_hunks(self.files[0]["patch"])
        self.assertEqual(len(hunks), 2)
        self.assertIn("return 3 * 10;", hunks[0])
        self.assertIn("return 27 * 10;", hunks[1])
        listing = a.describe_pr_files(self.files)
        self.assertIn("- renamed src/new_name.h (renamed from src/old_name.h)", listing)
        self.assertIn("hunk 2: @@", listing)

    def test_rename_is_extracted_and_applies_to_the_base(self):
        selected, problems = a.validate_split_plan({"files": [{"path": "src/new_name.h", "hunks": "all"}]}, self.files)
        self.assertEqual(problems, [])
        patch = a.build_split_patch(selected)
        self.assertIn("rename from src/old_name.h\nrename to src/new_name.h\n", patch)
        self.apply_to("master", patch)
        self.assertIsNone(self.read("src/old_name.h"))
        self.assertEqual(self.read("src/new_name.h"), "#pragma once\nint g();\n")
        self.assertEqual(self.read("src/a.cpp"), A_BASE, "only the rename")
        # The previous name finds the file as well.
        self.assertEqual(a.validate_split_plan({"files": [{"path": "src/old_name.h"}]}, self.files)[1], [])

    def test_partial_file_hunk(self):
        selected, problems = a.validate_split_plan({"files": [{"path": "src/a.cpp", "hunks": [2]}]}, self.files)
        self.assertEqual(problems, [])
        patch = a.build_split_patch(selected)
        self.apply_to("master", patch)
        self.assertEqual(self.read("src/a.cpp"), a_with(27), "the base plus the second hunk only")
        a.git(self.dir, "reset", "-q", "--hard")
        self.apply_to("feature", patch, reverse=True)
        self.assertEqual(self.read("src/a.cpp"), a_with(3), "the PR keeps the first hunk only")
        self.assertEqual(self.read("src/new_name.h"), "#pragma once\nint g();\n", "and the rename")

    def test_all_hunks_count_as_the_whole_file(self):
        selected, _ = a.validate_split_plan({"files": [{"path": "src/a.cpp", "hunks": [2, 1]}]}, self.files)
        self.assertEqual(selected[0][1], "all")

    def test_invalid_plans(self):
        def problems(*files):
            return a.validate_split_plan({"files": list(files)}, self.files)[1]
        self.assertIn("not a file of this pull request", problems({"path": "src/other.cpp"})[0])
        self.assertIn("not among its hunks 1-2", problems({"path": "src/a.cpp", "hunks": [3]})[0])
        self.assertIn("not among its hunks", problems({"path": "src/a.cpp", "hunks": []})[0])
        self.assertIn("only the whole file", problems({"path": "src/new_name.h", "hunks": [1]})[0])
        self.assertIn("chosen twice", problems({"path": "src/a.cpp"}, {"path": "src/a.cpp", "hunks": [1]})[0])
        self.assertEqual(problems(), ["the plan chooses no change"])
        large = [{"filename": "big.cpp", "status": "modified", "changes": 9000}]
        self.assertIn("not available", a.validate_split_plan({"files": [{"path": "big.cpp"}]}, large)[1][0])

    def test_plan_is_retried_once_then_refused(self):
        answers = [{"files": [{"path": "nope.cpp"}]}, {"files": [{"path": "src/new_name.h"}], "title": "t"}]
        a.chat = lambda messages, spend: answers.pop(0)
        _, selected = self.run_.plan_split(a.SplitRequest(self.task, "", False), self.files)
        self.assertEqual(selected[0][0]["filename"], "src/new_name.h")
        a.chat = lambda messages, spend: {"files": [{"path": "nope.cpp"}]}
        with self.assertRaisesRegex(RuntimeError, "does not fit the PR's diff"):
            self.run_.plan_split(a.SplitRequest(self.task, "", False), self.files)

    # The two steps and their sign-off.
    def test_split_keyword_and_signoff(self):
        def request(text):
            run = a.Run.__new__(a.Run)
            run.splits = []
            run.handle(a.Task(self.task.thread, a.Action.SPLIT, a.Signal(a.Action.SPLIT, text, "/split", "t"), "IDM059"))
            return run.splits[0]
        self.assertEqual((request("").title, request("").remove_now), ("", False))
        self.assertEqual((request("Rename the header").title, request("Rename the header").remove_now),
                         ("Rename the header", False))
        both = request("Rename the header /approve")
        self.assertEqual((both.title, both.remove_now), ("Rename the header", True))
        self.assertFalse(a.removal_approved("/approved later"))
        self.assertIs(a.reply_signal("/split Rename it", "t").action, a.Action.SPLIT)

    def test_free_text_split_request(self):
        # IDM059 on #323: "Please split this off into its own PR, open the PR, and link it here".
        original_harness = a.harness
        a.harness = lambda: (lambda task, content, statement, spend: a.EditResult(
            [], "behavior", "", {"split_request": True, "remove_requested": False}), None)
        self.run_.outdated = lambda task: False
        task = a.Task(self.task.thread, a.Action.IMPLEMENT, a.Signal(
            a.Action.IMPLEMENT, "Please split this off into its own PR, open the PR, and link it here", "reply", "t"),
            "IDM059")
        try:
            self.run_.implement(task)
        finally:
            a.harness = original_harness
        self.assertEqual([(s.task, s.title, s.remove_now) for s in self.run_.splits], [(task, "", False)])
        self.assertEqual(self.posted, [], "no 'no change' reply")

    def test_split_opens_the_pr_and_waits_for_approve(self):
        self.plan([{"path": "src/new_name.h", "hunks": "all"}])
        head = a.git(self.dir, "rev-parse", "HEAD")
        self.run_.split(a.SplitRequest(self.task, "", False))
        # The new PR's branch: the base plus exactly the rename.
        branch, title, body = self.opened[0]
        self.assertEqual((branch, title), ("split/7-1", "Rename old_name.h"))
        a.git(self.dir, "fetch", "-q", "origin")
        self.assertEqual(a.git(self.dir, "diff", "--name-status", "-M", "origin/master", "origin/split/7-1"),
                         "R100\tsrc/old_name.h\tsrc/new_name.h")
        # This PR is unchanged; the removal waits on its own branch.
        self.assertEqual(a.git(self.dir, "rev-parse", "origin/feature"), head)
        self.assertEqual(a.git(self.dir, "rev-parse", "HEAD"), head)
        text, state = self.posted[0]
        self.assertEqual(len(self.posted), 1, "not resolved yet")
        self.assertEqual(state["state"], "proposed")
        self.assertEqual(state["split"]["pr"], 8)
        self.assertIn("reply `/approve`", text)
        self.assertEqual(self.run_.results[0][0], "split: #8 (opened), removed from this PR: awaiting /approve")
        # `/approve`: the removal is cherry-picked like any approved proposal.
        task = a.Task(self.task.thread, a.Action.APPROVE, a.Signal(a.Action.APPROVE, "", "/approve", "t"), "IDM059",
                      proposal=state)
        self.run_.approve(task)
        self.assertEqual(self.read("src/old_name.h"), "#pragma once\nint g();\n")
        self.assertIsNone(self.read("src/new_name.h"))
        self.assertEqual(self.run_.applied[0].outcome, "split: #8 (opened), removed from this PR: yes")
        self.assertIn("the change is in #8", self.run_.applied[0].body("0" * 40))

    def test_split_with_approve_removes_at_once(self):
        self.plan([{"path": "src/a.cpp", "hunks": [2]}])
        self.run_.split(a.SplitRequest(self.task, "Scale f27", True))
        self.assertEqual(self.opened[0][1], "Scale f27", "the owner's title wins")
        a.git(self.dir, "fetch", "-q", "origin")
        self.assertEqual(a.git(self.dir, "show", "origin/split/7-1:src/a.cpp") + "\n", a_with(27))
        self.assertEqual(a.git(self.dir, "show", "origin/feature:src/a.cpp") + "\n", a_with(3))
        text, state = self.posted[0]
        self.assertEqual((state["state"], self.posted[1]), ("applied", "resolved"))
        self.assertEqual(self.run_.results[0][0], "split: #8 (opened), removed from this PR: yes")

    def test_change_that_does_not_apply_to_the_base(self):
        # The base moved on: the split branch is not created, nothing is opened.
        a.git(self.dir, "switch", "-q", "master")
        self.write("src/a.cpp", A_BASE.replace("return 27;", "return 270;"))
        self.commit("base moved")
        a.git(self.dir, "push", "-q", "origin", "master")
        a.git(self.dir, "switch", "-q", "feature")
        self.plan([{"path": "src/a.cpp", "hunks": [2]}])
        with self.assertRaisesRegex(RuntimeError, "does not apply to `master`"):
            self.run_.split(a.SplitRequest(self.task, "", False))
        self.assertEqual(self.opened, [])
        self.assertNotIn("split/7-1", a.git(self.dir, "branch", "--list"))
        self.assertEqual(a.git(self.dir, "rev-parse", "--abbrev-ref", "HEAD"), "feature")

    def test_pr_body(self):
        selected, _ = a.validate_split_plan({"files": [{"path": "src/new_name.h"}, {"path": "src/a.cpp", "hunks": [2]}]},
                                            self.files)
        body = a.split_pr_body("o/r", 7, "https://github.com/o/r/pull/7#discussion_r1", "IDM059",
                               "An unrelated rename.", selected)
        self.assertEqual(body, (
            "Split off from #7 at the owner's request in [this review thread]"
            "(https://github.com/o/r/pull/7#discussion_r1) (IDM059).\n\nAn unrelated rename.\n\n"
            "Contains, taken unchanged from #7:\n"
            "- `src/new_name.h` (renamed from `src/old_name.h`)\n"
            "- `src/a.cpp`, hunk 2\n\n"
            "Opened by the apply-replies flow of marvin7122/qlever; #7 keeps the change until its owner approves "
            "removing it there.\n"))


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
        # An unset repository variable reaches the script as an empty string.
        os.environ["OCR_APPLY_AGENT"] = ""
        try:
            self.assertEqual(a.harness(), (a.search_replace_harness, a.search_replace_rewrite))
        finally:
            del os.environ["OCR_APPLY_AGENT"]

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

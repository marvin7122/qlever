#!/usr/bin/env python3
# Copyright 2026, University of Freiburg,
# Chair of Algorithms and Data Structures.
# Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

"""Tests for the W3C export equivalence oracle in check_export_equivalence.py.

Run with:  python3 -m unittest misc.test_check_export_equivalence
or:        python3 misc/test_check_export_equivalence.py
"""
import os
import sys
import tempfile
import unittest
from unittest import mock

try:
    from misc.check_export_equivalence import main
except ModuleNotFoundError:
    from check_export_equivalence import main


def _write(tmpdir: str, name: str, content: str) -> str:
    path = os.path.join(tmpdir, name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    return path


class EquivalenceOracleTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    # --- RDF graph equivalence (Turtle) ---
    def test_turtle_isomorphic_reordered_and_bnodes(self):
        a = _write(self._tmp.name, "a.ttl", """
@prefix ex: <http://ex.org/> .
ex:a ex:p ex:b .
_:x ex:p _:y .
ex:b ex:q "literal" .
""")
        b = _write(self._tmp.name, "b.ttl", """
@prefix ex: <http://ex.org/> .
ex:b ex:q "literal" .
_:n1 ex:p _:n2 .
ex:a ex:p ex:b .
""")
        self.assertEqual(main(["--format", "turtle", a, b]), 0)

    def test_turtle_not_equivalent(self):
        a = _write(self._tmp.name, "a.ttl", """
@prefix ex: <http://ex.org/> .
ex:a ex:p ex:b .
ex:b ex:q "literal" .
""")
        c = _write(self._tmp.name, "c.ttl", """
@prefix ex: <http://ex.org/> .
ex:a ex:p ex:b .
ex:b ex:q "different" .
""")
        self.assertEqual(main(["--format", "turtle", a, c]), 1)

    def test_ntriples_equivalent_bnode_iso(self):
        a = _write(self._tmp.name, "a.nt",
                   "<http://ex.org/a> <http://ex.org/p> _:g1 .\n"
                   "_:g1 <http://ex.org/q> <http://ex.org/b> .\n")
        b = _write(self._tmp.name, "b.nt",
                   "_:h5 <http://ex.org/q> <http://ex.org/b> .\n"
                   "<http://ex.org/a> <http://ex.org/p> _:h5 .\n")
        self.assertEqual(main(["--format", "ntriples", a, b]), 0)

    # --- Solution multiset equivalence (TSV) ---
    def test_tsv_equivalent_multiset_order_and_bnodes(self):
        a = _write(self._tmp.name, "a.tsv", "?s\t?p\t?o\n<a>\t<p>\t<o>\n_:g1\t<p>\t\"v\"\n<a>\t<p>\t<o>\n")
        b = _write(self._tmp.name, "b.tsv", "?s\t?p\t?o\n<a>\t<p>\t<o>\n<a>\t<p>\t<o>\n_:z9\t<p>\t\"v\"\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 0)

    def test_tsv_different_multiset(self):
        a = _write(self._tmp.name, "a.tsv", "?s\t?p\t?o\n<a>\t<p>\t<o>\n<a>\t<p>\t<o>\n")
        b = _write(self._tmp.name, "b.tsv", "?s\t?p\t?o\n<a>\t<p>\t<o>\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 1)

    # --- CSV solution multiset ---
    def test_csv_equivalent(self):
        a = _write(self._tmp.name, "a.csv", "?s,?p,?o\n<a>,<p>,<o>\n_:g1,<p>,\"v\"\n")
        b = _write(self._tmp.name, "b.csv", "?s,?p,?o\n_:q7,<p>,\"v\"\n<a>,<p>,<o>\n")
        self.assertEqual(main(["--format", "csv", a, b]), 0)

    def test_tsv_multi_bnode_row_swap(self):
        # First-use labels plus a positional Counter would fail here.
        a = _write(self._tmp.name, "a.tsv", "?s\t?o\n_:x\t\"foo\"\n_:y\t\"bar\"\n")
        b = _write(self._tmp.name, "b.tsv", "?s\t?o\n_:a\t\"bar\"\n_:b\t\"foo\"\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 0)

    def test_tsv_column_reorder(self):
        a = _write(self._tmp.name, "a.tsv", "?s\t?o\n<a>\t<o>\n")
        b = _write(self._tmp.name, "b.tsv", "?o\t?s\n<o>\t<a>\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 0)

    def test_tsv_empty_vs_header_only(self):
        a = _write(self._tmp.name, "a.tsv", "")
        b = _write(self._tmp.name, "b.tsv", "?s\t?o\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 1)

    def test_turtle_duplicate_triples_are_a_set(self):
        a = _write(self._tmp.name, "a.ttl",
                   "<http://ex/s> <http://ex/p> <http://ex/o> .\n"
                   "<http://ex/s> <http://ex/p> <http://ex/o> .\n")
        b = _write(self._tmp.name, "b.ttl",
                   "<http://ex/s> <http://ex/p> <http://ex/o> .\n")
        self.assertEqual(main(["--format", "turtle", a, b]), 0)

    # --- SELECT term types and multiset cardinality ---
    def test_tsv_integer_differs_from_plain_literal(self):
        a = _write(self._tmp.name, "a.tsv", "?x\n1\n")
        b = _write(self._tmp.name, "b.tsv", "?x\n\"1\"\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 1)

    def test_tsv_literal_containing_scheme_is_not_an_iri(self):
        a = _write(self._tmp.name, "a.tsv", "?x\n\"a://b\"\n")
        b = _write(self._tmp.name, "b.tsv", "?x\n<a://b>\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 1)

    def test_tsv_cell_with_several_triples_is_rejected(self):
        a = _write(self._tmp.name, "a.tsv",
                   "?x\n\"a\" . <urn:x> <urn:y> <urn:z>\n")
        b = _write(self._tmp.name, "b.tsv", "?x\n\"a\"\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 2)

    def test_tsv_bare_word_is_rejected(self):
        a = _write(self._tmp.name, "a.tsv", "?x\nfoo\n")
        self.assertEqual(main(["--format", "tsv", a, a]), 2)

    def test_all_unbound_solutions_keep_cardinality(self):
        a = _write(self._tmp.name, "a.tsv", "?x\n\n")
        b = _write(self._tmp.name, "b.tsv", "?x\n\n\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 1)
        self.assertEqual(main(["--format", "tsv", a, a]), 0)
        c = _write(self._tmp.name, "c.csv", "x\n\n")
        d = _write(self._tmp.name, "d.csv", "x\n\n\n")
        self.assertEqual(main(["--format", "csv", c, d]), 1)

    def test_csv_compares_lexical_forms(self):
        a = _write(self._tmp.name, "a.csv", "x,y\nhttp://ex/a,1\n")
        b = _write(self._tmp.name, "b.csv", "y,x\n1,http://ex/a\n")
        c = _write(self._tmp.name, "c.csv", "x,y\nhttp://ex/a,2\n")
        self.assertEqual(main(["--format", "csv", a, b]), 0)
        self.assertEqual(main(["--format", "csv", a, c]), 1)

    # --- Header handling ---
    def test_dollar_and_question_mark_variables_match(self):
        a = _write(self._tmp.name, "a.tsv", "?x\n<a>\n")
        b = _write(self._tmp.name, "b.tsv", "$x\n<a>\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 0)

    def test_header_whitespace_and_bom_are_ignored(self):
        a = _write(self._tmp.name, "a.csv", "\ufeff?s, ?o\n<a>,<b>\n")
        b = _write(self._tmp.name, "b.csv", "?s,?o\n<a>,<b>\n")
        self.assertEqual(main(["--format", "csv", a, b]), 0)

    def test_crlf_tsv_matches_lf_tsv(self):
        a = _write(self._tmp.name, "a.tsv", "?s\t?o\r\n<a>\t<b>\r\n")
        b = _write(self._tmp.name, "b.tsv", "?s\t?o\n<a>\t<b>\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 0)

    def test_duplicate_header_variable_is_rejected(self):
        a = _write(self._tmp.name, "a.csv", "?x,?x\n<a>,<a>\n")
        b = _write(self._tmp.name, "b.csv", "?x\n<a>\n")
        self.assertEqual(main(["--format", "csv", a, b]), 2)

    def test_empty_header_is_rejected(self):
        a = _write(self._tmp.name, "a.tsv", "\n")
        b = _write(self._tmp.name, "b.tsv", "?x\n")
        self.assertEqual(main(["--format", "tsv", a, b]), 2)

    # --- Error handling ---
    def test_non_utf8_input_is_a_read_error(self):
        path = os.path.join(self._tmp.name, "a.tsv")
        with open(path, "wb") as fh:
            fh.write(b"?x\n\"\xff\"\n")
        self.assertEqual(main(["--format", "tsv", path, path]), 2)

    def test_missing_rdflib_is_a_usage_error(self):
        a = _write(self._tmp.name, "a.tsv", "?x\n<a>\n")
        with mock.patch.dict(sys.modules, {"rdflib": None}):
            self.assertEqual(main(["--format", "tsv", a, a]), 2)

    def test_unknown_format(self):
        self.assertEqual(main(["--format", "xml", "a", "b"]), 2)

    def test_wrong_arg_count(self):
        self.assertEqual(main(["a", "b", "c"]), 2)

    def test_missing_file(self):
        self.assertEqual(
            main(["--format", "tsv", "/nonexistent/a.tsv", "/nonexistent/b.tsv"]),
            2,
        )


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# Copyright 2026, University of Freiburg,
# Chair of Algorithms and Data Structures.
# Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

"""Tests for the W3C export equivalence oracle in check_export_equivalence.py.

Run with:  python3 -m unittest misc.test_check_export_equivalence
or:        python3 misc/test_check_export_equivalence.py
"""
import os
import tempfile
import unittest

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

    # --- Error handling ---
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

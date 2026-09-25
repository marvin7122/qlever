#!/usr/bin/env python3
# Copyright 2026, University of Freiburg,
# Chair of Algorithms and Data Structures.
# Author: Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR

"""
Export equivalence helper for QLever CONSTRUCT and SELECT outputs.

This is not a substitute for a byte-identity check. Graph isomorphism
of CONSTRUCT results is a different predicate from byte identity of an
order-preserving stream.

CONSTRUCT (turtle, ntriples)
  The result is an RDF graph, which is a set of triples. Comparison uses
  rdflib.compare.isomorphic. Duplicate triples are collapsed. A bug that
  emits the same triple twice will still pass.

SELECT (tsv, csv)
  The result is a multiset of solution mappings, keyed by variable name.
  Column order does not matter. Blank nodes shared across rows are
  compared by encoding each result as an RDF graph (one blank node per
  solution, predicates are variable names) and running isomorphism.
  An empty file is not equivalent to a header-only file.

  TSV cells are parsed as RDF terms in Turtle syntax, as the SPARQL 1.1
  TSV format prescribes, so `1` (xsd:integer) and `"1"` (plain literal)
  are different values. The SPARQL 1.1 CSV format is lossy by design:
  IRIs, literals and numbers are all written as bare lexical forms. CSV
  cells are therefore compared by their lexical form only (blank node
  labels excepted), which is the strongest equivalence CSV can express.

Exit status: 0 equivalent, 1 different, 2 usage or parse error.

Usage:
  check_export_equivalence.py --format turtle FILE_A FILE_B
  check_export_equivalence.py --format ntriples FILE_A FILE_B
  check_export_equivalence.py --format tsv FILE_A FILE_B
  check_export_equivalence.py --format csv FILE_A FILE_B

Requires rdflib for every format.
"""
import csv
import re
import sys
from typing import List, Optional, Sequence, Tuple

_BNODE_LABEL = re.compile(r"^_:[A-Za-z0-9_\-]+$")
_VAR_NS = "urn:qlever:var:"
# Every solution carries this marker, so that an all-unbound solution
# still contributes a triple and the multiset cardinality is preserved.
_SOLUTION_MARKER = "urn:qlever:solution"
# rdflib parser names for the CLI formats.
_RDFLIB_FORMAT = {"turtle": "turtle", "ntriples": "nt"}


class MalformedInput(ValueError):
    """The input is not a well-formed SELECT result."""


def _is_bnode(term: str) -> bool:
    return _BNODE_LABEL.match(term) is not None


def _normalize_var(name: str) -> str:
    name = name.strip()
    return name[1:] if name.startswith(("?", "$")) else name


def _load_table(path: str, fmt: str) -> Tuple[Optional[Tuple[str, ...]],
                                              List[Tuple[str, ...]]]:
    """Return (header, rows). header is None when the file is empty.

    A blank line is kept as a solution: for a single projected variable it
    is the solution in which that variable is unbound."""
    # `utf-8-sig` drops a leading byte order mark, if any.
    with open(path, encoding="utf-8-sig", newline="") as fh:
        if fmt == "csv":
            rows = [tuple(row) if row else ("",) for row in csv.reader(fh)]
        else:
            # SPARQL TSV has no CSV-style quoting (tabs and newlines inside
            # literals are escaped as in Turtle), so a plain split is exact.
            text = fh.read()
            lines = text.split("\n")
            if lines and lines[-1] == "":
                lines.pop()
            rows = [tuple(line.rstrip("\r").split("\t")) for line in lines]
    if not rows:
        return None, []
    return rows[0], rows[1:]


def _header_variables(header: Sequence[str]) -> List[str]:
    variables = [_normalize_var(name) for name in header]
    if any(not name for name in variables):
        raise MalformedInput(f"empty variable name in header {list(header)}")
    if len(set(variables)) != len(variables):
        raise MalformedInput(f"duplicate variable in header {list(header)}")
    return variables


def _tsv_cell_to_term(cell: str):
    from rdflib import BNode, Graph, URIRef

    if len(cell) >= 2 and cell[0] == "<" and cell[-1] == ">":
        # Taken verbatim, so relative IRIs are not resolved against a base.
        return URIRef(cell[1:-1])
    graph = Graph()
    graph.parse(data=f"_:s <urn:qlever:cell> {cell} .\n", format="turtle")
    objects = list(graph.objects())
    if len(graph) != 1 or len(objects) != 1 or isinstance(objects[0], BNode):
        raise MalformedInput(f"cell is not a single RDF term: {cell!r}")
    return objects[0]


def _cell_to_term(cell: str, fmt: str):
    from rdflib import BNode, Literal

    if cell == "":
        return None
    if _is_bnode(cell):
        return BNode(cell[2:])
    if fmt == "csv":
        return Literal(cell)
    return _tsv_cell_to_term(cell)


def _solutions_to_graph(variables: Sequence[str],
                        rows: Sequence[Sequence[str]], fmt: str):
    from rdflib import BNode, Graph, URIRef

    graph = Graph()
    marker = URIRef(_SOLUTION_MARKER)
    for row in rows:
        if len(row) != len(variables):
            raise MalformedInput(
                f"row has {len(row)} cells, header has {len(variables)}")
        row_node = BNode()
        graph.add((row_node, marker, marker))
        for name, cell in zip(variables, row):
            term = _cell_to_term(cell, fmt)
            if term is None:
                continue
            graph.add((row_node, URIRef(_VAR_NS + name), term))
    return graph


def _check_graph(path_a: str, path_b: str, fmt: str) -> int:
    from rdflib import Graph
    from rdflib import compare

    try:
        graph_a = Graph().parse(path_a, format=_RDFLIB_FORMAT[fmt])
        graph_b = Graph().parse(path_b, format=_RDFLIB_FORMAT[fmt])
    except Exception as exc:
        sys.stderr.write(f"parse error: {exc}\n")
        return 2

    if compare.isomorphic(graph_a, graph_b):
        print("equivalent (RDF graphs are isomorphic; CONSTRUCT is a set, "
              "duplicate triples are collapsed)")
        return 0

    print("NOT equivalent (RDF graphs are not isomorphic)")
    diff_a = set(graph_a) - set(graph_b)
    diff_b = set(graph_b) - set(graph_a)
    for triple in list(diff_a)[:10]:
        print(f"  only in {path_a}: {triple}")
    for triple in list(diff_b)[:10]:
        print(f"  only in {path_b}: {triple}")
    return 1


def _check_solutions(path_a: str, path_b: str, fmt: str) -> int:
    from rdflib import compare

    try:
        header_a, rows_a = _load_table(path_a, fmt)
        header_b, rows_b = _load_table(path_b, fmt)
    except (OSError, UnicodeError, csv.Error) as exc:
        sys.stderr.write(f"read error: {exc}\n")
        return 2

    if header_a is None or header_b is None:
        print("NOT equivalent (empty file is not a SELECT result)")
        return 1

    try:
        vars_a = _header_variables(header_a)
        vars_b = _header_variables(header_b)
    except MalformedInput as exc:
        sys.stderr.write(f"parse error: {exc}\n")
        return 2

    if set(vars_a) != set(vars_b):
        print("NOT equivalent (projected variables differ)")
        print(f"  {path_a}: {sorted(vars_a)}")
        print(f"  {path_b}: {sorted(vars_b)}")
        return 1

    try:
        graph_a = _solutions_to_graph(vars_a, rows_a, fmt)
        graph_b = _solutions_to_graph(vars_b, rows_b, fmt)
    except (MalformedInput, SyntaxError) as exc:
        # rdflib reports a malformed TSV cell as `BadSyntax`, a `SyntaxError`.
        sys.stderr.write(f"parse error: {exc}\n")
        return 2

    if compare.isomorphic(graph_a, graph_b):
        print("equivalent (SELECT mappings are isomorphic)")
        return 0

    print("NOT equivalent (SELECT mappings differ)")
    print(f"  {path_a}: {len(rows_a)} solutions")
    print(f"  {path_b}: {len(rows_b)} solutions")
    return 1


def main(argv: List[str]) -> int:
    if len(argv) != 4 or argv[0] != "--format":
        sys.stderr.write(__doc__)
        return 2
    fmt = argv[1].lower()
    path_a, path_b = argv[2], argv[3]

    if fmt not in ("turtle", "ntriples", "tsv", "csv"):
        sys.stderr.write(f"unknown format: {fmt}\n")
        return 2
    try:
        import rdflib  # noqa: F401
    except ImportError as exc:
        sys.stderr.write(f"rdflib is required: {exc}\n")
        return 2

    if fmt in ("turtle", "ntriples"):
        return _check_graph(path_a, path_b, fmt)
    return _check_solutions(path_a, path_b, fmt)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

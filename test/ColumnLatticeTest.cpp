// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/export_v2/ColumnLattice.h"
#include "parser/SparqlParser.h"

using namespace ql::engine::export_v2;

namespace {

ParsedQuery parse(std::string_view queryStr) {
  return SparqlParser::parseQuery(nullptr, std::string(queryStr));
}

TEST(ColumnLatticeTest, ToStringFunction) {
  EXPECT_EQ(toString(ColumnLattice::Int), "Int");
  EXPECT_EQ(toString(ColumnLattice::Double), "Double");
  EXPECT_EQ(toString(ColumnLattice::Bool), "Bool");
  EXPECT_EQ(toString(ColumnLattice::Date), "Date");
  EXPECT_EQ(toString(ColumnLattice::GeoPoint), "GeoPoint");
  EXPECT_EQ(toString(ColumnLattice::Encoded), "Encoded");
  EXPECT_EQ(toString(ColumnLattice::Vocab), "Vocab");
  EXPECT_EQ(toString(ColumnLattice::Union), "Union");
  EXPECT_EQ(toString(ColumnLattice::Undef), "Undef");
}

TEST(ColumnLatticeTest, BindConstantYieldsTrivialLattice) {
  auto query = parse(
      "SELECT * WHERE { ?s ?p ?o . BIND(1 AS ?x) . BIND(1.5 AS ?y) . "
      "BIND(true AS ?b) }");
  EXPECT_EQ(latticeForVariable(query, Variable{"?x"}), ColumnLattice::Int);
  EXPECT_EQ(latticeForVariable(query, Variable{"?y"}), ColumnLattice::Double);
  EXPECT_EQ(latticeForVariable(query, Variable{"?b"}), ColumnLattice::Bool);
}

TEST(ColumnLatticeTest, ScanColumnIsUnion) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o }");
  EXPECT_EQ(latticeForVariable(query, Variable{"?s"}), ColumnLattice::Union);
  EXPECT_EQ(latticeForVariable(query, Variable{"?o"}), ColumnLattice::Union);
}

TEST(ColumnLatticeTest, NonConstantBindIsUnion) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o . BIND(?o + 1 AS ?x) }");
  EXPECT_EQ(latticeForVariable(query, Variable{"?x"}), ColumnLattice::Union);
}

TEST(ColumnLatticeTest, UnknownVariableIsUnion) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o }");
  EXPECT_EQ(latticeForVariable(query, Variable{"?missing"}),
            ColumnLattice::Union);
}

TEST(ColumnLatticeTest, CompileCombinesBindUnionAndUndef) {
  auto query = parse("SELECT * WHERE { ?s ?p ?o . BIND(1 AS ?x) }");
  QueryExecutionTree::ColumnIndicesAndTypes selectedColumns;
  selectedColumns.emplace_back(
      QueryExecutionTree::VariableAndColumnIndex{"?x", 0});
  selectedColumns.emplace_back(std::nullopt);
  selectedColumns.emplace_back(
      QueryExecutionTree::VariableAndColumnIndex{"?s", 1});

  const auto lattice = compileColumnLattice(query, selectedColumns);
  ASSERT_EQ(lattice.size(), 3u);
  EXPECT_EQ(lattice.at(0), ColumnLattice::Int);
  EXPECT_EQ(lattice.at(1), ColumnLattice::Undef);
  EXPECT_EQ(lattice.at(2), ColumnLattice::Union);
}

}  // namespace

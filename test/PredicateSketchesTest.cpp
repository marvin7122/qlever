// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include "./util/FileTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "index/PredicateSketches.h"

using namespace ql::index::stats;
using ad_utility::testing::VocabId;

namespace {
// Return `PredicateSketches` with two predicates: `VocabId(100)` with the
// subjects 0..999 and the single object `VocabId(5000)`, and `VocabId(101)`
// with the subjects 500..599 and the objects 0..99.
PredicateSketches makeSketches() {
  PredicateSketches sketches;
  for (uint64_t i = 0; i < 1000; ++i) {
    sketches.addTriple(VocabId(i), VocabId(100), VocabId(5000));
  }
  for (uint64_t i = 0; i < 100; ++i) {
    sketches.addTriple(VocabId(500 + i), VocabId(101), VocabId(i));
  }
  return sketches;
}
}  // namespace

// _____________________________________________________________________________
TEST(PredicateSketchesTest, addTripleAndGet) {
  auto sketches = makeSketches();
  EXPECT_EQ(sketches.numPredicates(), 2u);
  EXPECT_EQ(sketches.get(VocabId(102)), nullptr);

  const auto* first = sketches.get(VocabId(100));
  ASSERT_NE(first, nullptr);
  EXPECT_NEAR(static_cast<double>(first->subjects_.estimateCardinality()),
              1000.0, 50.0);
  EXPECT_EQ(first->objects_.estimateCardinality(), 1u);

  const auto* second = sketches.get(VocabId(101));
  ASSERT_NE(second, nullptr);
  EXPECT_NEAR(static_cast<double>(second->subjects_.estimateCardinality()),
              100.0, 5.0);
  EXPECT_NEAR(static_cast<double>(second->objects_.estimateCardinality()),
              100.0, 5.0);
}

// _____________________________________________________________________________
TEST(PredicateSketchesTest, predicateThatReappearsIsMerged) {
  // The index builder adds the triples in PSO order, but a predicate that
  // occurs again after another predicate must still be added to its own
  // sketches.
  PredicateSketches sketches;
  sketches.addTriple(VocabId(1), VocabId(100), VocabId(2));
  sketches.addTriple(VocabId(3), VocabId(101), VocabId(4));
  sketches.addTriple(VocabId(5), VocabId(100), VocabId(6));
  EXPECT_EQ(sketches.numPredicates(), 2u);
  EXPECT_EQ(sketches.get(VocabId(100))->subjects_.estimateCardinality(), 2u);
  EXPECT_EQ(sketches.get(VocabId(101))->subjects_.estimateCardinality(), 1u);
}

// _____________________________________________________________________________
TEST(PredicateSketchesTest, writeAndReadFile) {
  auto [filename, cleanup] = ad_utility::testing::filenameForTesting();
  auto sketches = makeSketches();
  sketches.writeToFile(filename.string());
  auto read = PredicateSketches::readFromFile(filename.string());
  EXPECT_EQ(read, sketches);
  EXPECT_EQ(read.numPredicates(), 2u);

  // An empty set of sketches survives the round trip as well.
  PredicateSketches empty;
  empty.writeToFile(filename.string());
  EXPECT_EQ(PredicateSketches::readFromFile(filename.string()).numPredicates(),
            0u);
}

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <cmath>
#include <vector>

#include "engine/cbo/JoinCardinalityEstimator.h"
#include "global/Id.h"
#include "index/HyperLogLogSketch.h"

using namespace ql::engine::cbo;
using namespace ql::index::stats;

namespace {

// Helper to compute q-error: max(estimated/actual, actual/estimated)
double computeQError(size_t estimated, size_t actual) {
  if (estimated == 0 && actual == 0) {
    return 1.0;
  }
  double estD = std::max(1.0, static_cast<double>(estimated));
  double actD = std::max(1.0, static_cast<double>(actual));
  return std::max(estD / actD, actD / estD);
}

}  // namespace

// _____________________________________________________________________________
TEST(JoinCardinalityEstimatorTest, EmptyInputsAndBoundaryConditions) {
  HyperLogLogSketch<10> emptyA;
  HyperLogLogSketch<10> emptyB;
  HyperLogLogSketch<10> nonEmpty;

  for (uint64_t i = 0; i < 1000; ++i) {
    nonEmpty.insert(Id::fromBits(i + 1));
  }

  // Both empty
  EXPECT_EQ(0u, JoinCardinalityEstimator<10>::estimateJoinSize(
                    emptyA, 0, emptyB, 0, EstimationModel::CONTAINMENT_MIN));
  EXPECT_EQ(
      0u, JoinCardinalityEstimator<10>::estimateJoinSize(
              emptyA, 0, emptyB, 0, EstimationModel::HLL_INCLUSION_EXCLUSION));

  // One empty, one non-empty
  EXPECT_EQ(0u,
            JoinCardinalityEstimator<10>::estimateJoinSize(
                emptyA, 0, nonEmpty, 1000, EstimationModel::CONTAINMENT_MIN));
  EXPECT_EQ(0u, JoinCardinalityEstimator<10>::estimateJoinSize(
                    emptyA, 0, nonEmpty, 1000,
                    EstimationModel::HLL_INCLUSION_EXCLUSION));
  EXPECT_EQ(0u,
            JoinCardinalityEstimator<10>::estimateJoinSize(
                nonEmpty, 1000, emptyB, 0, EstimationModel::CONTAINMENT_MIN));
  EXPECT_EQ(0u, JoinCardinalityEstimator<10>::estimateJoinSize(
                    nonEmpty, 1000, emptyB, 0,
                    EstimationModel::HLL_INCLUSION_EXCLUSION));

  // Zero rows with non-empty sketch
  EXPECT_EQ(0u, JoinCardinalityEstimator<10>::estimateJoinSize(
                    nonEmpty, 0, nonEmpty, 1000,
                    EstimationModel::HLL_INCLUSION_EXCLUSION));
}

// _____________________________________________________________________________
TEST(JoinCardinalityEstimatorTest, IdenticalSetsUniformMultiplicity) {
  HyperLogLogSketch<10> sketchA;
  HyperLogLogSketch<10> sketchB;

  constexpr uint64_t NUM_KEYS = 20'000;
  for (uint64_t i = 0; i < NUM_KEYS; ++i) {
    Id id = Id::fromBits(i * 13 + 5);
    sketchA.insert(id);
    sketchB.insert(id);
  }

  constexpr size_t ROWS_A = NUM_KEYS;
  constexpr size_t ROWS_B = NUM_KEYS;
  constexpr size_t TRUE_JOIN_SIZE = NUM_KEYS;

  size_t estModelA = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, 1.0, sketchB, ROWS_B, 1.0,
      EstimationModel::CONTAINMENT_MIN);

  size_t estModelB = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, 1.0, sketchB, ROWS_B, 1.0,
      EstimationModel::HLL_INCLUSION_EXCLUSION);

  double qErrorA = computeQError(estModelA, TRUE_JOIN_SIZE);
  double qErrorB = computeQError(estModelB, TRUE_JOIN_SIZE);

  // For identical sets, both models should be accurate within 5%
  EXPECT_LE(qErrorA, 1.05);
  EXPECT_LE(qErrorB, 1.05);
}

// _____________________________________________________________________________
TEST(JoinCardinalityEstimatorTest, IdenticalSetsNonUnitMultiplicity) {
  HyperLogLogSketch<10> sketchA;
  HyperLogLogSketch<10> sketchB;

  constexpr uint64_t NUM_KEYS = 15'000;
  for (uint64_t i = 0; i < NUM_KEYS; ++i) {
    Id id = Id::fromBits(i * 7 + 11);
    sketchA.insert(id);
    sketchB.insert(id);
  }

  constexpr double MULT_A = 3.0;
  constexpr double MULT_B = 2.0;
  constexpr size_t ROWS_A = static_cast<size_t>(NUM_KEYS * MULT_A);  // 45,000
  constexpr size_t ROWS_B = static_cast<size_t>(NUM_KEYS * MULT_B);  // 30,000
  constexpr size_t TRUE_JOIN_SIZE =
      static_cast<size_t>(NUM_KEYS * MULT_A * MULT_B);  // 90,000

  size_t estModelA = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, MULT_A, sketchB, ROWS_B, MULT_B,
      EstimationModel::CONTAINMENT_MIN);

  size_t estModelB = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, MULT_A, sketchB, ROWS_B, MULT_B,
      EstimationModel::HLL_INCLUSION_EXCLUSION);

  EXPECT_LE(computeQError(estModelA, TRUE_JOIN_SIZE), 1.06);
  EXPECT_LE(computeQError(estModelB, TRUE_JOIN_SIZE), 1.06);
}

// _____________________________________________________________________________
TEST(JoinCardinalityEstimatorTest, PartiallyOverlappingSets50Percent) {
  HyperLogLogSketch<10> sketchA;
  HyperLogLogSketch<10> sketchB;

  // Set A: [0 .. 20,000)
  for (uint64_t i = 0; i < 20'000; ++i) {
    sketchA.insert(Id::fromBits(i));
  }

  // Set B: [10,000 .. 30,000) -> 10,000 keys overlap
  for (uint64_t i = 10'000; i < 30'000; ++i) {
    sketchB.insert(Id::fromBits(i));
  }

  constexpr size_t ROWS_A = 20'000;
  constexpr size_t ROWS_B = 20'000;
  constexpr size_t TRUE_JOIN_SIZE = 10'000;

  size_t estModelA = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, 1.0, sketchB, ROWS_B, 1.0,
      EstimationModel::CONTAINMENT_MIN);

  size_t estModelB = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, 1.0, sketchB, ROWS_B, 1.0,
      EstimationModel::HLL_INCLUSION_EXCLUSION);

  double qErrorA = computeQError(estModelA, TRUE_JOIN_SIZE);
  double qErrorB = computeQError(estModelB, TRUE_JOIN_SIZE);

  // Model A assumes min-containment -> predicts ~20,000 (2.0x error)
  EXPECT_GE(qErrorA, 1.90);
  EXPECT_LE(qErrorA, 2.15);

  // Model B uses HLL inclusion-exclusion -> predicts ~10,000 (q-error <= 1.15)
  EXPECT_LE(qErrorB, 1.15);

  // Verify Model B is substantially more accurate than Model A
  EXPECT_LT(qErrorB, qErrorA);
}

// _____________________________________________________________________________
TEST(JoinCardinalityEstimatorTest, PartiallyOverlappingSets10Percent) {
  HyperLogLogSketch<10> sketchA;
  HyperLogLogSketch<10> sketchB;

  // Set A: [0 .. 50,000)
  for (uint64_t i = 0; i < 50'000; ++i) {
    sketchA.insert(Id::fromBits(i));
  }

  // Set B: [45,000 .. 95,000) -> 5,000 keys overlap (10%)
  for (uint64_t i = 45'000; i < 95'000; ++i) {
    sketchB.insert(Id::fromBits(i));
  }

  constexpr size_t ROWS_A = 50'000;
  constexpr size_t ROWS_B = 50'000;
  constexpr size_t TRUE_JOIN_SIZE = 5'000;

  size_t estModelA = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, sketchB, ROWS_B, EstimationModel::CONTAINMENT_MIN);

  size_t estModelB = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, sketchB, ROWS_B,
      EstimationModel::HLL_INCLUSION_EXCLUSION);

  double qErrorA = computeQError(estModelA, TRUE_JOIN_SIZE);
  double qErrorB = computeQError(estModelB, TRUE_JOIN_SIZE);

  // Model A predicts ~50,000 (10x error)
  EXPECT_GE(qErrorA, 9.0);

  // Model B predicts ~5,000 (q-error <= 1.25)
  EXPECT_LE(qErrorB, 1.25);
  EXPECT_LT(qErrorB, qErrorA);
}

// _____________________________________________________________________________
TEST(JoinCardinalityEstimatorTest, CompletelyDisjointSets) {
  HyperLogLogSketch<10> sketchA;
  HyperLogLogSketch<10> sketchB;

  // Set A: [0 .. 40,000)
  for (uint64_t i = 0; i < 40'000; ++i) {
    sketchA.insert(Id::fromBits(i));
  }

  // Set B: [100,000 .. 140,000) (completely disjoint)
  for (uint64_t i = 100'000; i < 140'000; ++i) {
    sketchB.insert(Id::fromBits(i));
  }

  constexpr size_t ROWS_A = 40'000;
  constexpr size_t ROWS_B = 40'000;
  constexpr size_t TRUE_JOIN_SIZE = 0;

  size_t estModelA = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, sketchB, ROWS_B, EstimationModel::CONTAINMENT_MIN);

  size_t estModelB = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, sketchB, ROWS_B,
      EstimationModel::HLL_INCLUSION_EXCLUSION);

  // Model A fails catastrophically on disjoint sets, predicting ~40,000 rows
  EXPECT_GE(estModelA, 35'000u);

  // Model B detects zero overlap and returns 1 row floor for non-empty tables
  EXPECT_EQ(estModelB, 1u);

  // Distinct overlap query should return exactly 0 for Model B
  uint64_t distinctOverlap =
      JoinCardinalityEstimator<10>::estimateDistinctJoinKeys(
          sketchA, sketchB, EstimationModel::HLL_INCLUSION_EXCLUSION);
  EXPECT_EQ(distinctOverlap, 0u);
}

// _____________________________________________________________________________
TEST(JoinCardinalityEstimatorTest, SkewedMultiplicityFactDimensionJoin) {
  HyperLogLogSketch<10> sketchFact;
  HyperLogLogSketch<10> sketchDim;

  constexpr uint64_t NUM_ENTITIES = 5'000;
  for (uint64_t i = 0; i < NUM_ENTITIES; ++i) {
    Id id = Id::fromBits(i * 101 + 3);
    sketchFact.insert(id);
    sketchDim.insert(id);
  }

  // Fact table has 100,000 rows with average multiplicity 20.0
  constexpr size_t FACT_ROWS = 100'000;
  constexpr double FACT_MULT = 20.0;

  // Dimension table has 5,000 rows with multiplicity 1.0
  constexpr size_t DIM_ROWS = 5'000;
  constexpr double DIM_MULT = 1.0;

  constexpr size_t TRUE_JOIN_SIZE = 100'000;

  size_t estModelA = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchFact, FACT_ROWS, FACT_MULT, sketchDim, DIM_ROWS, DIM_MULT,
      EstimationModel::CONTAINMENT_MIN);

  size_t estModelB = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchFact, FACT_ROWS, FACT_MULT, sketchDim, DIM_ROWS, DIM_MULT,
      EstimationModel::HLL_INCLUSION_EXCLUSION);

  EXPECT_LE(computeQError(estModelA, TRUE_JOIN_SIZE), 1.05);
  EXPECT_LE(computeQError(estModelB, TRUE_JOIN_SIZE), 1.05);
}

// _____________________________________________________________________________
TEST(JoinCardinalityEstimatorTest, SkewedMultiWayJoinPipeline) {
  // Fact table A: 100,000 rows, 5,000 distinct entities (mult = 20)
  HyperLogLogSketch<10> sketchA;
  for (uint64_t i = 0; i < 5'000; ++i) {
    sketchA.insert(Id::fromBits(i));
  }
  constexpr size_t ROWS_A = 100'000;
  constexpr double MULT_A = 20.0;

  // Filter table C1: 100 entities that match subset of A
  HyperLogLogSketch<10> sketchC1;
  for (uint64_t i = 0; i < 100; ++i) {
    sketchC1.insert(Id::fromBits(i));
  }
  constexpr size_t ROWS_C1 = 100;
  constexpr double MULT_C1 = 1.0;
  constexpr size_t TRUE_JOIN_SIZE_C1 = 2'000;  // 100 matching * 20 mult

  // Filter table C2: 100 entities that are disjoint from A
  HyperLogLogSketch<10> sketchC2;
  for (uint64_t i = 50'000; i < 50'100; ++i) {
    sketchC2.insert(Id::fromBits(i));
  }
  constexpr size_t ROWS_C2 = 100;
  constexpr double MULT_C2 = 1.0;
  constexpr size_t TRUE_JOIN_SIZE_C2 = 0;

  // Join A with C1 (matching subset)
  size_t estModelB_C1 = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, MULT_A, sketchC1, ROWS_C1, MULT_C1,
      EstimationModel::HLL_INCLUSION_EXCLUSION);
  EXPECT_LE(computeQError(estModelB_C1, TRUE_JOIN_SIZE_C1), 1.25);

  // Join A with C2 (disjoint subset)
  size_t estModelA_C2 = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, MULT_A, sketchC2, ROWS_C2, MULT_C2,
      EstimationModel::CONTAINMENT_MIN);
  size_t estModelB_C2 = JoinCardinalityEstimator<10>::estimateJoinSize(
      sketchA, ROWS_A, MULT_A, sketchC2, ROWS_C2, MULT_C2,
      EstimationModel::HLL_INCLUSION_EXCLUSION);

  // Model A falsely predicts ~2,000 rows for disjoint join
  EXPECT_GE(estModelA_C2, 1'800u);

  // Model B correctly predicts 1 row floor for disjoint join
  EXPECT_EQ(estModelB_C2, 1u);
}

// _____________________________________________________________________________
TEST(JoinCardinalityEstimatorTest, StructuredResultAndDirectOverlapApi) {
  HyperLogLogSketch<10> sketchA;
  HyperLogLogSketch<10> sketchB;

  for (uint64_t i = 0; i < 10'000; ++i) {
    sketchA.insert(Id::fromBits(i));
  }
  for (uint64_t i = 5'000; i < 15'000; ++i) {
    sketchB.insert(Id::fromBits(i));
  }

  JoinEstimate result = JoinCardinalityEstimator<10>::estimateJoin(
      sketchA, 10'000, 1.0, sketchB, 10'000, 1.0,
      EstimationModel::HLL_INCLUSION_EXCLUSION);

  EXPECT_EQ(result.model, EstimationModel::HLL_INCLUSION_EXCLUSION);
  EXPECT_LE(computeQError(result.estimatedRows, 5'000), 1.15);
  EXPECT_LE(computeQError(result.distinctJoinKeys, 5'000), 1.15);

  uint64_t distinctKeysModelA =
      JoinCardinalityEstimator<10>::estimateDistinctJoinKeys(
          sketchA, sketchB, EstimationModel::CONTAINMENT_MIN);
  uint64_t distinctKeysModelB =
      JoinCardinalityEstimator<10>::estimateDistinctJoinKeys(
          sketchA, sketchB, EstimationModel::HLL_INCLUSION_EXCLUSION);

  EXPECT_LE(computeQError(distinctKeysModelA, 10'000), 1.05);
  EXPECT_LE(computeQError(distinctKeysModelB, 5'000), 1.15);
}

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "index/HyperLogLogSketch.h"

namespace ql::engine::cbo {

using ql::index::stats::HyperLogLogSketch;

// _____________________________________________________________________________
// Cardinality Estimation Strategy Model
enum class EstimationModel {
  CONTAINMENT_MIN,         // Model A: Min-containment heuristic
  HLL_INCLUSION_EXCLUSION  // Model B: HLL Inclusion-Exclusion Overlap
};

// _____________________________________________________________________________
// Structured outcome of join size estimation
struct JoinEstimate {
  size_t estimatedRows = 0;
  uint64_t distinctJoinKeys = 0;
  EstimationModel model = EstimationModel::HLL_INCLUSION_EXCLUSION;
};

// _____________________________________________________________________________
// Cost-Based Optimizer Join Cardinality Estimator
// Supports Model A (Containment Min) and Model B (HLL Inclusion-Exclusion)
// with SIMD-friendly HLL register union and overlap estimation.
template <size_t Precision = 10>
class JoinCardinalityEstimator {
 public:
  // Estimate join cardinality using explicit rows and average multiplicities.
  [[nodiscard]] static size_t estimateJoinSize(
      const HyperLogLogSketch<Precision>& sketchA, size_t rowsA, double multA,
      const HyperLogLogSketch<Precision>& sketchB, size_t rowsB, double multB,
      EstimationModel model =
          EstimationModel::HLL_INCLUSION_EXCLUSION) noexcept {
    return estimateJoin(sketchA, rowsA, multA, sketchB, rowsB, multB, model)
        .estimatedRows;
  }

  // Overload computing multiplicities automatically from row count and sketch
  // cardinality.
  [[nodiscard]] static size_t estimateJoinSize(
      const HyperLogLogSketch<Precision>& sketchA, size_t rowsA,
      const HyperLogLogSketch<Precision>& sketchB, size_t rowsB,
      EstimationModel model =
          EstimationModel::HLL_INCLUSION_EXCLUSION) noexcept {
    return estimateJoin(sketchA, rowsA, sketchB, rowsB, model).estimatedRows;
  }

  // Full estimation returning structured estimate (rows, distinct keys, model).
  [[nodiscard]] static JoinEstimate estimateJoin(
      const HyperLogLogSketch<Precision>& sketchA, size_t rowsA, double multA,
      const HyperLogLogSketch<Precision>& sketchB, size_t rowsB, double multB,
      EstimationModel model =
          EstimationModel::HLL_INCLUSION_EXCLUSION) noexcept {
    uint64_t cardA = sketchA.estimateCardinality();
    uint64_t cardB = sketchB.estimateCardinality();

    if (rowsA == 0 || rowsB == 0 || cardA == 0 || cardB == 0) {
      return {0, 0, model};
    }

    double effMultA =
        (multA > 0.0)
            ? multA
            : (static_cast<double>(rowsA) / static_cast<double>(cardA));
    double effMultB =
        (multB > 0.0)
            ? multB
            : (static_cast<double>(rowsB) / static_cast<double>(cardB));

    if (model == EstimationModel::CONTAINMENT_MIN) {
      // Model A: Assumes the smaller set of keys is a complete subset of the
      // larger set.
      uint64_t distinctKeys = std::min(cardA, cardB);
      uint64_t maxKeys = std::max(cardA, cardB);
      double estRows =
          (static_cast<double>(rowsA) * static_cast<double>(rowsB)) /
          static_cast<double>(maxKeys);
      size_t roundedRows =
          std::max<size_t>(1, static_cast<size_t>(std::round(estRows)));
      return {roundedRows, distinctKeys, model};
    }

    // Model B: Vectorized HLL register union and Inclusion-Exclusion overlap.
    HyperLogLogSketch<Precision> unionSketch = sketchA;
    unionSketch.merge(sketchB);
    uint64_t cardUnion = unionSketch.estimateCardinality();

    int64_t rawOverlap = static_cast<int64_t>(cardA) +
                         static_cast<int64_t>(cardB) -
                         static_cast<int64_t>(cardUnion);

    if (rawOverlap <= 0) {
      // Disjoint sets: 1 row minimum floor for non-empty tables to avoid
      // zero-cost anomalies
      return {1, 0, model};
    }

    uint64_t distinctOverlap = static_cast<uint64_t>(rawOverlap);
    double estRows = static_cast<double>(distinctOverlap) * effMultA * effMultB;
    size_t roundedRows =
        std::max<size_t>(1, static_cast<size_t>(std::round(estRows)));

    return {roundedRows, distinctOverlap, model};
  }

  // Overload for structured estimate using automatic multiplicities.
  [[nodiscard]] static JoinEstimate estimateJoin(
      const HyperLogLogSketch<Precision>& sketchA, size_t rowsA,
      const HyperLogLogSketch<Precision>& sketchB, size_t rowsB,
      EstimationModel model =
          EstimationModel::HLL_INCLUSION_EXCLUSION) noexcept {
    uint64_t cardA = sketchA.estimateCardinality();
    uint64_t cardB = sketchB.estimateCardinality();
    double multA =
        (cardA > 0) ? (static_cast<double>(rowsA) / static_cast<double>(cardA))
                    : 1.0;
    double multB =
        (cardB > 0) ? (static_cast<double>(rowsB) / static_cast<double>(cardB))
                    : 1.0;
    return estimateJoin(sketchA, rowsA, multA, sketchB, rowsB, multB, model);
  }

  // Compute distinct join key overlap directly.
  [[nodiscard]] static uint64_t estimateDistinctJoinKeys(
      const HyperLogLogSketch<Precision>& sketchA,
      const HyperLogLogSketch<Precision>& sketchB,
      EstimationModel model =
          EstimationModel::HLL_INCLUSION_EXCLUSION) noexcept {
    uint64_t cardA = sketchA.estimateCardinality();
    uint64_t cardB = sketchB.estimateCardinality();

    if (cardA == 0 || cardB == 0) {
      return 0;
    }

    if (model == EstimationModel::CONTAINMENT_MIN) {
      return std::min(cardA, cardB);
    }

    HyperLogLogSketch<Precision> unionSketch = sketchA;
    unionSketch.merge(sketchB);
    uint64_t cardUnion = unionSketch.estimateCardinality();

    int64_t rawOverlap = static_cast<int64_t>(cardA) +
                         static_cast<int64_t>(cardB) -
                         static_cast<int64_t>(cardUnion);
    return rawOverlap > 0 ? static_cast<uint64_t>(rawOverlap) : 0;
  }
};

}  // namespace ql::engine::cbo

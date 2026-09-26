// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <cmath>

#include "global/Id.h"
#include "index/HyperLogLogSketch.h"
#include "util/Serializer/ByteBufferSerializer.h"

using namespace ql::index::stats;

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, AccurateDistinctEstimation) {
  HyperLogLogSketch<10> hll;

  constexpr uint64_t EXACT_COUNT = 50'000;
  for (uint64_t i = 0; i < EXACT_COUNT; ++i) {
    hll.insert(Id::fromBits(i * 17 + 1));
  }

  uint64_t estimate = hll.estimateCardinality();

  // For p=10 (1024 registers), standard error is ~1.04 / sqrt(1024) = ~3.25%
  double relativeError = std::abs(static_cast<double>(estimate) -
                                  static_cast<double>(EXACT_COUNT)) /
                         static_cast<double>(EXACT_COUNT);

  EXPECT_LE(relativeError, 0.05);  // within 5%
}

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, SketchMergeCorrectness) {
  HyperLogLogSketch<10> hll1;
  HyperLogLogSketch<10> hll2;

  // Insert [0..20,000] into hll1
  for (uint64_t i = 0; i < 20'000; ++i) {
    hll1.insert(Id::fromBits(i));
  }

  // Insert [10,000..40,000] into hll2 (10,000 overlap)
  for (uint64_t i = 10'000; i < 40'000; ++i) {
    hll2.insert(Id::fromBits(i));
  }

  hll1.merge(hll2);

  uint64_t mergedEstimate = hll1.estimateCardinality();
  constexpr uint64_t TOTAL_DISTINCT = 40'000;

  double relativeError = std::abs(static_cast<double>(mergedEstimate) -
                                  static_cast<double>(TOTAL_DISTINCT)) /
                         static_cast<double>(TOTAL_DISTINCT);

  EXPECT_LE(relativeError, 0.05);
}

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, SmallAndLargePrecision) {
  // Relative estimation error for `numDistinct` distinct values.
  auto relativeError = [](auto sketch, uint64_t numDistinct) {
    for (uint64_t i = 0; i < numDistinct; ++i) {
      sketch.insert(Id::fromBits(i * 31 + 7));
    }
    return std::abs(static_cast<double>(sketch.estimateCardinality()) -
                    static_cast<double>(numDistinct)) /
           static_cast<double>(numDistinct);
  };
  // p = 4 (16 registers) uses the tabulated alpha; its standard error is
  // 1.04 / sqrt(16) = 26%, so only a loose bound is meaningful.
  EXPECT_LE(relativeError(HyperLogLogSketch<4>{}, 100'000), 0.75);
  // p = 16 (65536 registers): m * m does not fit into 32 bits; standard error
  // 0.4%.
  EXPECT_LE(relativeError(HyperLogLogSketch<16>{}, 1'000'000), 0.05);
}

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, SerializationAndEquality) {
  using namespace ad_utility::serialization;
  HyperLogLogSketch<10> sketch;
  for (uint64_t i = 0; i < 5'000; ++i) {
    sketch.insert(Id::fromBits(i));
  }
  EXPECT_FALSE(sketch == HyperLogLogSketch<10>{});

  ByteBufferWriteSerializer writer;
  writer << sketch;
  ByteBufferReadSerializer reader{std::move(writer).data()};
  HyperLogLogSketch<10> read;
  reader >> read;
  EXPECT_EQ(read, sketch);
  EXPECT_EQ(read.estimateCardinality(), sketch.estimateCardinality());

  // A sketch with a different number of registers cannot be read.
  ByteBufferWriteSerializer smallWriter;
  smallWriter << HyperLogLogSketch<4>{};
  ByteBufferReadSerializer smallReader{std::move(smallWriter).data()};
  EXPECT_ANY_THROW(smallReader >> read);
}

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cmath>

#include "global/Id.h"
#include "index/HyperLogLogSketch.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Views.h"

using namespace ql::index::stats;

// NOTE: The hash function of `HyperLogLogSketch` is deterministic, so every
// test below computes the same estimate on every run. The tolerances are about
// three standard errors (the standard error of a sketch with `m` registers is
// `1.04 / sqrt(m)`).

namespace {
// Return the `Id` with the bits `i * 31 + 7`. The stride spreads the keys over
// the lower bits, `HyperLogLogSketch` hashes the full bit pattern anyway.
Id makeKey(uint64_t i) { return Id::fromBits(i * 31U + 7U); }

// Insert the keys `makeKey(begin), ..., makeKey(end - 1)` into `sketch`.
template <size_t Precision>
void insertKeys(HyperLogLogSketch<Precision>& sketch, uint64_t begin,
                uint64_t end) {
  for (auto i : ql::views::iota(begin, end)) {
    sketch.insert(makeKey(i));
  }
}

// Return the relative error of the estimate of `sketch` for a set with
// `numDistinct` distinct keys.
template <size_t Precision>
double relativeErrorOfEstimate(const HyperLogLogSketch<Precision>& sketch,
                               uint64_t numDistinct) {
  auto estimate = static_cast<double>(sketch.estimateCardinality());
  auto exact = static_cast<double>(numDistinct);
  return std::abs(estimate - exact) / exact;
}

// Return a sketch with the precision `Precision` that contains `numDistinct`
// distinct keys.
template <size_t Precision>
HyperLogLogSketch<Precision> makeSketch(uint64_t numDistinct) {
  HyperLogLogSketch<Precision> sketch;
  insertKeys(sketch, 0, numDistinct);
  return sketch;
}
}  // namespace

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, emptyAndSingleElement) {
  HyperLogLogSketch<10> sketch;
  EXPECT_EQ(sketch.estimateCardinality(), 0U);
  // Linear counting with one nonzero register yields exactly 1.
  sketch.insert(makeKey(42));
  EXPECT_EQ(sketch.estimateCardinality(), 1U);
  // Inserting the same key again does not change the estimate.
  sketch.insert(makeKey(42));
  EXPECT_EQ(sketch.estimateCardinality(), 1U);
}

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, estimateWithDefaultPrecision) {
  // With 1024 registers, the standard error is 3.25 %.
  EXPECT_LE(relativeErrorOfEstimate(makeSketch<10>(50'000), 50'000), 0.1);
  // 200'000 keys are far beyond the linear counting range (`2.5 * 1024`), so
  // this checks the raw HyperLogLog estimate.
  EXPECT_LE(relativeErrorOfEstimate(makeSketch<10>(200'000), 200'000), 0.1);
}

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, estimateWithOtherPrecisions) {
  // 16, 32, and 64 registers use the tabulated bias correction. Their standard
  // errors are 26 %, 18 %, and 13 %, so only loose bounds are meaningful.
  EXPECT_LE(relativeErrorOfEstimate(makeSketch<4>(100'000), 100'000), 0.8);
  EXPECT_LE(relativeErrorOfEstimate(makeSketch<5>(100'000), 100'000), 0.6);
  EXPECT_LE(relativeErrorOfEstimate(makeSketch<6>(100'000), 100'000), 0.4);
  // 4096 registers: standard error 1.6 %.
  EXPECT_LE(relativeErrorOfEstimate(makeSketch<12>(20'000), 20'000), 0.05);
  // 65536 registers: `m * m` exceeds 32 bits; standard error 0.4 %.
  EXPECT_LE(relativeErrorOfEstimate(makeSketch<16>(1'000'000), 1'000'000),
            0.05);
}

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, merge) {
  // The key ranges [0, 20'000) and [10'000, 40'000) overlap in 10'000 keys, so
  // their union has 40'000 distinct keys.
  HyperLogLogSketch<10> lowerKeys;
  insertKeys(lowerKeys, 0, 20'000);
  HyperLogLogSketch<10> upperKeys;
  insertKeys(upperKeys, 10'000, 40'000);
  const auto upperKeysBeforeMerge = upperKeys;
  lowerKeys.merge(upperKeys);
  EXPECT_LE(relativeErrorOfEstimate(lowerKeys, 40'000), 0.1);
  // The merged sketch equals the sketch of the union, and the argument of
  // `merge` is unchanged.
  HyperLogLogSketch<10> allKeys;
  insertKeys(allKeys, 0, 40'000);
  EXPECT_EQ(lowerKeys, allKeys);
  EXPECT_EQ(upperKeys, upperKeysBeforeMerge);

  // Merging an empty sketch or the sketch itself changes nothing.
  const auto beforeMerge = lowerKeys;
  lowerKeys.merge(HyperLogLogSketch<10>{});
  EXPECT_EQ(lowerKeys, beforeMerge);
  lowerKeys.merge(beforeMerge);
  EXPECT_EQ(lowerKeys, beforeMerge);
}

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, insertionOrderDoesNotMatter) {
  HyperLogLogSketch<10> ascending;
  insertKeys(ascending, 0, 5'000);
  HyperLogLogSketch<10> descending;
  for (auto i : ad_utility::integerRange(uint64_t{5'000})) {
    descending.insert(makeKey(4'999 - i));
  }
  EXPECT_EQ(ascending, descending);
}

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, distinguishesDatatypeBits) {
  // The hash covers the full bit pattern of an `Id` including the datatype
  // bits, so the same payload with three different datatypes counts as three
  // distinct keys.
  HyperLogLogSketch<10> sketch;
  for (auto i : ad_utility::integerRange(uint64_t{1'000})) {
    sketch.insert(Id::fromBits(i));
    sketch.insert(Id::fromBits((uint64_t{1} << 60) | i));
    sketch.insert(Id::fromBits((uint64_t{2} << 60) | i));
  }
  EXPECT_LE(relativeErrorOfEstimate(sketch, 3'000), 0.1);
}

// _____________________________________________________________________________
TEST(HyperLogLogSketchTest, serializationAndEquality) {
  using namespace ad_utility::serialization;
  const auto sketch = makeSketch<10>(5'000);
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

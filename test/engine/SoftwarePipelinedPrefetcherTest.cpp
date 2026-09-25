// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gmock/gmock.h>

#include <numeric>
#include <vector>

#include "engine/SoftwarePipelinedPrefetcher.h"

using qlever::SoftwarePipelinedPrefetcher;

namespace {
constexpr size_t PREFETCH_DISTANCE = 4;

// Addresses of the elements of `values`, in order.
std::vector<const int*> addressesOf(const std::vector<int>& values) {
  std::vector<const int*> addresses;
  addresses.reserve(values.size());
  for (const int& value : values) {
    addresses.push_back(&value);
  }
  return addresses;
}

// Run the prefetcher on `numElements` pointers and check that each pointer is
// passed to the consumer exactly once and in order.
void expectEachPointerConsumedOnceInOrder(size_t numElements) {
  std::vector<int> values(numElements);
  std::iota(values.begin(), values.end(), 0);
  const auto addresses = addressesOf(values);
  std::vector<const int*> consumed;
  SoftwarePipelinedPrefetcher<PREFETCH_DISTANCE>::processWithPrefetch(
      addresses,
      [&consumed](const int* element) { consumed.push_back(element); });
  EXPECT_THAT(consumed, ::testing::ElementsAreArray(addresses))
      << "numElements = " << numElements;
}
}  // namespace

// _____________________________________________________________________________
TEST(SoftwarePipelinedPrefetcher, ConsumesEachPointerOnceInOrder) {
  // Empty input, fewer, exactly as many and more elements than the prefetch
  // distance, the latter with and without a remainder.
  for (size_t numElements :
       {size_t{0}, size_t{1}, PREFETCH_DISTANCE - 1, PREFETCH_DISTANCE,
        PREFETCH_DISTANCE + 1, 10 * PREFETCH_DISTANCE, size_t{1001}}) {
    expectEachPointerConsumedOnceInOrder(numElements);
  }
}

// _____________________________________________________________________________
TEST(SoftwarePipelinedPrefetcher, DefaultDistanceReadsValues) {
  std::vector<int> values(1000);
  std::iota(values.begin(), values.end(), 0);
  const auto addresses = addressesOf(values);
  long sum = 0;
  SoftwarePipelinedPrefetcher<>::processWithPrefetch(
      addresses, [&sum](const int* element) { sum += *element; });
  EXPECT_EQ(sum, 999 * 1000 / 2);
}

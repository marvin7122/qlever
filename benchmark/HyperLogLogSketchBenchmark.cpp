// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <chrono>
#include <cmath>
#include <iostream>
#include <unordered_set>
#include <vector>

#include "global/Id.h"
#include "index/HyperLogLogSketch.h"

using namespace ql::index::stats;

int main() {
  constexpr size_t NUM_ITEMS = 5'000'000;
  constexpr size_t DISTINCT_ITEMS = 500'000;

  std::cout << "=================================================================\n";
  std::cout << "Comparative Benchmark: Exact std::unordered_set vs HyperLogLog++ Sketch\n";
  std::cout << NUM_ITEMS << " stream items (" << DISTINCT_ITEMS << " distinct)\n";
  std::cout << "=================================================================\n";

  std::vector<Id> stream(NUM_ITEMS);
  for (size_t i = 0; i < NUM_ITEMS; ++i) {
    stream[i] = Id::fromBits((i % DISTINCT_ITEMS) * 31 + 7);
  }

  // 1. BASELINE: Exact std::unordered_set
  auto b0 = std::chrono::high_resolution_clock::now();
  std::unordered_set<uint64_t> exactSet;
  for (size_t i = 0; i < NUM_ITEMS; ++i) {
    exactSet.insert(stream[i].getBits());
  }
  uint64_t exactCount = exactSet.size();
  auto b1 = std::chrono::high_resolution_clock::now();
  double exactMs = std::chrono::duration<double, std::milli>(b1 - b0).count();
  size_t exactMemoryBytes = exactSet.bucket_count() * sizeof(void*) + exactSet.size() * (sizeof(uint64_t) + sizeof(void*));

  // 2. PROTOTYPE: HyperLogLog++ Sketch (1 KB)
  auto p0 = std::chrono::high_resolution_clock::now();
  HyperLogLogSketch<10> hll;  // 1024 bytes
  for (size_t i = 0; i < NUM_ITEMS; ++i) {
    hll.insert(stream[i]);
  }
  uint64_t hllEstimate = hll.estimateCardinality();
  auto p1 = std::chrono::high_resolution_clock::now();
  double hllMs = std::chrono::duration<double, std::milli>(p1 - p0).count();
  size_t hllMemoryBytes = 1024;  // Exactly 1 KB

  double relError = std::abs(static_cast<double>(hllEstimate) - static_cast<double>(DISTINCT_ITEMS)) /
                    static_cast<double>(DISTINCT_ITEMS) * 100.0;

  std::cout << "\n--- Baseline: Exact std::unordered_set ---\n";
  std::cout << "Runtime: " << exactMs << " ms ("
            << (NUM_ITEMS / (exactMs / 1000.0)) / 1e6 << " M items/sec, distinct: " << exactCount << ")\n";
  std::cout << "Memory:  " << exactMemoryBytes / (1024 * 1024) << " MB\n";

  std::cout << "\n--- Prototype: HyperLogLog++ Sketch ---\n";
  std::cout << "Runtime: " << hllMs << " ms ("
            << (NUM_ITEMS / (hllMs / 1000.0)) / 1e6 << " M items/sec, estimate: "
            << hllEstimate << ", error: " << relError << "%)\n";
  std::cout << "Memory:  " << hllMemoryBytes / 1024 << " KB\n";

  std::cout << "\n=================================================================\n";
  std::cout << ">>> HLL Throughput Speedup: " << (exactMs / hllMs) << "x faster\n";
  std::cout << ">>> Memory Reduction:       " << (exactMemoryBytes / hllMemoryBytes) << "x smaller footprint\n";
  std::cout << "=================================================================\n";

  return 0;
}

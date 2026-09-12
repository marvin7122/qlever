// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <chrono>
#include <iostream>
#include <unordered_set>
#include <vector>

#include "engine/BlockedBloomFilter.h"
#include "global/Id.h"

using namespace ql::engine::filter;

int main() {
  constexpr size_t NUM_ELEMENTS = 2'000'000;
  std::cout << "=================================================================\n";
  std::cout << "Comparative Benchmark: Baseline (std::unordered_set) vs BlockedBloomFilter ("
            << NUM_ELEMENTS << " elements)\n";
  std::cout << "=================================================================\n";

  std::vector<Id> data(NUM_ELEMENTS);
  for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
    data[i] = Id::fromBits(i * 7 + 1);
  }

  // 1. BASELINE: std::unordered_set
  auto b0 = std::chrono::high_resolution_clock::now();
  std::unordered_set<uint64_t> baseSet;
  baseSet.reserve(NUM_ELEMENTS);
  for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
    baseSet.insert(data[i].getBits());
  }
  auto b1 = std::chrono::high_resolution_clock::now();
  double baseInsertMs = std::chrono::duration<double, std::milli>(b1 - b0).count();

  size_t baseHits = 0;
  auto b2 = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
    if (baseSet.contains(data[i].getBits())) {
      baseHits++;
    }
  }
  auto b3 = std::chrono::high_resolution_clock::now();
  double baseProbeMs = std::chrono::duration<double, std::milli>(b3 - b2).count();

  // 2. PROTOTYPE: BlockedBloomFilter (Cache-Line Aligned)
  auto p0 = std::chrono::high_resolution_clock::now();
  BlockedBloomFilter blockedFilter{NUM_ELEMENTS, 0.01};
  for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
    blockedFilter.insert(data[i]);
  }
  auto p1 = std::chrono::high_resolution_clock::now();
  double protoInsertMs = std::chrono::duration<double, std::milli>(p1 - p0).count();

  size_t protoHits = 0;
  auto p2 = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
    if (blockedFilter.contains(data[i])) {
      protoHits++;
    }
  }
  auto p3 = std::chrono::high_resolution_clock::now();
  double protoProbeMs = std::chrono::duration<double, std::milli>(p3 - p2).count();

  std::cout << "\n--- Baseline (std::unordered_set) ---\n";
  std::cout << "Insert Time: " << baseInsertMs << " ms ("
            << (NUM_ELEMENTS / (baseInsertMs / 1000.0)) / 1e6 << " M/s)\n";
  std::cout << "Probe Time:  " << baseProbeMs << " ms ("
            << (NUM_ELEMENTS / (baseProbeMs / 1000.0)) / 1e6 << " M/s, hits: " << baseHits << ")\n";

  std::cout << "\n--- Prototype (BlockedBloomFilter) ---\n";
  std::cout << "Insert Time: " << protoInsertMs << " ms ("
            << (NUM_ELEMENTS / (protoInsertMs / 1000.0)) / 1e6 << " M/s)\n";
  std::cout << "Probe Time:  " << protoProbeMs << " ms ("
            << (NUM_ELEMENTS / (protoProbeMs / 1000.0)) / 1e6 << " M/s, hits: " << protoHits << ")\n";

  std::cout << "\n=================================================================\n";
  std::cout << ">>> Insert Speedup: " << (baseInsertMs / protoInsertMs) << "x faster\n";
  std::cout << ">>> Probe Speedup:  " << (baseProbeMs / protoProbeMs) << "x faster\n";
  std::cout << "=================================================================\n";

  return 0;
}

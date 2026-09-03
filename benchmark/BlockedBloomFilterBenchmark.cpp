// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <chrono>
#include <iostream>
#include <vector>

#include "engine/BlockedBloomFilter.h"
#include "global/Id.h"

using namespace ql::engine::filter;

int main() {
  constexpr size_t NUM_ELEMENTS = 1'000'000;
  std::cout << "Benchmarking BlockedBloomFilter with " << NUM_ELEMENTS << " elements...\n";

  BlockedBloomFilter filter{NUM_ELEMENTS, 0.01};

  auto t0 = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
    filter.insert(Id::fromBits(i * 3 + 1));
  }
  auto t1 = std::chrono::high_resolution_clock::now();
  double insertMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
  std::cout << "Insert Throughput: " << (NUM_ELEMENTS / (insertMs / 1000.0)) / 1e6
            << " M elements/sec (" << insertMs << " ms)\n";

  size_t hits = 0;
  auto t2 = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
    if (filter.contains(Id::fromBits(i * 3 + 1))) {
      hits++;
    }
  }
  auto t3 = std::chrono::high_resolution_clock::now();
  double probeMs = std::chrono::duration<double, std::milli>(t3 - t2).count();
  std::cout << "Probe Throughput (Hits): " << (NUM_ELEMENTS / (probeMs / 1000.0)) / 1e6
            << " M elements/sec (" << probeMs << " ms, hits: " << hits << ")\n";

  return 0;
}

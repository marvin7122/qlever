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

#include "engine/SimdStreamCompactor.h"
#include "global/Id.h"

using namespace ql::engine::vector;

int main() {
  constexpr size_t NUM_ROWS = 10'000'000;
  std::cout << "Benchmarking SimdStreamCompactor with " << NUM_ROWS << " rows...\n";

  std::vector<Id> input(NUM_ROWS);
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    input[i] = Id::makeFromInt(static_cast<int>(i));
  }
  std::vector<Id> output(NUM_ROWS);

  // Measure scalar branching filter
  auto t0 = std::chrono::high_resolution_clock::now();
  size_t scalarCount = 0;
  for (size_t i = 0; i < NUM_ROWS; ++i) {
    if (input[i].getInt() % 2 == 0) {
      output[scalarCount++] = input[i];
    }
  }
  auto t1 = std::chrono::high_resolution_clock::now();
  double scalarMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
  std::cout << "Scalar Filter Throughput: " << (NUM_ROWS / (scalarMs / 1000.0)) / 1e6
            << " M rows/sec (" << scalarMs << " ms, matches: " << scalarCount << ")\n";

  // Measure branchless vector compaction
  auto t2 = std::chrono::high_resolution_clock::now();
  size_t vectorCount = SimdStreamCompactor::compact(input, output, [](Id id) {
    return id.getInt() % 2 == 0;
  });
  auto t3 = std::chrono::high_resolution_clock::now();
  double vectorMs = std::chrono::duration<double, std::milli>(t3 - t2).count();
  std::cout << "SIMD Compactor Throughput: " << (NUM_ROWS / (vectorMs / 1000.0)) / 1e6
            << " M rows/sec (" << vectorMs << " ms, matches: " << vectorCount << ")\n";

  std::cout << "Speedup: " << (scalarMs / vectorMs) << "x\n";

  return 0;
}

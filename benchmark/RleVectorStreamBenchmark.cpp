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

#include "engine/RleVectorStream.h"
#include "global/Id.h"

using namespace ql::engine::rle;

int main() {
  constexpr size_t NUM_DISTINCT = 1000;
  constexpr size_t RUN_LENGTH = 10000;
  constexpr size_t TOTAL_ROWS = NUM_DISTINCT * RUN_LENGTH;  // 10,000,000 rows

  std::cout << "Benchmarking RleVectorStream with " << TOTAL_ROWS << " uncompressed rows ("
            << NUM_DISTINCT << " runs)...\n";

  // Measure memory and allocation of uncompressed vector
  auto t0 = std::chrono::high_resolution_clock::now();
  std::vector<Id> uncompressed(TOTAL_ROWS);
  for (size_t i = 0; i < NUM_DISTINCT; ++i) {
    for (size_t j = 0; j < RUN_LENGTH; ++j) {
      uncompressed[i * RUN_LENGTH + j] = Id::makeFromInt(static_cast<int>(i));
    }
  }
  auto t1 = std::chrono::high_resolution_clock::now();
  double uncompressedMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
  std::cout << "Uncompressed Vector Generation: " << uncompressedMs << " ms ("
            << (TOTAL_ROWS * sizeof(Id)) / (1024 * 1024) << " MB RAM)\n";

  // Measure RLE stream creation
  auto t2 = std::chrono::high_resolution_clock::now();
  RleVectorStream rleStream;
  for (size_t i = 0; i < NUM_DISTINCT; ++i) {
    rleStream.append(Id::makeFromInt(static_cast<int>(i)), RUN_LENGTH);
  }
  auto t3 = std::chrono::high_resolution_clock::now();
  double rleMs = std::chrono::duration<double, std::milli>(t3 - t2).count();
  std::cout << "RLE Stream Generation: " << rleMs << " ms ("
            << (rleStream.numRuns() * sizeof(RleVectorStream::Run)) / 1024 << " KB RAM)\n";

  // Measure Late Materialization
  auto t4 = std::chrono::high_resolution_clock::now();
  std::vector<Id> materialized(TOTAL_ROWS);
  rleStream.materialize(materialized);
  auto t5 = std::chrono::high_resolution_clock::now();
  double materializeMs = std::chrono::duration<double, std::milli>(t5 - t4).count();
  std::cout << "Late Materialization Throughput: "
            << (TOTAL_ROWS / (materializeMs / 1000.0)) / 1e6 << " M rows/sec ("
            << materializeMs << " ms)\n";

  std::cout << "Memory Compression Ratio: "
            << static_cast<double>(TOTAL_ROWS * sizeof(Id)) /
                   (rleStream.numRuns() * sizeof(RleVectorStream::Run))
            << "x\n";

  return 0;
}

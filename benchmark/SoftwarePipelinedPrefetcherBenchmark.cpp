// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <chrono>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include "engine/SoftwarePipelinedPrefetcher.h"

using namespace ql::engine::prefetch;

int main() {
  constexpr size_t POOL_SIZE = 10'000'000;
  constexpr size_t ACCESSES = 2'000'000;

  std::cout << "Benchmarking SoftwarePipelinedPrefetcher with " << ACCESSES << " random lookups in " << POOL_SIZE << " ints...\n";

  std::vector<int> pool(POOL_SIZE);
  std::iota(pool.begin(), pool.end(), 1);

  std::mt19937_64 rng(42);
  std::uniform_int_distribution<size_t> dist(0, POOL_SIZE - 1);

  std::vector<const int*> ptrs(ACCESSES);
  for (size_t i = 0; i < ACCESSES; ++i) {
    ptrs[i] = &pool[dist(rng)];
  }

  // Measure without prefetching
  auto t0 = std::chrono::high_resolution_clock::now();
  uint64_t sumNoPrefetch = 0;
  for (size_t i = 0; i < ACCESSES; ++i) {
    sumNoPrefetch += *ptrs[i];
  }
  auto t1 = std::chrono::high_resolution_clock::now();
  double noPrefetchMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
  std::cout << "No-Prefetch Latency: " << noPrefetchMs << " ms ("
            << (ACCESSES / (noPrefetchMs / 1000.0)) / 1e6 << " M lookups/sec)\n";

  // Measure with software prefetching
  auto t2 = std::chrono::high_resolution_clock::now();
  uint64_t sumWithPrefetch = 0;
  SoftwarePipelinedPrefetcher<16>::processWithPrefetch(
      ptrs, ACCESSES, [&sumWithPrefetch](const int* ptr) {
        sumWithPrefetch += *ptr;
      });
  auto t3 = std::chrono::high_resolution_clock::now();
  double withPrefetchMs = std::chrono::duration<double, std::milli>(t3 - t2).count();
  std::cout << "Software-Prefetched Latency: " << withPrefetchMs << " ms ("
            << (ACCESSES / (withPrefetchMs / 1000.0)) / 1e6 << " M lookups/sec)\n";

  std::cout << "Speedup: " << (noPrefetchMs / withPrefetchMs) << "x\n";

  return (sumNoPrefetch == sumWithPrefetch) ? 0 : 1;
}

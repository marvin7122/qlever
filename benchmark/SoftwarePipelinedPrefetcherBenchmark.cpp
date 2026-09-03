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
#include <string>

#include "engine/SoftwarePipelinedPrefetcher.h"

using namespace ql::engine::prefetch;

int main() {
  std::mt19937_64 rng(42);

  // =========================================================================
  // Benchmark 1: Synthetic Random Access Microbenchmark (40 MB working set)
  // =========================================================================
  {
    constexpr size_t POOL_SIZE = 10'000'000;
    constexpr size_t ACCESSES = 2'000'000;

    std::cout << "=================================================================\n";
    std::cout << "Microbenchmark: Synthetic Random Lookups (40 MB working set)\n";
    std::cout << "=================================================================\n";

    std::vector<int> pool(POOL_SIZE);
    std::iota(pool.begin(), pool.end(), 1);

    std::uniform_int_distribution<size_t> dist(0, POOL_SIZE - 1);
    std::vector<const int*> ptrs(ACCESSES);
    for (size_t i = 0; i < ACCESSES; ++i) {
      ptrs[i] = &pool[dist(rng)];
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    uint64_t sumNoPrefetch = 0;
    for (size_t i = 0; i < ACCESSES; ++i) {
      sumNoPrefetch += *ptrs[i];
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    double noPrefetchMs = std::chrono::duration<double, std::milli>(t1 - t0).count();

    auto t2 = std::chrono::high_resolution_clock::now();
    uint64_t sumWithPrefetch = 0;
    SoftwarePipelinedPrefetcher<16>::processWithPrefetch(
        ptrs, ACCESSES, [&sumWithPrefetch](const int* ptr) {
          sumWithPrefetch += *ptr;
        });
    auto t3 = std::chrono::high_resolution_clock::now();
    double withPrefetchMs = std::chrono::duration<double, std::milli>(t3 - t2).count();

    std::cout << "No-Prefetch Latency:          " << noPrefetchMs << " ms ("
              << (ACCESSES / (noPrefetchMs / 1000.0)) / 1e6 << " M lookups/sec)\n";
    std::cout << "Software-Prefetched Latency:  " << withPrefetchMs << " ms ("
              << (ACCESSES / (withPrefetchMs / 1000.0)) / 1e6 << " M lookups/sec)\n";
    std::cout << "Speedup:                      " << (noPrefetchMs / withPrefetchMs) << "x\n\n";
  }

  // =========================================================================
  // Benchmark 2: Macrobenchmark — Out-of-Cache Hash Join Probe (64 MB Hash Table)
  // =========================================================================
  {
    constexpr size_t NUM_BUCKETS = 2'000'000;
    constexpr size_t NUM_PROBES = 2'000'000;

    std::cout << "=================================================================\n";
    std::cout << "Macrobenchmark A: Hash Join Probe Phase (64 MB Hash Table)\n";
    std::cout << "=================================================================\n";

    struct HashBucket {
      uint64_t key;
      uint64_t value;
      uint64_t payload[2]; // 32-byte bucket
    };

    std::vector<HashBucket> hashTable(NUM_BUCKETS);
    for (size_t i = 0; i < NUM_BUCKETS; ++i) {
      hashTable[i] = {i * 17 + 3, i * 2, {0, 0}};
    }

    std::uniform_int_distribution<size_t> bucketDist(0, NUM_BUCKETS - 1);
    std::vector<const HashBucket*> probeBuckets(NUM_PROBES);
    for (size_t i = 0; i < NUM_PROBES; ++i) {
      probeBuckets[i] = &hashTable[bucketDist(rng)];
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    uint64_t joinMatchesNoPrefetch = 0;
    for (size_t i = 0; i < NUM_PROBES; ++i) {
      joinMatchesNoPrefetch += probeBuckets[i]->value;
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    double noPrefetchMs = std::chrono::duration<double, std::milli>(t1 - t0).count();

    auto t2 = std::chrono::high_resolution_clock::now();
    uint64_t joinMatchesWithPrefetch = 0;
    SoftwarePipelinedPrefetcher<16>::processWithPrefetch(
        probeBuckets, NUM_PROBES, [&joinMatchesWithPrefetch](const HashBucket* bucket) {
          joinMatchesWithPrefetch += bucket->value;
        });
    auto t3 = std::chrono::high_resolution_clock::now();
    double withPrefetchMs = std::chrono::duration<double, std::milli>(t3 - t2).count();

    std::cout << "Baseline Hash Probe:          " << noPrefetchMs << " ms ("
              << (NUM_PROBES / (noPrefetchMs / 1000.0)) / 1e6 << " M probes/sec)\n";
    std::cout << "Prefetched Hash Probe (D=16): " << withPrefetchMs << " ms ("
              << (NUM_PROBES / (withPrefetchMs / 1000.0)) / 1e6 << " M probes/sec)\n";
    std::cout << "Speedup:                      " << (noPrefetchMs / withPrefetchMs) << "x\n\n";
  }

  // =========================================================================
  // Benchmark 3: Macrobenchmark — Batch Vocabulary String Resolution (128 MB Block)
  // =========================================================================
  {
    constexpr size_t VOCAB_BLOCK_SIZE = 128 * 1024 * 1024; // 128 MB
    constexpr size_t NUM_RESOLUTIONS = 1'000'000;

    std::cout << "=================================================================\n";
    std::cout << "Macrobenchmark B: Batch Vocabulary String Offset Resolution (128 MB)\n";
    std::cout << "=================================================================\n";

    std::vector<char> vocabBlock(VOCAB_BLOCK_SIZE, 'A');
    std::uniform_int_distribution<size_t> offsetDist(0, VOCAB_BLOCK_SIZE - 64);
    std::vector<const char*> stringOffsets(NUM_RESOLUTIONS);
    for (size_t i = 0; i < NUM_RESOLUTIONS; ++i) {
      stringOffsets[i] = &vocabBlock[offsetDist(rng)];
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    uint64_t charSumNoPrefetch = 0;
    for (size_t i = 0; i < NUM_RESOLUTIONS; ++i) {
      charSumNoPrefetch += static_cast<uint8_t>(*stringOffsets[i]);
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    double noPrefetchMs = std::chrono::duration<double, std::milli>(t1 - t0).count();

    auto t2 = std::chrono::high_resolution_clock::now();
    uint64_t charSumWithPrefetch = 0;
    SoftwarePipelinedPrefetcher<16>::processWithPrefetch(
        stringOffsets, NUM_RESOLUTIONS, [&charSumWithPrefetch](const char* ptr) {
          charSumWithPrefetch += static_cast<uint8_t>(*ptr);
        });
    auto t3 = std::chrono::high_resolution_clock::now();
    double withPrefetchMs = std::chrono::duration<double, std::milli>(t3 - t2).count();

    std::cout << "Baseline Vocab Lookup:        " << noPrefetchMs << " ms ("
              << (NUM_RESOLUTIONS / (noPrefetchMs / 1000.0)) / 1e6 << " M lookups/sec)\n";
    std::cout << "Prefetched Vocab Lookup (D=16): " << withPrefetchMs << " ms ("
              << (NUM_RESOLUTIONS / (withPrefetchMs / 1000.0)) / 1e6 << " M lookups/sec)\n";
    std::cout << "Speedup:                      " << (noPrefetchMs / withPrefetchMs) << "x\n";
    std::cout << "=================================================================\n";
  }

  return 0;
}

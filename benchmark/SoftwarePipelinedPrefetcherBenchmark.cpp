// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <algorithm>
#include <chrono>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "engine/SoftwarePipelinedPrefetcher.h"

using namespace ql::engine::prefetch;

template <typename T>
inline void escape(T&& val) {
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" : "+r,m"(val) : : "memory");
#endif
}

int main() {
  std::mt19937_64 rng(42);
  constexpr size_t NUM_REPS = 5;

  // =========================================================================
  // Benchmark 1: Synthetic Random Access Microbenchmark (200 MB working set)
  // =========================================================================
  {
    constexpr size_t POOL_SIZE =
        50'000'000;  // 200 MB (vastly exceeds 32 MB L3 cache)
    constexpr size_t ACCESSES = 20'000'000;  // 20 Million accesses per rep

    std::cout << "============================================================="
                 "====\n";
    std::cout
        << "Microbenchmark: Synthetic Random Lookups (200 MB working set, "
        << ACCESSES << " accesses x " << NUM_REPS << " reps)\n";
    std::cout << "============================================================="
                 "====\n";

    std::vector<int> pool(POOL_SIZE);
    std::iota(pool.begin(), pool.end(), 1);

    std::uniform_int_distribution<size_t> dist(0, POOL_SIZE - 1);
    std::vector<const int*> ptrs(ACCESSES);
    for (size_t i = 0; i < ACCESSES; ++i) {
      ptrs[i] = &pool[dist(rng)];
    }

    std::vector<double> noPrefetchTimes;
    std::vector<double> withPrefetchTimes;

    for (size_t rep = 0; rep < NUM_REPS; ++rep) {
      // Baseline
      auto t0 = std::chrono::high_resolution_clock::now();
      uint64_t sumNoPrefetch = 0;
      for (size_t i = 0; i < ACCESSES; ++i) {
        sumNoPrefetch += *ptrs[i];
        escape(sumNoPrefetch);
      }
      auto t1 = std::chrono::high_resolution_clock::now();
      noPrefetchTimes.push_back(
          std::chrono::duration<double, std::milli>(t1 - t0).count());

      // Software prefetch
      auto t2 = std::chrono::high_resolution_clock::now();
      uint64_t sumWithPrefetch = 0;
      SoftwarePipelinedPrefetcher<16>::processWithPrefetch(
          ptrs, ACCESSES, [&sumWithPrefetch](const int* ptr) {
            sumWithPrefetch += *ptr;
            escape(sumWithPrefetch);
          });
      auto t3 = std::chrono::high_resolution_clock::now();
      withPrefetchTimes.push_back(
          std::chrono::duration<double, std::milli>(t3 - t2).count());
    }

    std::sort(noPrefetchTimes.begin(), noPrefetchTimes.end());
    std::sort(withPrefetchTimes.begin(), withPrefetchTimes.end());

    double medNoPrefetch = noPrefetchTimes[NUM_REPS / 2];
    double medWithPrefetch = withPrefetchTimes[NUM_REPS / 2];

    std::cout << "Baseline Latency (Median):    " << medNoPrefetch << " ms ("
              << (ACCESSES / (medNoPrefetch / 1000.0)) / 1e6
              << " M lookups/sec)\n";
    std::cout << "Prefetched (D=16, Median):    " << medWithPrefetch << " ms ("
              << (ACCESSES / (medWithPrefetch / 1000.0)) / 1e6
              << " M lookups/sec)\n";
    std::cout << "Speedup (Median):             "
              << (medNoPrefetch / medWithPrefetch) << "x\n\n";
  }

  // =========================================================================
  // Benchmark 2: Macrobenchmark A — Out-of-Cache Hash Join Probe (160 MB Hash
  // Table)
  // =========================================================================
  {
    constexpr size_t NUM_BUCKETS = 5'000'000;  // 160 MB
    constexpr size_t NUM_PROBES = 20'000'000;  // 20 Million probes per rep

    std::cout << "============================================================="
                 "====\n";
    std::cout << "Macrobenchmark A: Hash Join Probe Phase (160 MB Hash Table, "
              << NUM_PROBES << " probes x " << NUM_REPS << " reps)\n";
    std::cout << "============================================================="
                 "====\n";

    struct HashBucket {
      uint64_t key;
      uint64_t value;
      uint64_t payload[2];  // 32-byte bucket
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

    std::vector<double> noPrefetchTimes;
    std::vector<double> withPrefetchTimes;

    for (size_t rep = 0; rep < NUM_REPS; ++rep) {
      auto t0 = std::chrono::high_resolution_clock::now();
      uint64_t joinMatchesNoPrefetch = 0;
      for (size_t i = 0; i < NUM_PROBES; ++i) {
        joinMatchesNoPrefetch += probeBuckets[i]->value;
        escape(joinMatchesNoPrefetch);
      }
      auto t1 = std::chrono::high_resolution_clock::now();
      noPrefetchTimes.push_back(
          std::chrono::duration<double, std::milli>(t1 - t0).count());

      auto t2 = std::chrono::high_resolution_clock::now();
      uint64_t joinMatchesWithPrefetch = 0;
      SoftwarePipelinedPrefetcher<16>::processWithPrefetch(
          probeBuckets, NUM_PROBES,
          [&joinMatchesWithPrefetch](const HashBucket* bucket) {
            joinMatchesWithPrefetch += bucket->value;
            escape(joinMatchesWithPrefetch);
          });
      auto t3 = std::chrono::high_resolution_clock::now();
      withPrefetchTimes.push_back(
          std::chrono::duration<double, std::milli>(t3 - t2).count());
    }

    std::sort(noPrefetchTimes.begin(), noPrefetchTimes.end());
    std::sort(withPrefetchTimes.begin(), withPrefetchTimes.end());

    double medNoPrefetch = noPrefetchTimes[NUM_REPS / 2];
    double medWithPrefetch = withPrefetchTimes[NUM_REPS / 2];

    std::cout << "Baseline Hash Probe (Median): " << medNoPrefetch << " ms ("
              << (NUM_PROBES / (medNoPrefetch / 1000.0)) / 1e6
              << " M probes/sec)\n";
    std::cout << "Prefetched (D=16, Median):    " << medWithPrefetch << " ms ("
              << (NUM_PROBES / (medWithPrefetch / 1000.0)) / 1e6
              << " M probes/sec)\n";
    std::cout << "Speedup (Median):             "
              << (medNoPrefetch / medWithPrefetch) << "x\n\n";
  }

  // =========================================================================
  // Benchmark 3: Macrobenchmark B — Batch Vocabulary String Resolution (256 MB
  // Block)
  // =========================================================================
  {
    constexpr size_t VOCAB_BLOCK_SIZE = 256 * 1024 * 1024;  // 256 MB
    constexpr size_t NUM_RESOLUTIONS =
        10'000'000;  // 10 Million lookups per rep

    std::cout << "============================================================="
                 "====\n";
    std::cout
        << "Macrobenchmark B: Batch Vocabulary String Resolution (256 MB, "
        << NUM_RESOLUTIONS << " lookups x " << NUM_REPS << " reps)\n";
    std::cout << "============================================================="
                 "====\n";

    std::vector<char> vocabBlock(VOCAB_BLOCK_SIZE, 'A');
    std::uniform_int_distribution<size_t> offsetDist(0, VOCAB_BLOCK_SIZE - 64);
    std::vector<const char*> stringOffsets(NUM_RESOLUTIONS);
    for (size_t i = 0; i < NUM_RESOLUTIONS; ++i) {
      stringOffsets[i] = &vocabBlock[offsetDist(rng)];
    }

    std::vector<double> noPrefetchTimes;
    std::vector<double> withPrefetchTimes;

    for (size_t rep = 0; rep < NUM_REPS; ++rep) {
      auto t0 = std::chrono::high_resolution_clock::now();
      uint64_t charSumNoPrefetch = 0;
      for (size_t i = 0; i < NUM_RESOLUTIONS; ++i) {
        charSumNoPrefetch += static_cast<uint8_t>(*stringOffsets[i]);
        escape(charSumNoPrefetch);
      }
      auto t1 = std::chrono::high_resolution_clock::now();
      noPrefetchTimes.push_back(
          std::chrono::duration<double, std::milli>(t1 - t0).count());

      auto t2 = std::chrono::high_resolution_clock::now();
      uint64_t charSumWithPrefetch = 0;
      SoftwarePipelinedPrefetcher<16>::processWithPrefetch(
          stringOffsets, NUM_RESOLUTIONS,
          [&charSumWithPrefetch](const char* ptr) {
            charSumWithPrefetch += static_cast<uint8_t>(*ptr);
            escape(charSumWithPrefetch);
          });
      auto t3 = std::chrono::high_resolution_clock::now();
      withPrefetchTimes.push_back(
          std::chrono::duration<double, std::milli>(t3 - t2).count());
    }

    std::sort(noPrefetchTimes.begin(), noPrefetchTimes.end());
    std::sort(withPrefetchTimes.begin(), withPrefetchTimes.end());

    double medNoPrefetch = noPrefetchTimes[NUM_REPS / 2];
    double medWithPrefetch = withPrefetchTimes[NUM_REPS / 2];

    std::cout << "Baseline Vocab Lookup (Median): " << medNoPrefetch << " ms ("
              << (NUM_RESOLUTIONS / (medNoPrefetch / 1000.0)) / 1e6
              << " M lookups/sec)\n";
    std::cout << "Prefetched (D=16, Median):      " << medWithPrefetch
              << " ms (" << (NUM_RESOLUTIONS / (medWithPrefetch / 1000.0)) / 1e6
              << " M lookups/sec)\n";
    std::cout << "Speedup (Median):               "
              << (medNoPrefetch / medWithPrefetch) << "x\n";
    std::cout << "============================================================="
                 "====\n";
  }

  return 0;
}

// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/SoftwarePipelinedPrefetcher.h"
#include "util/Exception.h"

namespace ad_benchmark {

namespace {
// Fixed seed, so that every run uses the same access pattern.
constexpr uint64_t RANDOM_SEED = 42;

// `numTargets` pointers to uniformly random elements of `pool`.
template <typename T>
std::vector<const T*> randomTargets(const std::vector<T>& pool,
                                    size_t numTargets) {
  std::mt19937_64 randomEngine{RANDOM_SEED};
  std::uniform_int_distribution<size_t> index{0, pool.size() - 1};
  std::vector<const T*> targets;
  targets.reserve(numTargets);
  for (size_t i = 0; i < numTargets; ++i) {
    targets.push_back(&pool[index(randomEngine)]);
  }
  return targets;
}

// Add to `group` the time to fold `readValue` over all `targets` with a plain
// loop and with `SoftwarePipelinedPrefetcher`. Both measurements start after a
// full pass over `targets`, so they start with the same cache state, and both
// sums are checked against each other.
template <typename T, typename ReadValue>
void addPlainAndPrefetchedMeasurements(ResultGroup& group,
                                       const std::vector<const T*>& targets,
                                       const ReadValue& readValue) {
  auto plainSum = [&targets, &readValue]() {
    int64_t sum = 0;
    for (const T* target : targets) {
      sum += readValue(*target);
    }
    return sum;
  };
  const int64_t expectedSum = plainSum();

  int64_t plainResult = 0;
  group.addMeasurement("Plain loop", [&] { plainResult = plainSum(); });
  AD_CORRECTNESS_CHECK(plainResult == expectedSum);

  int64_t prefetchedResult = 0;
  group.addMeasurement(
      "SoftwarePipelinedPrefetcher (distance 16)",
      [&targets, &readValue, &prefetchedResult] {
        int64_t sum = 0;
        qlever::SoftwarePipelinedPrefetcher<>::processWithPrefetch(
            targets,
            [&sum, &readValue](const T* target) { sum += readValue(*target); });
        prefetchedResult = sum;
      });
  AD_CORRECTNESS_CHECK(prefetchedResult == expectedSum);
}
}  // namespace

// Random reads through a pointer array into working sets that are much larger
// than a typical last-level cache, with and without software prefetching.
class SoftwarePipelinedPrefetcherBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final { return "SoftwarePipelinedPrefetcher"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    constexpr size_t NUM_TARGETS = 20'000'000;

    // 4-byte values, 50 M values = 200 MB.
    {
      constexpr size_t NUM_VALUES = 50'000'000;
      std::vector<int32_t> pool(NUM_VALUES);
      for (size_t i = 0; i < NUM_VALUES; ++i) {
        pool[i] = static_cast<int32_t>(i % 1'000);
      }
      auto& group = results.addGroup("20 M random 4-byte reads from 200 MB");
      group.metadata().addKeyValuePair("pool bytes",
                                       NUM_VALUES * sizeof(int32_t));
      addPlainAndPrefetchedMeasurements(
          group, randomTargets(pool, NUM_TARGETS),
          [](int32_t value) { return int64_t{value}; });
    }

    // 32-byte records whose key is compared to a value derived from the
    // payload, 5 M records = 160 MB.
    {
      struct Record {
        uint64_t key_;
        uint64_t value_;
        uint64_t padding_[2];
      };
      static_assert(sizeof(Record) == 32);
      constexpr size_t NUM_RECORDS = 5'000'000;
      std::vector<Record> records(NUM_RECORDS);
      for (size_t i = 0; i < NUM_RECORDS; ++i) {
        records[i] = Record{i, i % 2 == 0 ? i : i + 1, {0, 0}};
      }
      auto& group = results.addGroup("20 M random 32-byte records, 160 MB");
      group.metadata().addKeyValuePair("pool bytes",
                                       NUM_RECORDS * sizeof(Record));
      addPlainAndPrefetchedMeasurements(
          group, randomTargets(records, NUM_TARGETS), [](const Record& record) {
            return int64_t{record.key_ == record.value_};
          });
    }

    // Single bytes from a 256 MiB character block.
    {
      constexpr size_t BLOCK_BYTES = size_t{256} << 20;
      std::vector<char> block(BLOCK_BYTES);
      for (size_t i = 0; i < BLOCK_BYTES; ++i) {
        block[i] = static_cast<char>('a' + i % 26);
      }
      auto& group = results.addGroup("10 M random 1-byte reads from 256 MiB");
      group.metadata().addKeyValuePair("pool bytes", BLOCK_BYTES);
      addPlainAndPrefetchedMeasurements(
          group, randomTargets(block, NUM_TARGETS / 2), [](char character) {
            return int64_t{static_cast<unsigned char>(character)};
          });
    }
    return results;
  }
};
AD_REGISTER_BENCHMARK(SoftwarePipelinedPrefetcherBenchmark);
}  // namespace ad_benchmark

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/BlockedBloomFilter.h"
#include "global/Id.h"

namespace ad_benchmark {

// Comparative benchmark: baseline `std::unordered_set` vs `BlockedBloomFilter`
// for build-side insertion and probe-side membership checks.
class BlockedBloomFilterBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "Baseline std::unordered_set vs BlockedBloomFilter";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    constexpr size_t NUM_ELEMENTS = 2'000'000;
    std::vector<Id> data(NUM_ELEMENTS);
    for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
      data[i] = Id::fromBits(i * 7 + 1);
    }

    auto& baseline = results.addGroup("Baseline std::unordered_set");
    baseline.metadata().addKeyValuePair("elements", NUM_ELEMENTS);
    baseline.addMeasurement("Build set", [&data] {
      std::unordered_set<uint64_t> baseSet;
      baseSet.reserve(data.size());
      for (const Id& id : data) {
        baseSet.insert(id.getBits());
      }
    });
    baseline.addMeasurement("Probe set", [&data] {
      std::unordered_set<uint64_t> baseSet;
      baseSet.reserve(data.size());
      for (const Id& id : data) {
        baseSet.insert(id.getBits());
      }
      size_t hits = 0;
      for (const Id& id : data) {
        if (baseSet.find(id.getBits()) != baseSet.end()) {
          ++hits;
        }
      }
      // Print the result so the computation is not optimized away.
      std::cout << hits;
    });

    auto& filter = results.addGroup("BlockedBloomFilter");
    filter.metadata().addKeyValuePair("elements", NUM_ELEMENTS);
    filter.addMeasurement("Build filter", [&data] {
      ql::engine::filter::BlockedBloomFilter blockedFilter{data.size(), 0.01};
      for (const Id& id : data) {
        blockedFilter.insert(id);
      }
    });
    filter.addMeasurement("Probe filter", [&data] {
      ql::engine::filter::BlockedBloomFilter blockedFilter{data.size(), 0.01};
      for (const Id& id : data) {
        blockedFilter.insert(id);
      }
      size_t hits = 0;
      for (const Id& id : data) {
        if (blockedFilter.contains(id)) {
          ++hits;
        }
      }
      // Print the result so the computation is not optimized away.
      std::cout << hits;
    });

    return results;
  }
};
AD_REGISTER_BENCHMARK(BlockedBloomFilterBenchmark);
}  // namespace ad_benchmark

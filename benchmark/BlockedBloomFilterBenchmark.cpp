// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/BlockedBloomFilter.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/HashSet.h"

namespace ad_benchmark {

// Build and probe cost of an exact hash set (`ad_utility::HashSet`, the
// structure a hash join probes) versus a `BlockedBloomFilter`. The probe
// measurements use prebuilt structures, so they contain only the lookups. Half
// of the probed keys are present, half are absent.
class BlockedBloomFilterBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "ad_utility::HashSet vs BlockedBloomFilter";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    constexpr size_t NUM_ELEMENTS = 2'000'000;
    std::vector<Id> data;
    std::vector<Id> probes;
    data.reserve(NUM_ELEMENTS);
    probes.reserve(2 * NUM_ELEMENTS);
    for (uint64_t i = 0; i < NUM_ELEMENTS; ++i) {
      data.push_back(Id::fromBits(i * 7 + 1));
      probes.push_back(Id::fromBits(i * 7 + 1));
      probes.push_back(Id::fromBits(i * 7 + 2));
    }
    auto allocator = ad_utility::makeUnlimitedAllocator<Id>();

    auto buildSet = [&data] {
      ad_utility::HashSet<Id> set;
      set.reserve(data.size());
      for (const Id& id : data) {
        set.insert(id);
      }
      return set;
    };
    auto buildFilter = [&data, &allocator] {
      return ql::engine::filter::BlockedBloomFilter::createFromColumn(
          data, allocator, 0.01);
    };

    auto& baseline = results.addGroup("ad_utility::HashSet");
    baseline.metadata().addKeyValuePair("elements", NUM_ELEMENTS);
    baseline.addMeasurement("Build", [&buildSet] { (void)buildSet(); });
    auto set = buildSet();
    size_t setHits = 0;
    baseline.addMeasurement("Probe", [&set, &probes, &setHits] {
      setHits = 0;
      for (const Id& id : probes) {
        setHits += set.contains(id);
      }
    });
    AD_CORRECTNESS_CHECK(setHits == NUM_ELEMENTS);

    auto& filter = results.addGroup("BlockedBloomFilter");
    filter.metadata().addKeyValuePair("elements", NUM_ELEMENTS);
    filter.addMeasurement("Build", [&buildFilter] { (void)buildFilter(); });
    auto bloom = buildFilter();
    size_t filterHits = 0;
    filter.addMeasurement("Probe", [&bloom, &probes, &filterHits] {
      filterHits = 0;
      for (const Id& id : probes) {
        filterHits += bloom.contains(id);
      }
    });
    // No false negatives; the surplus over `NUM_ELEMENTS` are false positives.
    AD_CORRECTNESS_CHECK(filterHits >= NUM_ELEMENTS);
    filter.metadata().addKeyValuePair(
        "false positives among absent keys",
        static_cast<double>(filterHits - NUM_ELEMENTS) / NUM_ELEMENTS);

    return results;
  }
};
AD_REGISTER_BENCHMARK(BlockedBloomFilterBenchmark);
}  // namespace ad_benchmark

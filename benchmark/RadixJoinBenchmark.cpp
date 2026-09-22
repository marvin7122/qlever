// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

// Microbenchmark for `RadixPartitionedHashJoin::executeJoinCount` against
// two baselines on the same inputs: a hash-map count (the classic hash-join
// counting behavior) and a sort-plus-`equal_range` count without
// partitioning (the partitioning ablation). All arms must agree on the
// match count; any mismatch fails the run.
//
// Usage:
// RadixJoinBenchmark --rows N --groups G --skew 0|1 --reps R --warmup W
//                    [--only hashmap|sort|radix]
// Output: TSV lines `arm rep wall_ms matches` on stdout.

#include <absl/hash/hash.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "engine/RadixPartitionedHashJoin.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Allocator.h"
#include "util/Exception.h"
#include "util/HashMap.h"

namespace {

// _____________________________________________________________________________
struct Config {
  size_t rows = 1'000'000;
  size_t groups = 100'000;
  bool skewed = false;
  size_t reps = 7;
  size_t warmup = 2;
  std::string only;  // Empty runs all arms; otherwise one of them.
};

// _____________________________________________________________________________
Config parseArgs(int argc, char** argv) {
  Config config;
  for (int i = 1; i + 1 < argc; i += 2) {
    std::string key{argv[i]};
    std::string value{argv[i + 1]};
    if (key == "--rows") {
      config.rows = std::stoull(value);
    } else if (key == "--groups") {
      config.groups = std::stoull(value);
    } else if (key == "--skew") {
      config.skewed = value != "0";
    } else if (key == "--reps") {
      config.reps = std::stoull(value);
    } else if (key == "--warmup") {
      config.warmup = std::stoull(value);
    } else if (key == "--only") {
      config.only = value;
    } else {
      AD_THROW("Unknown argument: " + key);
    }
  }
  AD_CONTRACT_CHECK(config.rows > 0);
  AD_CONTRACT_CHECK(config.groups > 0);
  AD_CONTRACT_CHECK(config.reps > 0);
  return config;
}

// _____________________________________________________________________________
// Fill a single-column table with keys in [0, groups): uniformly shuffled,
// or skewed via squaring modulo `groups` (heavy head, long tail).
IdTable makeKeyTable(size_t rows, size_t groups, bool skewed, uint64_t seed) {
  IdTable table{1, qlever::makeUnlimitedAllocator<Id>()};
  table.reserve(rows);
  std::mt19937_64 engine{seed};
  for (size_t i = 0; i < rows; ++i) {
    uint64_t key = engine() % groups;
    if (skewed) {
      key = (key * key) % groups;
    }
    table.push_back({Id::makeFromInt(key)});
  }
  return table;
}

// _____________________________________________________________________________
// Classic hash-join counting: build key multiplicities, probe and sum.
size_t hashMapCount(const IdTable& left, const IdTable& right) {
  ad_utility::HashMap<Id, size_t, absl::Hash<Id>> multiplicities;
  multiplicities.reserve(left.numRows());
  for (size_t row = 0; row < left.numRows(); ++row) {
    ++multiplicities[left(row, 0)];
  }
  size_t matches = 0;
  for (size_t row = 0; row < right.numRows(); ++row) {
    auto it = multiplicities.find(right(row, 0));
    if (it != multiplicities.end()) {
      matches += it->second;
    }
  }
  return matches;
}

// _____________________________________________________________________________
// Sort-plus-`equal_range` count without partitioning (ablation arm).
size_t sortCount(const IdTable& left, const IdTable& right) {
  std::vector<Id> buildKeys;
  buildKeys.reserve(left.numRows());
  for (size_t row = 0; row < left.numRows(); ++row) {
    buildKeys.push_back(left(row, 0));
  }
  std::sort(buildKeys.begin(), buildKeys.end());
  size_t matches = 0;
  for (size_t row = 0; row < right.numRows(); ++row) {
    auto range =
        std::equal_range(buildKeys.begin(), buildKeys.end(), right(row, 0));
    matches += static_cast<size_t>(range.second - range.first);
  }
  return matches;
}

// _____________________________________________________________________________
using Clock = std::chrono::steady_clock;

// _____________________________________________________________________________
template <typename Func>
void runArm(const std::string& name, Func&& func, const IdTable& left,
            const IdTable& right, const Config& config, size_t expected) {
  for (size_t rep = 0; rep < config.warmup + config.reps; ++rep) {
    auto start = Clock::now();
    size_t matches = func(left, right);
    auto end = Clock::now();
    double wallMs =
        std::chrono::duration<double, std::milli>(end - start).count();
    if (rep >= config.warmup) {
      std::printf("%s\t%zu\t%.3f\t%zu\n", name.c_str(), rep - config.warmup,
                  wallMs, matches);
    }
    if (matches != expected) {
      AD_THROW("Count mismatch in arm " + name + ": expected " +
               std::to_string(expected) + ", got " + std::to_string(matches));
    }
  }
  std::fflush(stdout);
}

}  // namespace

// _____________________________________________________________________________
int main(int argc, char** argv) {
  using ql::engine::join::RadixPartitionedHashJoin;
  Config config = parseArgs(argc, argv);
  IdTable left = makeKeyTable(config.rows, config.groups, config.skewed, 42);
  IdTable right = makeKeyTable(config.rows, config.groups, config.skewed, 1337);
  // Reference count from the obviously-correct arm. Single-arm runs still
  // verify their count against it.
  size_t expected = hashMapCount(left, right);
  auto runIf = [&](const std::string& name, auto&& func) {
    if (config.only.empty() || config.only == name) {
      runArm(name, func, left, right, config, expected);
    }
  };
  runIf("hashmap", hashMapCount);
  runIf("sort", sortCount);
  runIf("radix", RadixPartitionedHashJoin<>::executeJoinCount);
  return 0;
}

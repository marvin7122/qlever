// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace ql::engine::join {

// _____________________________________________________________________________
// Radix-partitioned join counting:
// Partitions both input tables by radix hash and counts matches per partition
// with a sorted build run plus binary search, keeping each partition's
// working set small. Counts follow bag semantics: every pair of equal keys
// contributes one match.
template <size_t RadixBits = 6>  // 2^6 = 64 partitions
class RadixPartitionedHashJoin {
 public:
  static_assert(RadixBits < 8 * sizeof(size_t),
                "RadixBits must fit into a size_t shift");
  static constexpr size_t NUM_PARTITIONS = size_t{1} << RadixBits;
  static constexpr size_t RADIX_MASK = NUM_PARTITIONS - 1;

  // Simple, fast multiplicative hash for 64-bit Id integers.
  [[nodiscard]] static constexpr size_t getPartitionIndex(Id id) noexcept {
    uint64_t key = id.getBits();
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    return static_cast<size_t>(key & RADIX_MASK);
  }

  // Structure representing a single cache-resident partition bucket.
  struct PartitionBucket {
    std::vector<size_t> rowIndices;
  };

  // Partition an IdTable by join column into 2^RadixBits buckets.
  static std::vector<PartitionBucket> partitionTable(const IdTable& table,
                                                     size_t joinColumnIndex) {
    AD_CORRECTNESS_CHECK(joinColumnIndex < table.numColumns());
    std::vector<PartitionBucket> partitions(NUM_PARTITIONS);
    const size_t numRows = table.numRows();

    for (size_t row = 0; row < numRows; ++row) {
      size_t p = getPartitionIndex(table(row, joinColumnIndex));
      partitions[p].rowIndices.push_back(row);
    }
    return partitions;
  }

  // Count matches between two partitioned tables in cache-isolated loops.
  static size_t executeJoinCount(const IdTable& leftTable, size_t leftCol,
                                 const IdTable& rightTable, size_t rightCol) {
    auto leftPartitions = partitionTable(leftTable, leftCol);
    auto rightPartitions = partitionTable(rightTable, rightCol);

    size_t totalMatches = 0;

    for (size_t p = 0; p < NUM_PARTITIONS; ++p) {
      const auto& leftBucket = leftPartitions[p].rowIndices;
      const auto& rightBucket = rightPartitions[p].rowIndices;

      if (leftBucket.empty() || rightBucket.empty()) {
        continue;
      }

      // Build a sorted run of the left bucket's keys
      std::vector<Id> buildKeys;
      buildKeys.reserve(leftBucket.size());
      for (size_t lRow : leftBucket) {
        buildKeys.push_back(leftTable(lRow, leftCol));
      }
      std::sort(buildKeys.begin(), buildKeys.end());

      // Probe the right bucket; every equal key pair counts (bag semantics)
      for (size_t rRow : rightBucket) {
        Id probeKey = rightTable(rRow, rightCol);
        auto range =
            std::equal_range(buildKeys.begin(), buildKeys.end(), probeKey);
        totalMatches += static_cast<size_t>(
            std::distance(range.first, range.second));
      }
    }

    return totalMatches;
  }
};

}  // namespace ql::engine::join

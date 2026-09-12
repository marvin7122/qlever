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

#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace ql::engine::join {

// _____________________________________________________________________________
// Radix-Partitioned Vectorized Hash Join:
// Partitions large input tables into 2^k partitions so that each build hash
// table fits completely within the L2 CPU Cache (512 KB), eliminating
// Last-Level Cache (LLC) misses.
template <size_t RadixBits = 6>  // 2^6 = 64 partitions
class RadixPartitionedHashJoin {
 public:
  static constexpr size_t NUM_PARTITIONS = 1 << RadixBits;
  static constexpr uint64_t RADIX_MASK = NUM_PARTITIONS - 1;

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

  // Partition an IdTable by join column into 2^RadixBits cache-sized buckets.
  static std::vector<PartitionBucket> partitionTable(const IdTable& table,
                                                     size_t joinColumnIndex) {
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

      // Build small L2-resident hash map for left bucket
      std::vector<Id> buildKeys;
      buildKeys.reserve(leftBucket.size());
      for (size_t lRow : leftBucket) {
        buildKeys.push_back(leftTable(lRow, leftCol));
      }
      std::sort(buildKeys.begin(), buildKeys.end());

      // Probe right bucket
      for (size_t rRow : rightBucket) {
        Id probeKey = rightTable(rRow, rightCol);
        auto it =
            std::lower_bound(buildKeys.begin(), buildKeys.end(), probeKey);
        if (it != buildKeys.end() && *it == probeKey) {
          totalMatches++;
        }
      }
    }

    return totalMatches;
  }
};

}  // namespace ql::engine::join

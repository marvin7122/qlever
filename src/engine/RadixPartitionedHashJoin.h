// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <absl/hash/hash.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/VectorWithMemoryLimit.h"

namespace ql::engine::join {

// Count the matches of an equi-join between the join columns of two
// `IdTable`s, following bag semantics: every pair of equal keys contributes
// one match. Only the number of matches is computed; no result rows are
// materialized.
//
// Preconditions: `leftCol` and `rightCol` are valid column indices of their
// respective tables. Violations throw (via `AD_CONTRACT_CHECK`). All `Id`
// datatypes are supported: partitioning hashes each key with the same hash
// that `Id::operator==` is consistent with, so equal keys always land in the
// same partition.
template <size_t RadixBits = 6>  // 2^6 = 64 partitions
class RadixPartitionedHashJoin {
 public:
  static_assert(RadixBits < std::numeric_limits<size_t>::digits,
                "RadixBits must fit into a size_t shift");
  static constexpr size_t NUM_PARTITIONS = size_t{1} << RadixBits;
  static constexpr size_t RADIX_MASK = NUM_PARTITIONS - 1;

  // Return the partition index for a join key. The hash is consistent with
  // `Id::operator==` for all datatypes (including `LocalVocabIndex`), so
  // equal keys always share a partition.
  [[nodiscard]] static size_t getPartitionIndex(Id id) {
    return absl::Hash<Id>{}(id)&RADIX_MASK;
  }

  // A single partition bucket. The join keys are stored directly (rather
  // than row indices), so the build phase needs no indirection back into
  // the table. All memory is charged against the memory limit of the
  // partitioned table.
  struct PartitionBucket {
    explicit PartitionBucket(
        const ad_utility::AllocatorWithLimit<Id>& allocator)
        : keys(allocator) {}
    ad_utility::VectorWithMemoryLimit<Id> keys;
  };

  // Partition the join column of `table` into `NUM_PARTITIONS` buckets.
  static std::vector<PartitionBucket> partitionTable(const IdTable& table,
                                                     ColumnIndex joinColumn) {
    AD_CONTRACT_CHECK(joinColumn < table.numColumns(),
                      "joinColumn=", joinColumn,
                      ", numColumns=", table.numColumns());
    ad_utility::AllocatorWithLimit<Id> allocator{table.getAllocator()};
    std::vector<PartitionBucket> partitions;
    partitions.reserve(NUM_PARTITIONS);
    for (size_t i = 0; i < NUM_PARTITIONS; ++i) {
      partitions.emplace_back(allocator);
    }
    const size_t numRows = table.numRows();
    const size_t estimatedBucketSize = numRows / NUM_PARTITIONS + 1;
    for (auto& bucket : partitions) {
      bucket.keys.reserve(estimatedBucketSize);
    }
    for (size_t row = 0; row < numRows; ++row) {
      Id key = table(row, joinColumn);
      partitions[getPartitionIndex(key)].keys.push_back(key);
    }
    return partitions;
  }

  // Count the matches between the join columns of the two tables. Return
  // the total number of matching key pairs.
  //
  // TODO<marvin7122> Process the independent partitions in parallel when
  // this helper is used on large inputs.
  static size_t executeJoinCount(const IdTable& leftTable,
                                 ColumnIndex leftColumn,
                                 const IdTable& rightTable,
                                 ColumnIndex rightColumn) {
    auto leftPartitions = partitionTable(leftTable, leftColumn);
    auto rightPartitions = partitionTable(rightTable, rightColumn);

    size_t totalMatches = 0;

    for (size_t partition = 0; partition < NUM_PARTITIONS; ++partition) {
      auto& buildKeys = leftPartitions[partition].keys;
      const auto& probeKeys = rightPartitions[partition].keys;

      if (buildKeys.empty() || probeKeys.empty()) {
        continue;
      }
      if (buildKeys.size() > 1) {
        std::sort(buildKeys.begin(), buildKeys.end());
      }

      // Every equal key pair counts (bag semantics).
      size_t partitionMatches = 0;
      for (const Id& probeKey : probeKeys) {
        auto range =
            std::equal_range(buildKeys.begin(), buildKeys.end(), probeKey);
        partitionMatches += static_cast<size_t>(range.second - range.first);
      }
      AD_CONTRACT_CHECK(totalMatches <=
                        std::numeric_limits<size_t>::max() - partitionMatches);
      totalMatches += partitionMatches;
    }

    return totalMatches;
  }
};

}  // namespace ql::engine::join

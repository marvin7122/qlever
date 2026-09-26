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
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/VectorWithMemoryLimit.h"

namespace ql::engine::join {

// Radix-partitioned equi-join of two join columns: both sides are split into
// `NUM_PARTITIONS` partitions by the low bits of the hash of the key, and only
// partitions with the same index are joined. `matchRows` finds the matching
// rows (used by the hash join in `JoinImpl`), `executeJoinCount` only counts
// the matching pairs (bag semantics: every pair of equal keys counts).
//
// All `Id` datatypes are supported: partitioning hashes each key with the
// same hash that `Id::operator==` is consistent with, so equal keys always
// land in the same partition.
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
        size_t matches = static_cast<size_t>(range.second - range.first);
        AD_CONTRACT_CHECK(partitionMatches <=
                          std::numeric_limits<size_t>::max() - matches);
        partitionMatches += matches;
      }
      AD_CONTRACT_CHECK(totalMatches <=
                        std::numeric_limits<size_t>::max() - partitionMatches);
      totalMatches += partitionMatches;
    }

    return totalMatches;
  }

  // The keys of a join column, partitioned by `getPartitionIndex`. Partition
  // `p` is `[bounds_[p], bounds_[p + 1])` of `keys_` and `rows_`; `rows_[i]` is
  // the row of `keys_[i]`, and the rows of a partition are in ascending order.
  struct PartitionedColumn {
    std::vector<Id> keys_;
    std::vector<size_t> rows_;
    std::vector<size_t> bounds_;
  };

  // Partition the keys of `column` (a histogram pass and a scatter pass).
  static PartitionedColumn partitionColumn(ql::span<const Id> column) {
    PartitionedColumn result;
    auto& bounds = result.bounds_;
    bounds.assign(NUM_PARTITIONS + 1, 0);
    for (Id key : column) {
      ++bounds[getPartitionIndex(key) + 1];
    }
    std::partial_sum(bounds.begin(), bounds.end(), bounds.begin());
    result.keys_.resize(column.size());
    result.rows_.resize(column.size());
    std::vector<size_t> next(bounds.begin(), bounds.end() - 1);
    for (size_t row = 0; row < column.size(); ++row) {
      Id key = column[row];
      size_t position = next[getPartitionIndex(key)]++;
      result.keys_[position] = key;
      result.rows_[position] = row;
    }
    return result;
  }

  // The result of `matchRows`: the build rows that have the same key as probe
  // row `i` are `buildRows_[k]` for `k` in `[ranges_[i][0], ranges_[i][1])`,
  // in ascending order.
  struct RowMatches {
    std::vector<size_t> buildRows_;
    std::vector<std::array<uint32_t, 2>> ranges_;
  };

  // Find the rows of `buildKeys` that match each row of `probeKeys`. Both
  // columns are radix-partitioned, then each partition of the build side is
  // put into a hash map (key -> range of its rows), which is probed with the
  // keys of the same partition of the probe side. With enough partitions, the
  // hash map of one partition fits into the cache.
  static RowMatches matchRows(ql::span<const Id> buildKeys,
                              ql::span<const Id> probeKeys) {
    AD_CONTRACT_CHECK(buildKeys.size() <= std::numeric_limits<uint32_t>::max());
    auto build = partitionColumn(buildKeys);
    auto probe = partitionColumn(probeKeys);
    RowMatches result;
    result.ranges_.assign(probeKeys.size(), {0, 0});
    std::vector<std::pair<Id, size_t>> sortedPartition;
    ad_utility::HashMap<Id, std::array<uint32_t, 2>> rangeOfKey;
    for (size_t partition = 0; partition < NUM_PARTITIONS; ++partition) {
      size_t buildBegin = build.bounds_[partition];
      size_t buildEnd = build.bounds_[partition + 1];
      size_t probeBegin = probe.bounds_[partition];
      size_t probeEnd = probe.bounds_[partition + 1];
      if (buildBegin == buildEnd || probeBegin == probeEnd) {
        continue;
      }
      // Sort the partition by key, so that the rows of a key are contiguous
      // (and still in ascending order), and map each key to its rows.
      sortedPartition.clear();
      for (size_t i = buildBegin; i < buildEnd; ++i) {
        sortedPartition.emplace_back(build.keys_[i], build.rows_[i]);
      }
      std::sort(sortedPartition.begin(), sortedPartition.end());
      rangeOfKey.clear();
      for (size_t i = buildBegin; i < buildEnd; ++i) {
        const auto& [key, row] = sortedPartition[i - buildBegin];
        build.rows_[i] = row;
        auto it =
            rangeOfKey
                .try_emplace(key,
                             std::array<uint32_t, 2>{static_cast<uint32_t>(i),
                                                     static_cast<uint32_t>(i)})
                .first;
        ++it->second[1];
      }
      for (size_t i = probeBegin; i < probeEnd; ++i) {
        auto it = rangeOfKey.find(probe.keys_[i]);
        if (it != rangeOfKey.end()) {
          result.ranges_[probe.rows_[i]] = it->second;
        }
      }
    }
    result.buildRows_ = std::move(build.rows_);
    return result;
  }
};

}  // namespace ql::engine::join

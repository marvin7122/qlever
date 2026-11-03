// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/OffsetRuns.h"

#include "backports/algorithm.h"
#include "util/Exception.h"

namespace ad_utility::vocabulary {

// _____________________________________________________________________________
std::vector<IndexRun> splitIntoRuns(ql::span<const size_t> indices) {
  // `chunk_by` yields no empty chunks, so `begin(run)` is valid.
  return ::ranges::to<std::vector<IndexRun>>(
      indices | ::ranges::views::chunk_by([](size_t previous, size_t next) {
        return next == previous + 1;
      }) |
      ::ranges::views::transform([](const auto& run) {
        return IndexRun{*::ranges::begin(run),
                        static_cast<size_t>(::ranges::distance(run))};
      }));
}

// _____________________________________________________________________________
std::vector<OffsetPair> readOffsetPairs(ad_utility::BatchManagerBase& manager,
                                        int fd,
                                        ql::span<const size_t> indices) {
  const auto runs = splitIntoRuns(indices);
  // A run of `k` indices needs the `k + 1` entries that bound its words. Each
  // run gets its own consecutive part of `entries`.
  std::vector<uint64_t> entries(indices.size() + runs.size());
  std::vector<ql::span<uint64_t>> entriesOfRuns;
  entriesOfRuns.reserve(runs.size());
  ql::span<uint64_t> unassignedEntries{entries};
  for (const IndexRun& run : runs) {
    entriesOfRuns.push_back(unassignedEntries.first(run.numIndices_ + 1));
    unassignedEntries = unassignedEntries.subspan(run.numIndices_ + 1);
  }
  AD_CORRECTNESS_CHECK(unassignedEntries.empty());

  // One read per run.
  const auto numBytes = ::ranges::to<std::vector<size_t>>(
      entriesOfRuns | ql::views::transform([](ql::span<uint64_t> runEntries) {
        return runEntries.size_bytes();
      }));
  const auto fileOffsets = ::ranges::to<std::vector<uint64_t>>(
      runs | ql::views::transform([](const IndexRun& run) {
        return static_cast<uint64_t>(run.firstIndex_) * sizeof(uint64_t);
      }));
  auto targets = ::ranges::to<std::vector<char*>>(
      entriesOfRuns | ql::views::transform([](ql::span<uint64_t> runEntries) {
        return reinterpret_cast<char*>(runEntries.data());
      }));
  manager.wait(manager.addBatch(fd, numBytes, fileOffsets, targets));

  // Each pair of neighbouring entries of a run bounds one word.
  std::vector<OffsetPair> offsetPairs;
  offsetPairs.reserve(indices.size());
  for (ql::span<const uint64_t> runEntries : entriesOfRuns) {
    for (const auto& [offset, nextOffset] : ::ranges::views::zip(
             runEntries, runEntries | ::ranges::views::drop(1))) {
      offsetPairs.push_back(OffsetPair{offset, nextOffset});
    }
  }
  return offsetPairs;
}

}  // namespace ad_utility::vocabulary

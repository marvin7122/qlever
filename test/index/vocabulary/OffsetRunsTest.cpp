// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IoUringManagerTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/OffsetRuns.h"
#include "util/io_uring/IoUringManager.h"

namespace {

using namespace ::testing;
using ad_utility::vocabulary::IndexRun;
using ad_utility::vocabulary::OffsetPair;
using ad_utility::vocabulary::readOffsetPairs;
using ad_utility::vocabulary::splitIntoRuns;

// _____________________________________________________________________________
TEST(SplitIntoRuns, emptyAndSingleIndex) {
  EXPECT_THAT(splitIntoRuns({}), IsEmpty());
  std::vector<size_t> indices{7};
  EXPECT_THAT(splitIntoRuns(indices), ElementsAre(IndexRun{7, 1}));
}

// _____________________________________________________________________________
TEST(SplitIntoRuns, onlyAscendingNeighboursFormARun) {
  // `3, 4, 5` and `9, 10` are runs. Repeated (`5, 5`), descending (`2, 1`) and
  // non-adjacent (`0, 2`) indices start a new run.
  std::vector<size_t> indices{3, 4, 5, 5, 9, 10, 2, 1, 0, 2};
  EXPECT_THAT(splitIntoRuns(indices),
              ElementsAre(IndexRun{3, 3}, IndexRun{5, 1}, IndexRun{9, 2},
                          IndexRun{2, 1}, IndexRun{1, 1}, IndexRun{0, 1},
                          IndexRun{2, 1}));
}

// A `BatchManagerBase` that reads synchronously and records the reads of every
// batch, so that the tests can check how the offsets are read.
class RecordingManager : public ad_utility::BatchManagerBase {
 public:
  // The `(fileOffset, numBytes)` of every read, per batch.
  std::vector<std::vector<std::pair<uint64_t, size_t>>> batches_;

  BatchHandle addBatch(int fd, ql::span<const size_t> numBytes,
                       ql::span<const uint64_t> offsets,
                       ql::span<char*> buffers) override {
    auto& batch = batches_.emplace_back();
    for (const auto& [size, offset, buffer] :
         ::ranges::views::zip(numBytes, offsets, buffers)) {
      batch.emplace_back(offset, size);
      ad_utility::SyncIoPolicy::readFullyOrThrow(fd, buffer, size, offset);
    }
    return batches_.size() - 1;
  }
  void wait([[maybe_unused]] BatchHandle handle) override {}
};

// An `.offsets` file for the words of the given lengths: the start of each
// word and the end of the last one, as `uint64_t`s.
std::string offsetsFileContent(const std::vector<uint64_t>& wordLengths) {
  std::vector<uint64_t> entries{0};
  for (uint64_t length : wordLengths) {
    entries.push_back(entries.back() + length);
  }
  return std::string{reinterpret_cast<const char*>(entries.data()),
                     entries.size() * sizeof(uint64_t)};
}

// _____________________________________________________________________________
TEST(ReadOffsetPairs, pairsInTheOrderOfTheIndicesWithOneReadPerRun) {
  // Words 0..4 with the lengths 1, 2, 3, 4, 5, i.e. start offsets 0, 1, 3, 6,
  // 10 and the end offset 15.
  auto [tmp, fd] = ioUringManagerTestHelpers::makeTempFile(
      offsetsFileContent({1, 2, 3, 4, 5}));
  RecordingManager manager;
  // The runs are `3, 4` (including the last word, which needs the end offset),
  // `0, 1, 2` and `1`.
  std::vector<size_t> indices{3, 4, 0, 1, 2, 1};
  EXPECT_THAT(
      readOffsetPairs(manager, fd, indices),
      ElementsAre(OffsetPair{6, 10}, OffsetPair{10, 15}, OffsetPair{0, 1},
                  OffsetPair{1, 3}, OffsetPair{3, 6}, OffsetPair{1, 3}));
  // One batch with one read per run; a run of `k` indices reads `k + 1`
  // entries.
  constexpr size_t entry = sizeof(uint64_t);
  using Read = std::pair<uint64_t, size_t>;
  EXPECT_THAT(
      manager.batches_,
      ElementsAre(ElementsAre(Read{3 * entry, 3 * entry}, Read{0, 4 * entry},
                              Read{1 * entry, 2 * entry})));
}

// _____________________________________________________________________________
TEST(ReadOffsetPairs, noIndicesReadNothing) {
  auto [tmp, fd] =
      ioUringManagerTestHelpers::makeTempFile(offsetsFileContent({1}));
  RecordingManager manager;
  EXPECT_THAT(readOffsetPairs(manager, fd, {}), IsEmpty());
  EXPECT_THAT(manager.batches_, ElementsAre(IsEmpty()));
}

}  // namespace

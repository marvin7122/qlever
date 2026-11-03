// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_OFFSETRUNS_H
#define QLEVER_SRC_INDEX_VOCABULARY_OFFSETRUNS_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "backports/three_way_comparison.h"
#include "util/io_uring/IoUringManager.h"

// Reading the `.offsets` file of a `VocabularyOnDisk` for a batch of indices.
// The file stores one `uint64_t` per word (the offset of its first byte in the
// words file) followed by the end offset of the last word, so the word of index
// `i` is bounded by the entries `i` and `i + 1`.

namespace ad_utility::vocabulary {

// The offset of the first byte of a word in the words file and the offset of
// the first byte of the following word, which marks the end of the word.
struct OffsetPair {
  uint64_t offset_;
  uint64_t nextOffset_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(OffsetPair, offset_, nextOffset_)
};

// The consecutive indices `firstIndex_, ..., firstIndex_ + numIndices_ - 1`.
struct IndexRun {
  size_t firstIndex_;
  size_t numIndices_;
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(IndexRun, firstIndex_, numIndices_)
};

// Split `indices` into maximal runs of consecutive indices (each index is one
// more than the previous one), in the order of `indices`. Every index belongs
// to exactly one run, so the runs together have `indices.size()` indices.
//
// TODO<marvin7122> Indices that are almost consecutive (for example every
// second index) are split into many runs of one index each. Up to some gap
// size, it is probably cheaper to read them as one range and skip the entries
// in between; that threshold has to be measured.
std::vector<IndexRun> splitIntoRuns(ql::span<const size_t> indices);

// Read the `OffsetPair` of each of the `indices` from the `.offsets` file `fd`
// of a vocabulary with `numWords` words, with one batch of `manager`, and
// return them in the order of `indices`. Throw if an index is not smaller than
// `numWords`. Reading each run of consecutive indices (see `splitIntoRuns`) as
// one range is only an optimization: a run of `k` indices needs `k + 1`
// entries instead of `2 * k`, in one read instead of `k`.
std::vector<OffsetPair> readOffsetPairs(ad_utility::BatchManagerBase& manager,
                                        int fd, ql::span<const size_t> indices,
                                        size_t numWords);

}  // namespace ad_utility::vocabulary

#endif  // QLEVER_SRC_INDEX_VOCABULARY_OFFSETRUNS_H

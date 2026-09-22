// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_COMPRESSION_H
#define QLEVER_COMPRESSION_H

#include <cstddef>

#include "backports/span.h"
#include "util/Exception.h"

namespace ad_utility {

// Decompress a single word into `destination` using `decompress(span)`. The
// caller guarantees that `decompress` writes at most `bound` bytes, so
// `destination` must hold at least `bound` bytes (precondition). A `bound` of
// 0 returns an empty view without calling `decompress` (mirroring the
// overload in `index/vocabulary/VocabularyTypes.h`): words with a zero upper
// bound carry no payload. Writing zero bytes is still legitimate here:
// e.g. the FSST decoder emits nothing for an empty stored word even when it
// is invoked with a positive bound.
template <typename DecompressFunc>
std::string_view decompressIntoSpan(ql::span<char> destination, size_t bound,
                                    DecompressFunc&& decompress) {
  if (bound == 0) {
    return "";
  }
  AD_CONTRACT_CHECK(destination.size() >= bound);
  size_t bytesWritten = decompress(destination);
  AD_CORRECTNESS_CHECK(bytesWritten <= bound);
  return std::string_view{destination.data(), bytesWritten};
}

}  // namespace ad_utility

#endif  // QLEVER_COMPRESSION_H

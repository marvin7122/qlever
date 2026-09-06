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
// `destination` must hold at least `bound` bytes (precondition), and at least
// one byte (`bound > 0`, precondition): words with a zero upper bound carry no
// payload, so callers register their empty views directly instead of routing
// them through this helper. Writing zero bytes is still legitimate here:
// e.g. the FSST decoder emits nothing for an empty stored word even when it
// is invoked with a positive bound.
template <typename DecompressFunc>
std::string_view decompressIntoSpan(ql::span<char> destination, size_t bound,
                                    DecompressFunc&& decompress) {
  AD_CONTRACT_CHECK(bound > 0);
  AD_CONTRACT_CHECK(destination.size() >= bound);
  size_t bytesWritten = decompress(destination);
  AD_CORRECTNESS_CHECK(bytesWritten <= bound);
  return std::string_view{destination.data(), bytesWritten};
}

}  // namespace ad_utility

#endif  // QLEVER_COMPRESSION_H

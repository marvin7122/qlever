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
#include <string_view>

#include "backports/span.h"
#include "util/Exception.h"

namespace ad_utility {

// Decompress a single word into `destination` using `decompress(span)`. All
// size values below are measured in bytes. The caller guarantees that
// `decompress` writes at most `maxNumBytes` bytes, so `destination` must hold
// at least `maxNumBytes` bytes. A zero upper bound is rejected because words
// without payload are handled by callers without this helper. Writing zero
// bytes remains valid when `maxNumBytes` is positive.
// No generic decompression helper is provided until it has a concrete caller
// and a repository-level ownership contract.

}  // namespace ad_utility

#endif  // QLEVER_COMPRESSION_H

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_STRINGGENERATORTESTHELPERS_H
#define QLEVER_TEST_UTIL_STRINGGENERATORTESTHELPERS_H

#include <cstddef>
#include <string>

#include "util/Generator.h"
#include "util/views/ChunkedIotaView.h"

// Helpers for tests of code that consumes or produces a generator of strings,
// such as the body of an HTTP response and its compression.
namespace ad_utility::testing {

// _____________________________________________________________________________
// Yield the `data` in pieces of `pieceSize` bytes (the last one may be
// shorter), as a response body comes in pieces. Nothing is yielded for empty
// `data`.
inline cppcoro::generator<std::string> inPieces(std::string data,
                                                size_t pieceSize) {
  for (auto [begin, end] :
       ad_utility::chunkedIotaView(size_t{0}, data.size(), pieceSize)) {
    co_yield data.substr(begin, end - begin);
  }
}

// _____________________________________________________________________________
// Return all the pieces of the `generator` as one string.
inline std::string concatenate(cppcoro::generator<std::string> generator) {
  std::string result;
  for (const std::string& piece : generator) {
    result += piece;
  }
  return result;
}

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_STRINGGENERATORTESTHELPERS_H

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_BUFFERPOOLTESTHELPERS_H
#define QLEVER_TEST_UTIL_BUFFERPOOLTESTHELPERS_H

#include <gmock/gmock.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "util/BufferPool.h"
#include "util/File.h"

namespace ad_utility::testing {

// Return the host's OS page size, the page size of every `BufferPool`.
inline size_t bufferPoolTestPageSize() {
  return ad_utility::BufferPool::hostPageSize();
}

// Write `contents` to `filename`.
inline void writeBufferPoolTestFile(const std::string& filename,
                                    const std::string& contents) {
  ad_utility::File file(filename, "w");
  file.write(contents.data(), contents.size());
}

// Return `numPages` pages plus `extra` bytes of distinct bytes (the byte at
// position `i` depends on `i` and on the page, so a copy from a wrong page
// is noticed).
inline std::string makeBufferPoolTestContents(size_t numPages,
                                              size_t extra = 0) {
  const size_t pageSize = bufferPoolTestPageSize();
  std::string contents;
  for (size_t i = 0; i < numPages * pageSize + extra; ++i) {
    contents.push_back(static_cast<char>('a' + (i + i / pageSize) % 26));
  }
  return contents;
}

// Read the ranges `(offset, size)` through `pool` and check them against
// `contents`.
inline void readAndCheckBufferPool(
    ad_utility::BufferPool& pool, const std::string& contents,
    const std::vector<std::pair<uint64_t, size_t>>& ranges,
    const ad_utility::BufferPool::Loader* loader = nullptr,
    size_t prefetchDistance = 0) {
  std::vector<size_t> numBytes;
  std::vector<uint64_t> offsets;
  std::vector<std::string> buffers;
  for (auto [offset, size] : ranges) {
    offsets.push_back(offset);
    numBytes.push_back(size);
    buffers.emplace_back(size, 'X');
  }
  std::vector<char*> targets;
  for (auto& buffer : buffers) {
    targets.push_back(buffer.data());
  }
  pool.readAll(numBytes, offsets, targets, loader, prefetchDistance);
  for (size_t i = 0; i < ranges.size(); ++i) {
    ASSERT_EQ(buffers[i], contents.substr(offsets[i], numBytes[i]))
        << "range " << i << " at offset " << offsets[i];
  }
}

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_BUFFERPOOLTESTHELPERS_H

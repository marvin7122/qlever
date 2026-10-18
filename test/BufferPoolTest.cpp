// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <unistd.h>

#include <atomic>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "util/BufferPool.h"
#include "util/File.h"

namespace {
using ad_utility::BufferPool;
constexpr size_t P = BufferPool::pageSize;

// Write `contents` to `filename`.
void writeFile(const std::string& filename, const std::string& contents) {
  ad_utility::File file(filename, "w");
  file.write(contents.data(), contents.size());
}

// `numPages` pages plus `extra` bytes of distinct bytes (the byte at position
// `i` depends on `i` and on the page, so a copy from a wrong page is noticed).
std::string makeContents(size_t numPages, size_t extra) {
  std::string contents;
  for (size_t i = 0; i < numPages * P + extra; ++i) {
    contents.push_back(static_cast<char>('a' + (i + i / P) % 26));
  }
  return contents;
}

// Read the ranges `(offset, size)` through `pool` and check them.
void readAndCheck(BufferPool& pool, const std::string& contents,
                  const std::vector<std::pair<uint64_t, size_t>>& ranges,
                  const BufferPool::Loader* loader = nullptr,
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
}  // namespace

// _____________________________________________________________________________
TEST(BufferPool, hitsAndMisses) {
  const std::string filename = "bufferPoolTestHits.dat";
  const std::string contents = makeContents(3, 2048);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  BufferPool pool(filename, contents.size(), 100 * P);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  ASSERT_TRUE(pool.isValid());
  EXPECT_EQ(pool.numPages(), 4u);
  // Within a page, across a page boundary, the incomplete last page, empty.
  std::vector<std::pair<uint64_t, size_t>> ranges{
      {10, 20}, {P - 5, 10}, {3 * P + 100, 2048 - 100}, {7, 0}};
  readAndCheck(pool, contents, ranges);
  EXPECT_EQ(pool.numPageLoads(), 3u);  // pages 0, 1, 3
  EXPECT_EQ(pool.numLoadedPages(), 3u);
  EXPECT_EQ(pool.numHits(), 1u);  // the empty read
  // The same reads again: all hits, nothing loaded.
  readAndCheck(pool, contents, ranges, nullptr, 2);
  EXPECT_EQ(pool.numPageLoads(), 3u);
  EXPECT_EQ(pool.numHits(), 5u);
  // The whole file in one read.
  readAndCheck(pool, contents, {{0, contents.size()}});
  EXPECT_EQ(pool.numPageLoads(), 4u);
  EXPECT_EQ(pool.numEvictions(), 0u);
}

// _____________________________________________________________________________
TEST(BufferPool, loaderGetsMergedAlignedRuns) {
  const std::string filename = "bufferPoolTestLoader.dat";
  const std::string contents = makeContents(6, 100);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  BufferPool pool(filename, contents.size(), 100 * P);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  ASSERT_TRUE(pool.isValid());
  std::vector<std::pair<uint64_t, size_t>> calls;
  BufferPool::Loader loader = [&calls](int fd, ql::span<const size_t> numBytes,
                                       ql::span<const uint64_t> offsets,
                                       ql::span<char*> targets) {
    for (size_t i = 0; i < numBytes.size(); ++i) {
      EXPECT_EQ(numBytes[i] % P, 0u);
      EXPECT_EQ(offsets[i] % P, 0u);
      EXPECT_EQ(reinterpret_cast<uintptr_t>(targets[i]) % P, 0u);
      calls.emplace_back(offsets[i], numBytes[i]);
      ASSERT_EQ(
          ::pread(fd, targets[i], numBytes[i], static_cast<off_t>(offsets[i])),
          static_cast<ssize_t>(numBytes[i]));
    }
  };
  // Pages 0-2 (adjacent: one run), page 4, and the incomplete last page 6
  // (read without the loader).
  readAndCheck(pool, contents,
               {{100, 10}, {P + 50, P}, {4 * P + 1, 5}, {6 * P, 100}}, &loader);
  EXPECT_THAT(calls,
              ::testing::ElementsAre(std::pair<uint64_t, size_t>{0, 3 * P},
                                     std::pair<uint64_t, size_t>{4 * P, P}));
  EXPECT_EQ(pool.numPageLoads(), 5u);
}

// _____________________________________________________________________________
TEST(BufferPool, budgetAndEviction) {
  const std::string filename = "bufferPoolTestBudget.dat";
  const std::string contents = makeContents(8, 0);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  BufferPool pool(filename, contents.size(), 2 * P);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  ASSERT_TRUE(pool.isValid());
  for (size_t round = 0; round < 3; ++round) {
    for (size_t page = 0; page < 8; ++page) {
      readAndCheck(pool, contents, {{page * P + 3, 100}});
      EXPECT_LE(pool.numLoadedPages(), 2u);
    }
  }
  EXPECT_GT(pool.numEvictions(), 0u);
  // A batch that needs more pages than the budget is still served; afterwards
  // the budget holds again.
  readAndCheck(pool, contents, {{0, 5 * P}, {6 * P, 2 * P}});
  EXPECT_LE(pool.numLoadedPages(), 2u);
  // Budget zero: nothing stays loaded.
  pool.setBudget(0);
  readAndCheck(pool, contents, {{P, 10}, {5 * P - 1, 2}});
  EXPECT_EQ(pool.numLoadedPages(), 0u);
  readAndCheck(pool, contents, {{P, 10}});
  EXPECT_EQ(pool.numLoadedPages(), 0u);
  // A larger budget keeps pages again.
  pool.setBudget(8 * P);
  readAndCheck(pool, contents, {{0, 8 * P}});
  const auto loads = pool.numPageLoads();
  readAndCheck(pool, contents, {{0, 8 * P}});
  EXPECT_EQ(pool.numPageLoads(), loads);
}

// _____________________________________________________________________________
TEST(BufferPool, failedLoadLeavesPoolUsable) {
  const std::string filename = "bufferPoolTestFail.dat";
  const std::string contents = makeContents(4, 0);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  BufferPool pool(filename, contents.size(), 4 * P);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  ASSERT_TRUE(pool.isValid());
  BufferPool::Loader failing = [](int, ql::span<const size_t>,
                                  ql::span<const uint64_t>, ql::span<char*>) {
    throw std::runtime_error{"simulated I/O error"};
  };
  std::string buffer(10, 'X');
  std::vector<size_t> numBytes{10};
  std::vector<uint64_t> offsets{P + 1};
  std::vector<char*> targets{buffer.data()};
  EXPECT_THROW(pool.readAll(numBytes, offsets, targets, &failing),
               std::runtime_error);
  EXPECT_EQ(pool.numLoadedPages(), 0u);
  readAndCheck(pool, contents, {{P + 1, 10}});
}

// _____________________________________________________________________________
TEST(BufferPool, emptyFile) {
  const std::string filename = "bufferPoolTestEmpty.dat";
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, "");
  BufferPool pool(filename, 0, 4 * P);
  EXPECT_FALSE(pool.isValid());
}

// _____________________________________________________________________________
// Many threads read random ranges, with a budget much smaller than the file,
// so pages are evicted and reloaded while other threads read them. Every read
// must return the right bytes.
TEST(BufferPool, concurrentReadersUnderEviction) {
  const std::string filename = "bufferPoolTestConcurrent.dat";
  const std::string contents = makeContents(64, 1234);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  for (size_t budgetPages : {size_t{0}, size_t{3}, size_t{16}, size_t{100}}) {
    BufferPool pool(filename, contents.size(), budgetPages * P);
#ifndef __linux__
    GTEST_SKIP() << "BufferPool needs Linux";
#endif
    ASSERT_TRUE(pool.isValid());
    std::atomic<size_t> errors = 0;
    std::vector<std::thread> threads;
    for (size_t t = 0; t < 8; ++t) {
      threads.emplace_back([&pool, &contents, &errors, t]() {
        std::mt19937_64 random{t};
        for (size_t iteration = 0; iteration < 300; ++iteration) {
          // A batch of up to 16 reads, half of them in a hot set of 4 pages
          // (many threads on the same pages).
          const size_t batchSize = 1 + random() % 16;
          std::vector<size_t> numBytes;
          std::vector<uint64_t> offsets;
          std::vector<std::string> buffers;
          for (size_t i = 0; i < batchSize; ++i) {
            const uint64_t limit =
                (random() % 2 == 0) ? 4 * P : contents.size();
            const uint64_t offset = random() % limit;
            const size_t size = std::min<uint64_t>(random() % (2 * P),
                                                   contents.size() - offset);
            offsets.push_back(offset);
            numBytes.push_back(size);
            buffers.emplace_back(size, 'X');
          }
          std::vector<char*> targets;
          for (auto& buffer : buffers) {
            targets.push_back(buffer.data());
          }
          pool.readAll(numBytes, offsets, targets, nullptr, t % 3);
          for (size_t i = 0; i < batchSize; ++i) {
            if (buffers[i] != contents.substr(offsets[i], numBytes[i])) {
              ++errors;
            }
          }
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    EXPECT_EQ(errors.load(), 0u) << "budget " << budgetPages << " pages";
    // After all readers are done, the budget holds.
    EXPECT_LE(pool.numLoadedPages(), budgetPages);
  }
}

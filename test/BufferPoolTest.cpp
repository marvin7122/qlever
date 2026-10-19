// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <fcntl.h>
#include <gmock/gmock.h>
#include <sys/uio.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "util/BufferPool.h"
#include "util/File.h"
#include "util/IoUringManager.h"

namespace {
using ad_utility::BufferPool;
constexpr size_t P = BufferPool::defaultPageSize;

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
                  size_t prefetchDistance = 0, size_t partition = 0) {
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
  pool.readAll(numBytes, offsets, targets, loader, prefetchDistance,
               partition);
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

// _____________________________________________________________________________
// Pages larger than the default: the unit of loading and eviction is the
// larger page, reads stay byte-identical (also across page boundaries, in the
// incomplete last page and under a budget of one page), and invalid page
// sizes are rejected.
TEST(BufferPool, largerPages) {
  const std::string filename = "bufferPoolTestLargePages.dat";
  const std::string contents = makeContents(37, 1000);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  constexpr size_t L = 4 * P;
  {
    BufferPool pool(filename, contents.size(), 100 * L, L);
    ASSERT_TRUE(pool.isValid());
    EXPECT_EQ(pool.pageSize(), L);
    EXPECT_EQ(pool.numPages(), 10u);  // 37 pages + 1000 bytes in 16 KiB pages
    // Two reads in the first large page, one across the boundary of large
    // pages 2 and 3, one in the incomplete last page.
    readAndCheck(pool, contents,
                 {{10, 20}, {P + 3, 100}, {3 * L - 7, 50}, {9 * L + 5, 900}});
    EXPECT_EQ(pool.numPageLoads(), 4u);  // large pages 0, 2, 3, 9
    EXPECT_EQ(pool.loadedBytes(), 4 * L);
    readAndCheck(pool, contents, {{L + 1, 2 * L}, {0, contents.size()}});
    EXPECT_EQ(pool.numPageLoads(), 10u);
  }
  {
    // A budget of one large page: every read still returns the right bytes.
    BufferPool pool(filename, contents.size(), L, L);
    std::mt19937_64 random{7};
    for (size_t round = 0; round < 50; ++round) {
      std::vector<std::pair<uint64_t, size_t>> ranges;
      for (size_t i = 0; i < 20; ++i) {
        const uint64_t offset = random() % contents.size();
        ranges.emplace_back(offset,
                            std::min<size_t>(random() % (2 * L),
                                             contents.size() - offset));
      }
      readAndCheck(pool, contents, ranges);
    }
    EXPECT_LE(pool.loadedBytes(), L);
  }
  EXPECT_ANY_THROW(BufferPool(filename, contents.size(), L, 3 * P));
  EXPECT_ANY_THROW(BufferPool(filename, contents.size(), L, 2048));
}

// _____________________________________________________________________________
// Fixed frames (`numFramePartitions` > 0): the pool loads into an arena of
// frames that is faulted in by the constructor, reuses the frames of evicted
// pages, and still returns the right bytes, also across page boundaries, in
// the incomplete last page, and under eviction.
TEST(BufferPool, fixedFramesHitsMissesAndEviction) {
  const std::string filename = "bufferPoolTestFixedFrames.dat";
  const std::string contents = makeContents(100, 777);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  // 40 pages in 2 partitions of 20 frames (above the minimum of 16).
  BufferPool pool(filename, contents.size(), 40 * P, P, 2);
  ASSERT_TRUE(pool.isValid());
  ASSERT_TRUE(pool.usesFixedFrames());
  EXPECT_EQ(pool.numFrames(), 40u);
  EXPECT_EQ(pool.numFramePartitions(), 2u);
  EXPECT_EQ(pool.budgetBytes(), 40 * P);
  EXPECT_GT(pool.prefaultThreads(), 0u);
  // One registered buffer per partition, of its 20 frames, not overlapping.
  auto buffers0 = pool.fixedBufferIovecs(0);
  auto buffers1 = pool.fixedBufferIovecs(1);
  ASSERT_EQ(buffers0.size(), 1u);
  ASSERT_EQ(buffers1.size(), 1u);
  EXPECT_EQ(buffers0[0].second, 20 * P);
  EXPECT_EQ(buffers1[0].first, buffers0[0].first + 20 * P);
  EXPECT_ANY_THROW(pool.fixedBufferIovecs(2));
  // Without fixed frames there are no buffers to register.
  BufferPool plain(filename, contents.size(), 40 * P);
  EXPECT_FALSE(plain.usesFixedFrames());
  EXPECT_TRUE(plain.fixedBufferIovecs(0).empty());

  std::vector<std::pair<uint64_t, size_t>> ranges{
      {10, 20}, {P - 5, 10}, {100 * P + 100, 600}, {7, 0}, {3 * P, 2 * P}};
  readAndCheck(pool, contents, ranges, nullptr, 0, 1);
  EXPECT_EQ(pool.numPageLoads(), 5u);  // pages 0, 1, 3, 4, 100
  readAndCheck(pool, contents, ranges, nullptr, 4, 0);
  EXPECT_EQ(pool.numPageLoads(), 5u);
  // Many pages, several times: more pages than frames, so frames are reused.
  for (size_t round = 0; round < 3; ++round) {
    for (size_t page = 0; page < 101; page += 3) {
      readAndCheck(pool, contents,
                   {{page * P + 1, std::min<size_t>(2 * P, contents.size() -
                                                               page * P - 1)}},
                   nullptr, 0, page);
      EXPECT_LE(pool.numLoadedPages(), 40u);
    }
  }
  EXPECT_GT(pool.numEvictions(), 0u);
  EXPECT_EQ(pool.numBypassedReads(), 0u);
  // The budget can be lowered, but not raised above the frames.
  pool.setBudget(1000 * P);
  EXPECT_EQ(pool.budgetBytes(), 40 * P);
  pool.setBudget(0);
  readAndCheck(pool, contents, {{5 * P, 10}});
  EXPECT_EQ(pool.numLoadedPages(), 0u);
}

// _____________________________________________________________________________
// With fixed frames, the loader gets runs of adjacent pages in consecutive
// frames of the requested partition (where it has free frames), and the
// incomplete last page is read without the loader.
TEST(BufferPool, fixedFramesLoaderGetsFrameTargets) {
  const std::string filename = "bufferPoolTestFixedLoader.dat";
  const std::string contents = makeContents(60, 100);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  // 61 pages: min(64, 61) / 2 = 30 frames per partition.
  BufferPool pool(filename, contents.size(), 64 * P, P, 2);
  ASSERT_TRUE(pool.isValid());
  ASSERT_EQ(pool.numFrames(), 60u);
  const auto partition1 = pool.fixedBufferIovecs(1)[0];
  std::vector<std::pair<uint64_t, size_t>> calls;
  BufferPool::Loader loader = [&](int fd, ql::span<const size_t> numBytes,
                                  ql::span<const uint64_t> offsets,
                                  ql::span<char*> targets) {
    for (size_t i = 0; i < numBytes.size(); ++i) {
      EXPECT_EQ(numBytes[i] % P, 0u);
      EXPECT_EQ(offsets[i] % P, 0u);
      // Into the frames of partition 1.
      EXPECT_GE(targets[i], partition1.first);
      EXPECT_LE(targets[i] + numBytes[i], partition1.first + partition1.second);
      calls.emplace_back(offsets[i], numBytes[i]);
      ASSERT_EQ(
          ::pread(fd, targets[i], numBytes[i], static_cast<off_t>(offsets[i])),
          static_cast<ssize_t>(numBytes[i]));
    }
  };
  readAndCheck(pool, contents,
               {{100, 10}, {P + 50, P}, {4 * P + 1, 5}, {60 * P, 100}}, &loader,
               0, 1);
  EXPECT_THAT(calls,
              ::testing::ElementsAre(std::pair<uint64_t, size_t>{0, 3 * P},
                                     std::pair<uint64_t, size_t>{4 * P, P}));
  EXPECT_EQ(pool.numPageLoads(), 5u);
  // Partition 1 has 30 frames, of which 5 are used: a run of 30 pages takes
  // its remaining 25 frames and 5 of partition 0, so it is split where the
  // frames are not consecutive.
  calls.clear();
  BufferPool::Loader anyFrame = [&](int fd, ql::span<const size_t> numBytes,
                                    ql::span<const uint64_t> offsets,
                                    ql::span<char*> targets) {
    for (size_t i = 0; i < numBytes.size(); ++i) {
      calls.emplace_back(offsets[i], numBytes[i]);
      ASSERT_EQ(
          ::pread(fd, targets[i], numBytes[i], static_cast<off_t>(offsets[i])),
          static_cast<ssize_t>(numBytes[i]));
    }
  };
  readAndCheck(pool, contents, {{10 * P, 30 * P}}, &anyFrame, 0, 1);
  EXPECT_THAT(calls, ::testing::ElementsAre(
                         std::pair<uint64_t, size_t>{10 * P, 25 * P},
                         std::pair<uint64_t, size_t>{35 * P, 5 * P}));
}

// _____________________________________________________________________________
// Reads whose pages find no frame are read past the pool, and a failed load
// leaves the pool usable.
TEST(BufferPool, fixedFramesBypassAndFailedLoad) {
  const std::string filename = "bufferPoolTestFixedBypass.dat";
  const std::string contents = makeContents(40, 0);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  BufferPool pool(filename, contents.size(), 0, P, 1);
  ASSERT_TRUE(pool.isValid());
  ASSERT_EQ(pool.numFrames(), BufferPool::minFramesPerPartition);
  // 32 pages, 16 frames: pages 0-15 are loaded, both reads (which also need
  // pages 16-30 and 35) are read past the pool.
  readAndCheck(pool, contents, {{3, 30 * P}, {35 * P, 10}});
  EXPECT_EQ(pool.numBypassedReads(), 2u);
  EXPECT_EQ(pool.numPageLoads(), 16u);
  EXPECT_EQ(pool.numLoadedPages(), 0u);  // budget 0
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
  readAndCheck(pool, contents, {{P + 1, 10}, {0, 4 * P}});
  EXPECT_EQ(pool.numBypassedReads(), 2u);
}

// _____________________________________________________________________________
// Many threads under eviction with fixed frames, as in
// `concurrentReadersUnderEviction`, also with fewer frames than the threads
// claim at once (then some reads are read past the pool).
TEST(BufferPool, fixedFramesConcurrentReaders) {
  const std::string filename = "bufferPoolTestFixedConcurrent.dat";
  const std::string contents = makeContents(64, 1234);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  for (auto [budgetPages, partitions] : std::vector<std::pair<size_t, size_t>>{
           {0, 1}, {3, 2}, {40, 4}, {100, 3}}) {
    BufferPool pool(filename, contents.size(), budgetPages * P, P, partitions);
    ASSERT_TRUE(pool.isValid());
    std::atomic<size_t> errors = 0;
    std::vector<std::thread> threads;
    for (size_t t = 0; t < 8; ++t) {
      threads.emplace_back([&pool, &contents, &errors, t]() {
        std::mt19937_64 random{t + 100};
        for (size_t iteration = 0; iteration < 300; ++iteration) {
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
          pool.readAll(numBytes, offsets, targets, nullptr, t % 3 * 4, t);
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
    EXPECT_LE(pool.numLoadedPages(), budgetPages);
  }
}

#ifdef QLEVER_HAS_IO_URING
// _____________________________________________________________________________
// The frames of a partition registered as fixed buffers of an io_uring ring:
// the loads through the ring (`IORING_OP_READ_FIXED`) return the right bytes,
// the registration pins the frames (`VmPin` in `/proc/self/status`), and a
// read into a buffer outside the frames still works (plain read).
TEST(BufferPool, fixedFramesRegisteredWithIoUring) {
  const std::string filename = "bufferPoolTestFixedIoUring.dat";
  const std::string contents = makeContents(200, 999);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  std::unique_ptr<ad_utility::BatchManagerBase> manager;
  try {
    manager = std::make_unique<ad_utility::BatchIoManager>(64);
  } catch (const std::exception& e) {
    GTEST_SKIP() << "io_uring not available: " << e.what();
  }
  auto vmPinKb = []() -> long {
    std::ifstream status{"/proc/self/status"};
    std::string line;
    while (std::getline(status, line)) {
      if (line.rfind("VmPin:", 0) == 0) {
        return std::stol(line.substr(6));
      }
    }
    return -1;
  };
  // 201 pages: 64 frames per partition.
  BufferPool pool(filename, contents.size(), 128 * P, P, 2);
  ASSERT_TRUE(pool.isValid());
  ASSERT_EQ(pool.numFrames(), 128u);
  const long pinnedBefore = vmPinKb();
  std::vector<::iovec> iovecs;
  for (auto [base, size] : pool.fixedBufferIovecs(1)) {
    iovecs.push_back(::iovec{base, size});
  }
  int owner = 0;
  if (!manager->registerFixedBuffers(iovecs, &owner)) {
    GTEST_SKIP() << "io_uring_register_buffers refused (RLIMIT_MEMLOCK?)";
  }
  EXPECT_EQ(manager->fixedBuffersOwner(), &owner);
  EXPECT_TRUE(manager->fixedBuffersRegistered());
  if (pinnedBefore >= 0) {
    EXPECT_GE(vmPinKb() - pinnedBefore, static_cast<long>(64 * P / 1024));
  }
  BufferPool::Loader loader = [&manager](int fd,
                                         ql::span<const size_t> numBytes,
                                         ql::span<const uint64_t> offsets,
                                         ql::span<char*> targets) {
    manager->wait(manager->addBatch(fd, numBytes, offsets, targets));
  };
  std::mt19937_64 random{11};
  for (size_t round = 0; round < 40; ++round) {
    std::vector<std::pair<uint64_t, size_t>> ranges;
    for (size_t i = 0; i < 30; ++i) {
      const uint64_t offset = random() % contents.size();
      ranges.emplace_back(offset, std::min<size_t>(random() % (3 * P),
                                                   contents.size() - offset));
    }
    // Partition 1 first; when it has no free frame, frames of partition 0
    // (not registered: plain reads).
    readAndCheck(pool, contents, ranges, &loader, 8, 1);
  }
  EXPECT_GT(pool.numEvictions(), 0u);
  EXPECT_EQ(pool.numBypassedReads(), 0u);
  // A plain read next to the fixed ones.
  const int fd = ::open(filename.c_str(), O_RDONLY | O_CLOEXEC);
  ASSERT_GE(fd, 0);
  absl::Cleanup closeFd{[fd]() { ::close(fd); }};
  std::string outside(100, 'X');
  std::vector<size_t> numBytes{100};
  std::vector<uint64_t> offsets{12345};
  std::vector<char*> targets{outside.data()};
  manager->wait(manager->addBatch(fd, numBytes, offsets, targets));
  EXPECT_EQ(outside, contents.substr(12345, 100));
  // Dropping the registration is allowed while no read is in flight.
  EXPECT_FALSE(manager->registerFixedBuffers({}, nullptr));
  EXPECT_FALSE(manager->fixedBuffersRegistered());
}
#endif

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
#include <chrono>
#include <deque>
#include <memory>
#include <stdexcept>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "util/BufferPool.h"
#include "util/File.h"

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

namespace {
// An `AsyncLoader` that does nothing in `submit` and reads the ranges with
// `pread` in `wait`, so a test can see what happens while loads are pending.
// For one thread only.
struct DeferredLoader {
  struct Batch {
    int fd_;
    std::vector<size_t> numBytes_;
    std::vector<uint64_t> offsets_;
    std::vector<char*> targets_;
  };
  std::vector<Batch> batches_;
  size_t numWaits_ = 0;

  BufferPool::AsyncLoader get() {
    BufferPool::AsyncLoader loader;
    loader.submit = [this](int fd, ql::span<const size_t> numBytes,
                           ql::span<const uint64_t> offsets,
                           ql::span<char*> targets) -> uint64_t {
      batches_.push_back({fd,
                          {numBytes.begin(), numBytes.end()},
                          {offsets.begin(), offsets.end()},
                          {targets.begin(), targets.end()}});
      return batches_.size() - 1;
    };
    loader.wait = [this](uint64_t token) {
      ++numWaits_;
      auto& batch = batches_.at(token);
      for (size_t i = 0; i < batch.numBytes_.size(); ++i) {
        size_t done = 0;
        while (done < batch.numBytes_[i]) {
          ssize_t n = ::pread(batch.fd_, batch.targets_[i] + done,
                              batch.numBytes_[i] - done,
                              static_cast<off_t>(batch.offsets_[i] + done));
          if (n <= 0) {
            throw std::runtime_error{"DeferredLoader: read failed"};
          }
          done += static_cast<size_t>(n);
        }
      }
      batch.numBytes_.clear();
    };
    return loader;
  }
};

// The spans of a batch of reads `(offset, size)` and their buffers.
struct Reads {
  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<std::string> buffers_;
  std::vector<char*> targets_;
  explicit Reads(const std::vector<std::pair<uint64_t, size_t>>& ranges) {
    for (auto [offset, size] : ranges) {
      offsets_.push_back(offset);
      numBytes_.push_back(size);
      buffers_.emplace_back(size, 'X');
    }
    for (auto& buffer : buffers_) {
      targets_.push_back(buffer.data());
    }
  }
  bool matches(const std::string& contents) const {
    for (size_t i = 0; i < numBytes_.size(); ++i) {
      if (buffers_[i] != contents.substr(offsets_[i], numBytes_[i])) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace

// _____________________________________________________________________________
TEST(BufferPool, splitPhaseRead) {
  const std::string filename = "bufferPoolTestSplitPhase.dat";
  const std::string contents = makeContents(6, 100);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  BufferPool pool(filename, contents.size(), 100 * P);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  ASSERT_TRUE(pool.isValid());
  readAndCheck(pool, contents, {{0, 10}});  // page 0 is a hit below
  DeferredLoader deferred;
  auto loader = deferred.get();
  // A hit, adjacent pages 2-3, page 4, the incomplete last page.
  Reads reads{{{5, 20}, {2 * P + 7, P + 3}, {4 * P, 9}, {6 * P + 3, 50}}};
  BufferPool::PendingRead pending;
  pool.startReadAll(pending, reads.numBytes_, reads.offsets_, reads.targets_,
                    loader);
  EXPECT_TRUE(pending.active());
  // The hit is copied right away. Pages 2-4 are one run of one submitted
  // batch, which is not read yet (the incomplete last page is read
  // separately).
  EXPECT_EQ(reads.buffers_[0], contents.substr(5, 20));
  ASSERT_EQ(deferred.batches_.size(), 1u);
  EXPECT_EQ(deferred.batches_[0].numBytes_.size(), 1u);
  EXPECT_EQ(deferred.numWaits_, 0u);
  EXPECT_EQ(reads.buffers_[2], std::string(9, 'X'));
  pool.finishReadAll(pending, reads.numBytes_, reads.offsets_, reads.targets_);
  EXPECT_FALSE(pending.active());
  EXPECT_EQ(deferred.numWaits_, 1u);
  EXPECT_TRUE(reads.matches(contents));
  EXPECT_EQ(pool.numPageLoads(), 5u);  // 0, 2, 3, 4, 6
  // Everything is loaded now: a second split-phase read submits nothing.
  Reads again{{{2 * P, 2 * P}, {6 * P, 100}}};
  pool.startReadAll(pending, again.numBytes_, again.offsets_, again.targets_,
                    loader);
  EXPECT_EQ(deferred.batches_.size(), 1u);
  pool.finishReadAll(pending, again.numBytes_, again.offsets_, again.targets_);
  EXPECT_TRUE(again.matches(contents));
}

// _____________________________________________________________________________
TEST(BufferPool, pendingClaimsOfTheSameThreadDoNotDeadlock) {
  const std::string filename = "bufferPoolTestOwnPending.dat";
  const std::string contents = makeContents(4, 0);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  BufferPool pool(filename, contents.size(), 100 * P);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  ASSERT_TRUE(pool.isValid());
  DeferredLoader deferred;
  auto loader = deferred.get();
  Reads first{{{P + 1, 100}}};
  BufferPool::PendingRead pending;
  pool.startReadAll(pending, first.numBytes_, first.offsets_, first.targets_,
                    loader);
  // Page 1 is claimed by the pending read. A synchronous read of the same
  // thread that needs it must first complete the pending read instead of
  // waiting for itself.
  readAndCheck(pool, contents, {{P + 50, 10}, {3 * P, 5}});
  EXPECT_EQ(deferred.numWaits_, 1u);
  // A second pending read of the same page finds it loaded.
  Reads second{{{P + 200, 30}}};
  BufferPool::PendingRead pending2;
  pool.startReadAll(pending2, second.numBytes_, second.offsets_,
                    second.targets_, loader);
  pool.finishReadAll(pending2, second.numBytes_, second.offsets_,
                     second.targets_);
  pool.finishReadAll(pending, first.numBytes_, first.offsets_,
                     first.targets_);
  EXPECT_TRUE(first.matches(contents));
  EXPECT_TRUE(second.matches(contents));
  EXPECT_EQ(pool.numPageLoads(), 2u);
}

// _____________________________________________________________________________
TEST(BufferPool, destroyedPendingReadPublishesItsPages) {
  const std::string filename = "bufferPoolTestDestroyedPending.dat";
  const std::string contents = makeContents(3, 0);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  BufferPool pool(filename, contents.size(), 100 * P);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  ASSERT_TRUE(pool.isValid());
  DeferredLoader deferred;
  auto loader = deferred.get();
  {
    Reads reads{{{0, 3 * P}}};
    BufferPool::PendingRead pending;
    pool.startReadAll(pending, reads.numBytes_, reads.offsets_,
                      reads.targets_, loader);
  }
  EXPECT_EQ(deferred.numWaits_, 1u);
  EXPECT_EQ(pool.numLoadedPages(), 3u);
  readAndCheck(pool, contents, {{0, 3 * P}});
  EXPECT_EQ(pool.numPageLoads(), 3u);
}

// _____________________________________________________________________________
TEST(BufferPool, failedSplitPhaseLoadLeavesPoolUsable) {
  const std::string filename = "bufferPoolTestFailedSplit.dat";
  const std::string contents = makeContents(2, 0);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  BufferPool pool(filename, contents.size(), 100 * P);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  ASSERT_TRUE(pool.isValid());
  BufferPool::AsyncLoader failing;
  failing.submit = [](int, ql::span<const size_t>, ql::span<const uint64_t>,
                      ql::span<char*>) -> uint64_t { return 0; };
  failing.wait = [](uint64_t) { throw std::runtime_error{"injected"}; };
  Reads reads{{{10, 100}}};
  BufferPool::PendingRead pending;
  pool.startReadAll(pending, reads.numBytes_, reads.offsets_, reads.targets_,
                    failing);
  EXPECT_THROW(pool.finishReadAll(pending, reads.numBytes_, reads.offsets_,
                                  reads.targets_),
               std::runtime_error);
  EXPECT_FALSE(pending.active());
  EXPECT_EQ(pool.numLoadedPages(), 0u);
  readAndCheck(pool, contents, {{10, 100}, {P, P}});
}

// _____________________________________________________________________________
// Several threads with up to three split-phase reads in flight each (like the
// export pipeline) plus synchronous reads, on a hot set of pages and under
// eviction, with the waiting threads yielding or sleeping on the futex. A
// thread must never wait for another thread's page while it holds claims of
// its own pending reads; otherwise this test deadlocks.
TEST(BufferPool, concurrentSplitPhaseReaders) {
  const std::string filename = "bufferPoolTestConcurrentSplit.dat";
  const std::string contents = makeContents(64, 777);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  absl::Cleanup resetWait{[]() { ad_utility::bufferPoolFutexWait = false; }};
  for (bool futex : {false, true}) {
    ad_utility::bufferPoolFutexWait = futex;
    for (size_t budgetPages : {size_t{0}, size_t{4}, size_t{100}}) {
      BufferPool pool(filename, contents.size(), budgetPages * P);
#ifndef __linux__
      GTEST_SKIP() << "BufferPool needs Linux";
#endif
      ASSERT_TRUE(pool.isValid());
      std::atomic<size_t> errors = 0;
      std::vector<std::thread> threads;
      for (size_t t = 0; t < 8; ++t) {
        threads.emplace_back([&pool, &contents, &errors, t]() {
          std::mt19937_64 random{t + 100};
          DeferredLoader deferred;
          auto loader = deferred.get();
          auto randomReads = [&]() {
            std::vector<std::pair<uint64_t, size_t>> ranges;
            const size_t batchSize = 1 + random() % 12;
            for (size_t i = 0; i < batchSize; ++i) {
              const uint64_t limit =
                  (random() % 2 == 0) ? 4 * P : contents.size();
              const uint64_t offset = random() % limit;
              const size_t size = std::min<uint64_t>(
                  random() % (2 * P), contents.size() - offset);
              ranges.emplace_back(offset, size);
            }
            return std::make_unique<Reads>(ranges);
          };
          struct InFlight {
            std::unique_ptr<Reads> reads_;
            std::unique_ptr<BufferPool::PendingRead> pending_;
          };
          std::deque<InFlight> inFlight;
          auto finishOldest = [&]() {
            auto& done = inFlight.front();
            pool.finishReadAll(*done.pending_, done.reads_->numBytes_,
                               done.reads_->offsets_, done.reads_->targets_);
            errors += !done.reads_->matches(contents);
            inFlight.pop_front();
          };
          for (size_t iteration = 0; iteration < 200; ++iteration) {
            InFlight next{randomReads(),
                          std::make_unique<BufferPool::PendingRead>()};
            pool.startReadAll(*next.pending_, next.reads_->numBytes_,
                              next.reads_->offsets_, next.reads_->targets_,
                              loader, t % 2);
            inFlight.push_back(std::move(next));
            if (iteration % 5 == 0) {
              auto sync = randomReads();
              pool.readAll(sync->numBytes_, sync->offsets_, sync->targets_);
              errors += !sync->matches(contents);
            }
            if (inFlight.size() == 3 || random() % 4 == 0) {
              finishOldest();
            }
          }
          while (!inFlight.empty()) {
            finishOldest();
          }
        });
      }
      for (auto& thread : threads) {
        thread.join();
      }
      EXPECT_EQ(errors.load(), 0u)
          << "budget " << budgetPages << " pages, futex " << futex;
      EXPECT_LE(pool.numLoadedPages(), budgetPages);
    }
  }
}

// _____________________________________________________________________________
// A thread waiting for a page that another thread loads sleeps on the futex
// and is woken when the page is published.
TEST(BufferPool, futexWaiterIsWokenByPublish) {
  const std::string filename = "bufferPoolTestFutex.dat";
  const std::string contents = makeContents(2, 0);
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  writeFile(filename, contents);
  absl::Cleanup resetWait{[]() { ad_utility::bufferPoolFutexWait = false; }};
  ad_utility::bufferPoolFutexWait = true;
  BufferPool pool(filename, contents.size(), 100 * P);
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  ASSERT_TRUE(pool.isValid());
  DeferredLoader deferred;
  auto loader = deferred.get();
  Reads reads{{{0, 100}}};
  BufferPool::PendingRead pending;
  pool.startReadAll(pending, reads.numBytes_, reads.offsets_, reads.targets_,
                    loader);
  std::atomic<bool> done = false;
  std::thread waiter{[&]() {
    readAndCheck(pool, contents, {{50, 100}});
    done = true;
  }};
  // The waiter cannot finish while page 0 is claimed.
  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  EXPECT_FALSE(done.load());
  pool.finishReadAll(pending, reads.numBytes_, reads.offsets_, reads.targets_);
  waiter.join();
  EXPECT_TRUE(done.load());
  EXPECT_TRUE(reads.matches(contents));
  EXPECT_EQ(pool.numPageLoads(), 1u);
}

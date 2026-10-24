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
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "util/BufferPool.h"
#include "util/BufferPoolBudget.h"
#include "util/File.h"

namespace {
using ad_utility::BufferPool;
using ad_utility::BufferPoolBudgetConfig;
using ad_utility::BufferPoolBudgetController;
using ad_utility::BufferPoolSignals;
using ad_utility::nextBufferPoolTarget;
// The host's OS page size (a run-time value where the kernel chooses it).
const size_t P = BufferPool::hostPageSize();
constexpr size_t MiB = BufferPoolBudgetConfig::MiB;
constexpr size_t GiB = BufferPoolBudgetConfig::GiB;

// Write `contents` to `filename`.
void writeFile(const std::string& filename, const std::string& contents) {
  ad_utility::File file(filename, "w");
  file.write(contents.data(), contents.size());
}

// Distinct bytes per page, so a copy from a wrong page is noticed.
std::string makeContents(size_t numPages) {
  std::string contents;
  for (size_t i = 0; i < numPages * P; ++i) {
    contents.push_back(static_cast<char>('a' + (i + i / P) % 26));
  }
  return contents;
}

// Read the ranges `(offset, size)` through `pool` and check the bytes.
void readAndCheck(BufferPool& pool, const std::string& contents,
                  const std::vector<std::pair<uint64_t, size_t>>& ranges) {
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
  pool.readAll(numBytes, offsets, targets);
  for (size_t i = 0; i < ranges.size(); ++i) {
    ASSERT_EQ(buffers[i], contents.substr(offsets[i], numBytes[i]));
  }
}

// Wait up to `timeout` for `condition`.
template <typename F>
bool waitFor(F condition, std::chrono::milliseconds timeout) {
  const auto end = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < end) {
    if (condition()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{2});
  }
  return condition();
}
}  // namespace

// _____________________________________________________________________________
TEST(BufferPoolBudget, policyGrowsToTheCeiling) {
  const size_t floor = 256 * MiB;
  const size_t ceiling = 3 * GiB;
  size_t target = floor;
  // Plenty of free memory, no pressure: one step per tick, then the ceiling.
  for (size_t expected : {floor + GiB, floor + 2 * GiB, 3 * GiB, 3 * GiB}) {
    target = nextBufferPoolTarget(target, target, ceiling, floor, 2 * GiB, GiB,
                                  100 * GiB, 0.0, 10.0);
    EXPECT_EQ(target, expected);
  }
  // No growth unless the free memory exceeds the reserve by more than a step.
  EXPECT_EQ(nextBufferPoolTarget(floor, 0, ceiling, floor, 2 * GiB, GiB,
                                 3 * GiB, 0.0, 10.0),
            floor);
  // Unknown signals: unchanged.
  EXPECT_EQ(nextBufferPoolTarget(GiB, GiB, ceiling, floor, 2 * GiB, GiB,
                                 std::nullopt, std::nullopt, 10.0),
            GiB);
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, policyShrinksOnLowMemoryAndPressure) {
  const size_t floor = 256 * MiB;
  const size_t ceiling = 16 * GiB;
  // Low free memory: shrink by the deficit (1 GiB) when it is more than 10 %.
  EXPECT_EQ(nextBufferPoolTarget(4 * GiB, 4 * GiB, ceiling, floor, 2 * GiB, GiB,
                                 1 * GiB, 0.0, 10.0),
            3 * GiB);
  // A small deficit still shrinks by 10 %.
  EXPECT_EQ(nextBufferPoolTarget(10 * GiB, 10 * GiB, ceiling, floor, 2 * GiB,
                                 GiB, 2 * GiB - MiB, 0.0, 10.0),
            9 * GiB);
  // Pressure with plenty of free memory: 10 %.
  EXPECT_EQ(nextBufferPoolTarget(10 * GiB, 10 * GiB, ceiling, floor, 2 * GiB,
                                 GiB, 50 * GiB, 25.0, 10.0),
            9 * GiB);
  // Shrinking starts from what is loaded: lowering an unused target would
  // free nothing.
  EXPECT_EQ(nextBufferPoolTarget(8 * GiB, 1 * GiB, ceiling, floor, 2 * GiB, GiB,
                                 50 * GiB, 25.0, 10.0),
            1 * GiB - (1 * GiB) / 10);
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, policyStaysWithinFloorAndCeiling) {
  const size_t floor = 256 * MiB;
  // A huge deficit never goes below the floor.
  EXPECT_EQ(nextBufferPoolTarget(300 * MiB, 300 * MiB, 16 * GiB, floor, 2 * GiB,
                                 GiB, 0, 99.0, 10.0),
            floor);
  // A target above the ceiling (e.g. the free memory dropped) is clamped.
  EXPECT_EQ(nextBufferPoolTarget(8 * GiB, 8 * GiB, 5 * GiB, floor, 2 * GiB, GiB,
                                 100 * GiB, 0.0, 10.0),
            5 * GiB);
  // A ceiling below the floor wins.
  EXPECT_EQ(nextBufferPoolTarget(GiB, GiB, 100 * MiB, floor, 2 * GiB, GiB,
                                 100 * GiB, 0.0, 10.0),
            100 * MiB);
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, evictTowardsBudgetIsBounded) {
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  const std::string filename = "bufferPoolBudgetTestBounded.dat";
  const std::string contents = makeContents(20);
  writeFile(filename, contents);
  absl::Cleanup cleanup{[&filename]() { ::unlink(filename.c_str()); }};
  BufferPool pool{filename, contents.size(), 20 * P};
  ASSERT_TRUE(pool.isValid());
  readAndCheck(pool, contents, {{0, 20 * P}});
  ASSERT_EQ(pool.loadedBytes(), 20 * P);
  pool.setBudget(5 * P);
  EXPECT_EQ(pool.evictTowardsBudget(4), 4u);
  EXPECT_EQ(pool.loadedBytes(), 16 * P);
  EXPECT_EQ(pool.evictTowardsBudget(100), 11u);
  EXPECT_EQ(pool.loadedBytes(), 5 * P);
  EXPECT_EQ(pool.evictTowardsBudget(100), 0u);
  // The remaining pages are still read correctly.
  readAndCheck(pool, contents, {{0, 20 * P}});
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, backgroundEvictionWithConcurrentReaders) {
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  const size_t numPages = 256;
  const std::string filename = "bufferPoolBudgetTestBackground.dat";
  const std::string contents = makeContents(numPages);
  writeFile(filename, contents);
  absl::Cleanup cleanup{[&filename]() { ::unlink(filename.c_str()); }};
  BufferPool pool{filename, contents.size(), 4 * P};
  ASSERT_TRUE(pool.isValid());

  std::atomic<size_t> available{1000 * numPages * P};
  BufferPoolSignals signals;
  signals.availableBytes = [&available]() -> std::optional<size_t> {
    return available.load();
  };
  BufferPoolBudgetConfig config;
  config.floorBytes = 4 * P;
  config.reserveBytes = 100 * P;
  config.stepBytes = 64 * P;
  config.maxEvictBytesPerTick = 8 * P;
  config.period = std::chrono::milliseconds{5};
  BufferPoolBudgetController controller{signals, config};
  auto registration = controller.add({&pool});

  // Free memory: the target grows to the whole file, and the readers load it.
  ASSERT_TRUE(
      waitFor([&]() { return controller.targetBytes() == numPages * P; },
              std::chrono::milliseconds{5000}));
  readAndCheck(pool, contents, {{0, numPages * P}});
  EXPECT_EQ(pool.loadedBytes(), numPages * P);

  // Readers that keep checking random ranges while the budget shrinks.
  std::atomic<bool> stop{false};
  std::atomic<size_t> rounds{0};
  std::vector<std::thread> readers;
  for (size_t t = 0; t < 4; ++t) {
    readers.emplace_back([&, t]() {
      std::mt19937_64 rng{t};
      std::uniform_int_distribution<uint64_t> offsetDist{0,
                                                         contents.size() - 1};
      while (!stop.load()) {
        std::vector<std::pair<uint64_t, size_t>> ranges;
        for (size_t i = 0; i < 8; ++i) {
          const uint64_t offset = offsetDist(rng);
          ranges.emplace_back(
              offset,
              std::min<size_t>(1 + offset % (2 * P), contents.size() - offset));
        }
        readAndCheck(pool, contents, ranges);
        ++rounds;
      }
    });
  }

  // Low free memory: the target shrinks to the floor in bounded steps.
  available = 10 * P;
  EXPECT_TRUE(waitFor([&]() { return controller.targetBytes() == 4 * P; },
                      std::chrono::milliseconds{5000}));
  ASSERT_TRUE(waitFor([&]() { return rounds.load() > 50; },
                      std::chrono::milliseconds{10000}));
  stop = true;
  for (auto& reader : readers) {
    reader.join();
  }
  // Without readers, the background thread brings the pool down to its target
  // within a few ticks.
  EXPECT_TRUE(waitFor([&]() { return pool.loadedBytes() <= 4 * P; },
                      std::chrono::milliseconds{5000}));
  EXPECT_GT(pool.numEvictions(), 0u);
  readAndCheck(pool, contents, {{0, numPages * P}});
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, unregisteringStopsTheThread) {
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  const std::string filename = "bufferPoolBudgetTestStop.dat";
  const std::string contents = makeContents(8);
  writeFile(filename, contents);
  absl::Cleanup cleanup{[&filename]() { ::unlink(filename.c_str()); }};

  std::atomic<size_t> calls{0};
  BufferPoolSignals signals;
  signals.availableBytes = [&calls]() -> std::optional<size_t> {
    ++calls;
    return 1000 * P;
  };
  BufferPoolBudgetConfig config;
  config.floorBytes = P;
  config.reserveBytes = P;
  config.stepBytes = P;
  config.period = std::chrono::milliseconds{1};
  BufferPoolBudgetController controller{signals, config};
  for (size_t round = 0; round < 3; ++round) {
    auto pool = std::make_unique<BufferPool>(filename, contents.size(), P);
    auto registration = controller.add({pool.get()});
    const size_t before = calls.load();
    ASSERT_TRUE(waitFor([&]() { return calls.load() > before + 3; },
                        std::chrono::milliseconds{5000}));
    // Unregister, then destroy the pool: the thread must neither tick nor
    // touch the pool any more.
    registration.reset();
    pool.reset();
    const size_t afterStop = calls.load();
    std::this_thread::sleep_for(std::chrono::milliseconds{30});
    EXPECT_EQ(calls.load(), afterStop);
  }
  // A controller with a running thread stops it when destroyed.
  BufferPool pool{filename, contents.size(), P};
  auto other = std::make_unique<BufferPoolBudgetController>(signals, config);
  auto registration = other->add({&pool});
  registration.reset();
  other.reset();
}

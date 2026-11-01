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
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "./util/BufferPoolTestHelpers.h"
#include "util/BufferPool.h"
#include "util/BufferPoolBudget.h"
#include "util/File.h"

namespace {
using ad_utility::BufferPool;
using ad_utility::BufferPoolBudgetConfig;
using ad_utility::BufferPoolBudgetController;
using ad_utility::bufferPoolBudgetShare;
using ad_utility::BufferPoolSignals;
using ad_utility::nextBufferPoolTarget;
using ad_utility::testing::makeBufferPoolTestContents;
using ad_utility::testing::readAndCheckBufferPool;
using ad_utility::testing::writeBufferPoolTestFile;
// The host's OS page size (a run-time value where the kernel chooses it).
const size_t P = BufferPool::hostPageSize();
constexpr size_t MIB = BufferPoolBudgetConfig::MIB;
constexpr size_t GIB = BufferPoolBudgetConfig::GIB;

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
TEST(BufferPoolBudget, budgetShareIsProportionalWithBoundaries) {
  // The one split rule shared by the fixed and elastic paths.
  EXPECT_EQ(bufferPoolBudgetShare(100, 0, 300), 0u);
  EXPECT_EQ(bufferPoolBudgetShare(100, 300, 300), 100u);
  EXPECT_EQ(bufferPoolBudgetShare(100, 75, 300), 25u);
  EXPECT_EQ(bufferPoolBudgetShare(0, 75, 300), 0u);
  // No whole (both files empty): no share, and no division by zero.
  EXPECT_EQ(bufferPoolBudgetShare(100, 0, 0), 0u);
  EXPECT_EQ(bufferPoolBudgetShare(100, 75, 0), 0u);
  // The shares of a two-way split add up to at most the total.
  const size_t total = 1234567;
  const size_t words = bufferPoolBudgetShare(total, 1980, 1980 + 1260);
  EXPECT_LE(words + bufferPoolBudgetShare(total, 1260, 1980 + 1260), total);
  EXPECT_GT(words, total / 2);
}

// _____________________________________________________________________________
// The parsers behind `BufferPoolSignals::system`, fed with fake `/proc` and
// cgroup trees (the production signal correctness that injected-signal
// policy tests cannot cover): meminfo, cgroup v2 and v1, and PSI.
TEST(BufferPoolBudget, systemSignalParsersOnFakeTrees) {
  namespace fs = std::filesystem;
  namespace detail = ad_utility::detail;
  const fs::path root = fs::temp_directory_path() / "bufferPoolBudgetFakeProc";
  absl::Cleanup removeTree{[&root] { fs::remove_all(root); }};
  auto write = [&root](const std::string& relative, const std::string& text) {
    const fs::path path = root / relative;
    fs::create_directories(path.parent_path());
    writeBufferPoolTestFile(path.string(), text);
    return path.string();
  };

  // meminfo: value in kB, missing key and missing file give no signal.
  const std::string meminfo =
      write("meminfo", "MemTotal: 16384 kB\nMemAvailable: 8192 kB\n");
  EXPECT_EQ(detail::meminfoBytesFrom(meminfo, "MemAvailable"), 8192 * 1024u);
  EXPECT_EQ(detail::meminfoBytesFrom(meminfo, "Nope"), std::nullopt);
  EXPECT_EQ(detail::meminfoBytesFrom(root.string() + "/absent", "MemTotal"),
            std::nullopt);

  // Cgroup byte counts: plain value, v2 "max", the v1 no-limit sentinel,
  // garbage, and a missing file all parse without throwing.
  EXPECT_EQ(detail::cgroupBytesFrom(write("v", "12345\n")), 12345u);
  EXPECT_EQ(detail::cgroupBytesFrom(write("v", "max\n")), std::nullopt);
  EXPECT_EQ(detail::cgroupBytesFrom(write("v", "9223372036854771712\n")),
            std::nullopt);
  EXPECT_EQ(detail::cgroupBytesFrom(write("v", "junk\n")), std::nullopt);
  EXPECT_EQ(detail::cgroupBytesFrom(root.string() + "/absent"), std::nullopt);

  // PSI: the `some avg10` field.
  EXPECT_EQ(detail::psiSomeAvg10From(
                write("pressure",
                      "some avg10=1.25 avg60=0.50 avg300=0.10 total=42\n"
                      "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")),
            1.25);
  EXPECT_EQ(detail::psiSomeAvg10From(root.string() + "/absent"), std::nullopt);

  // Location: v2 wins when present, else the v1 memory controller.
  auto location = detail::ownCgroupLocation(
      write("cgroup", "9:memory:/legacy\n0::/user.slice\n"),
      (root / "cg2").string(), (root / "cg1").string());
  ASSERT_TRUE(location.has_value());
  EXPECT_TRUE(location->version2_);
  EXPECT_EQ(location->dir_, (root / "cg2/user.slice").string());
  location = detail::ownCgroupLocation(
      write("cgroup", "5:cpu,memory:/docker/abc\n"), (root / "cg2").string(),
      (root / "cg1").string());
  ASSERT_TRUE(location.has_value());
  EXPECT_FALSE(location->version2_);
  EXPECT_EQ(location->dir_, (root / "cg1/docker/abc").string());

  // Limits v2: headroom counts inactive file fully and active file half as
  // reclaimable; the tighter ancestor wins.
  const std::string sg = "cg2/user.slice/";
  write(sg + "memory.max", "1000\n");
  write(sg + "memory.current", "800\n");
  write(sg + "memory.stat", "inactive_file 100\nactive_file 100\n");
  write("cg2/memory.max", "5000\n");
  write("cg2/memory.current", "900\n");
  write("cg2/memory.stat", "inactive_file 0\nactive_file 0\n");
  auto limits = detail::cgroupLimitsFrom(detail::CgroupLocation{
      (root / "cg2/user.slice").string(), (root / "cg2").string(), true});
  EXPECT_EQ(limits.limit_, 1000u);
  EXPECT_EQ(limits.headroom_, 350u);

  // Limits v1: `limit_in_bytes`/`usage_in_bytes`, hierarchical total_ stat
  // keys, and the sentinel at the root means no limit there.
  const std::string lg = "cg1/docker/abc/";
  write(lg + "memory.limit_in_bytes", "2000\n");
  write(lg + "memory.usage_in_bytes", "1500\n");
  write(lg + "memory.stat", "total_inactive_file 500\ntotal_active_file 0\n");
  write("cg1/memory.limit_in_bytes", "9223372036854771712\n");
  limits = detail::cgroupLimitsFrom(detail::CgroupLocation{
      (root / "cg1/docker/abc").string(), (root / "cg1").string(), false});
  EXPECT_EQ(limits.limit_, 2000u);
  EXPECT_EQ(limits.headroom_, 1000u);
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, policyGrowsToTheCeiling) {
  const size_t floor = 256 * MIB;
  const size_t ceiling = 3 * GIB;
  size_t target = floor;
  // Plenty of free memory, no pressure: one step per tick, then the ceiling.
  for (size_t expected : {floor + GIB, floor + 2 * GIB, 3 * GIB, 3 * GIB}) {
    target = nextBufferPoolTarget(target, target, ceiling, floor, 2 * GIB, GIB,
                                  100 * GIB, 0.0, 10.0);
    EXPECT_EQ(target, expected);
  }
  // No growth unless the free memory exceeds the reserve by more than a step.
  EXPECT_EQ(nextBufferPoolTarget(floor, 0, ceiling, floor, 2 * GIB, GIB,
                                 3 * GIB, 0.0, 10.0),
            floor);
  // Unknown signals: unchanged.
  EXPECT_EQ(nextBufferPoolTarget(GIB, GIB, ceiling, floor, 2 * GIB, GIB,
                                 std::nullopt, std::nullopt, 10.0),
            GIB);
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, policyShrinksOnLowMemoryAndPressure) {
  const size_t floor = 256 * MIB;
  const size_t ceiling = 16 * GIB;
  // Low free memory: shrink by the deficit (1 GiB) when it is more than 10 %.
  EXPECT_EQ(nextBufferPoolTarget(4 * GIB, 4 * GIB, ceiling, floor, 2 * GIB, GIB,
                                 1 * GIB, 0.0, 10.0),
            3 * GIB);
  // A small deficit still shrinks by 10 %.
  EXPECT_EQ(nextBufferPoolTarget(10 * GIB, 10 * GIB, ceiling, floor, 2 * GIB,
                                 GIB, 2 * GIB - MIB, 0.0, 10.0),
            9 * GIB);
  // Pressure with plenty of free memory: 10 %.
  EXPECT_EQ(nextBufferPoolTarget(10 * GIB, 10 * GIB, ceiling, floor, 2 * GIB,
                                 GIB, 50 * GIB, 25.0, 10.0),
            9 * GIB);
  // Shrinking starts from what is loaded: lowering an unused target would
  // free nothing.
  EXPECT_EQ(nextBufferPoolTarget(8 * GIB, 1 * GIB, ceiling, floor, 2 * GIB, GIB,
                                 50 * GIB, 25.0, 10.0),
            1 * GIB - (1 * GIB) / 10);
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, policyStaysWithinFloorAndCeiling) {
  const size_t floor = 256 * MIB;
  // A huge deficit never goes below the floor.
  EXPECT_EQ(nextBufferPoolTarget(300 * MIB, 300 * MIB, 16 * GIB, floor, 2 * GIB,
                                 GIB, 0, 99.0, 10.0),
            floor);
  // A target above the ceiling (e.g. the free memory dropped) is clamped.
  EXPECT_EQ(nextBufferPoolTarget(8 * GIB, 8 * GIB, 5 * GIB, floor, 2 * GIB, GIB,
                                 100 * GIB, 0.0, 10.0),
            5 * GIB);
  // A ceiling below the floor wins.
  EXPECT_EQ(nextBufferPoolTarget(GIB, GIB, 100 * MIB, floor, 2 * GIB, GIB,
                                 100 * GIB, 0.0, 10.0),
            100 * MIB);
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, evictTowardsBudgetIsBounded) {
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  const std::string filename = "bufferPoolBudgetTestBounded.dat";
  const std::string contents = makeBufferPoolTestContents(20);
  writeBufferPoolTestFile(filename, contents);
  absl::Cleanup cleanup{[&filename]() { ::unlink(filename.c_str()); }};
  BufferPool pool{filename, contents.size(), 20 * P};
  readAndCheckBufferPool(pool, contents, {{0, 20 * P}});
  ASSERT_EQ(pool.loadedBytes(), 20 * P);
  pool.setBudget(5 * P);
  EXPECT_EQ(pool.evictTowardsBudget(4), 4u);
  EXPECT_EQ(pool.loadedBytes(), 16 * P);
  EXPECT_EQ(pool.evictTowardsBudget(100), 11u);
  EXPECT_EQ(pool.loadedBytes(), 5 * P);
  EXPECT_EQ(pool.evictTowardsBudget(100), 0u);
  // The remaining pages are still read correctly.
  readAndCheckBufferPool(pool, contents, {{0, 20 * P}});
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, backgroundEvictionWithConcurrentReaders) {
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  const size_t numPages = 256;
  const std::string filename = "bufferPoolBudgetTestBackground.dat";
  const std::string contents = makeBufferPoolTestContents(numPages);
  writeBufferPoolTestFile(filename, contents);
  absl::Cleanup cleanup{[&filename]() { ::unlink(filename.c_str()); }};
  BufferPool pool{filename, contents.size(), 4 * P};

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
  readAndCheckBufferPool(pool, contents, {{0, numPages * P}});
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
        readAndCheckBufferPool(pool, contents, ranges);
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
  readAndCheckBufferPool(pool, contents, {{0, numPages * P}});
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, unregisteringStopsTheThread) {
#ifndef __linux__
  GTEST_SKIP() << "BufferPool needs Linux";
#endif
  const std::string filename = "bufferPoolBudgetTestStop.dat";
  const std::string contents = makeBufferPoolTestContents(8);
  writeBufferPoolTestFile(filename, contents);
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

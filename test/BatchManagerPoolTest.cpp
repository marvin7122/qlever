// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <fcntl.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <range/v3/range/conversion.hpp>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/Views.h"
#include "util/io_uring/BatchManagerPool.h"

using ad_utility::BatchManagerBase;
using ad_utility::BatchManagerPool;

namespace {

// A file with known contents, deleted on destruction.
struct TempFile {
  std::string name_;
  int fd_;
  explicit TempFile(const std::string& contents) {
    char name[] = "/tmp/batchManagerPoolTestXXXXXX";
    fd_ = mkstemp(name);
    AD_CORRECTNESS_CHECK(fd_ != -1,
                         "`mkstemp` failed to create a temporary file.");
    name_ = name;
    AD_CORRECTNESS_CHECK(::write(fd_, contents.data(), contents.size()) ==
                             static_cast<ssize_t>(contents.size()),
                         "Writing the contents of the temporary file failed.");
  }
  ~TempFile() {
    ::close(fd_);
    std::remove(name_.c_str());
  }
};

// Read four bytes at `offset` from the `file` via the `manager`.
std::string readFourBytesAt(BatchManagerBase& manager, const TempFile& file,
                            uint64_t offset) {
  std::string target(4, '\0');
  std::array<size_t, 1> numBytes{4};
  std::array<uint64_t, 1> offsets{offset};
  std::array<char*, 1> buffers{target.data()};
  manager.wait(manager.addBatch(file.fd_, numBytes, offsets, buffers));
  return target;
}

// Return `true` iff the `manager` reads with the synchronous fallback.
bool usesSyncBackend(BatchManagerBase& manager) {
  return dynamic_cast<ad_utility::BatchManager<ad_utility::SyncIoPolicy>*>(
             &manager) != nullptr;
}

}  // namespace

// _____________________________________________________________________________
// The pool has the requested number of managers, and they perform reads.
TEST(BatchManagerPool, managersReadThroughTheBackend) {
  BatchManagerPool pool{3, /*preferIoUring=*/false};
  EXPECT_EQ(pool.size(), 3u);
  EXPECT_FALSE(pool.usesIoUring());
  TempFile file{"AAAABBBBCCCC"};
  auto lease = pool.acquire();
  EXPECT_TRUE(usesSyncBackend(*lease));
  EXPECT_EQ(readFourBytesAt(*lease, file, 4), "BBBB");
  EXPECT_EQ(readFourBytesAt(*lease, file, 8), "CCCC");
}

// _____________________________________________________________________________
// With io_uring preferred, the managers use io_uring iff it is compiled in and
// works at runtime (it can be blocked, e.g. by seccomp inside Docker), and
// `usesIoUring()` tells which backend the managers have.
TEST(BatchManagerPool, backendMatchesUsesIoUring) {
  BatchManagerPool pool{2, /*preferIoUring=*/true};
  TempFile file{"AAAABBBBCCCC"};
  auto lease = pool.acquire();
  EXPECT_EQ(usesSyncBackend(*lease), !pool.usesIoUring());
#ifdef QLEVER_HAS_IO_URING
  if (!pool.usesIoUring()) {
    GTEST_SKIP() << "io_uring is compiled in, but not available at runtime "
                    "(e.g. blocked by seccomp inside Docker)";
  }
  EXPECT_FALSE(usesSyncBackend(*lease));
#else
  EXPECT_FALSE(pool.usesIoUring());
#endif
  EXPECT_EQ(readFourBytesAt(*lease, file, 0), "AAAA");
}

// _____________________________________________________________________________
// A manager goes back to the pool when its lease ends, also after an
// exception, and is handed out again.
TEST(BatchManagerPool, leaseGivesTheManagerBack) {
  BatchManagerPool pool{1, /*preferIoUring=*/false};
  BatchManagerBase* manager = nullptr;
  {
    auto lease = pool.acquire();
    manager = &*lease;
  }
  {
    auto lease = pool.acquire();
    EXPECT_EQ(&*lease, manager);
  }
  EXPECT_THROW(
      {
        auto lease = pool.acquire();
        throw std::runtime_error{"lookup failed"};
      },
      std::runtime_error);
  auto lease = pool.acquire();
  EXPECT_EQ(&*lease, manager);
}

// _____________________________________________________________________________
// A moved-from lease does not give the manager back; the moved-to lease does.
TEST(BatchManagerPool, leaseIsMovable) {
  BatchManagerPool pool{1, /*preferIoUring=*/false};
  BatchManagerBase* manager = nullptr;
  {
    auto lease = pool.acquire();
    manager = &*lease;
    auto movedLease = std::move(lease);
    EXPECT_TRUE(movedLease.isValid());
    EXPECT_EQ(&*movedLease, manager);
    EXPECT_FALSE(lease.isValid());
    AD_EXPECT_THROW_WITH_MESSAGE(*lease, ::testing::HasSubstr("moved-from"));
    AD_EXPECT_THROW_WITH_MESSAGE(lease->wait(0),
                                 ::testing::HasSubstr("moved-from"));
  }
  auto lease = pool.acquire();
  EXPECT_EQ(&*lease, manager);
}

// _____________________________________________________________________________
// When all managers are in use, `acquire` waits until one is given back and
// then receives exactly that one.
TEST(BatchManagerPool, acquireWaitsWhileAllManagersAreInUse) {
  using namespace std::chrono_literals;
  BatchManagerPool pool{1, /*preferIoUring=*/false};
  std::atomic<bool> acquired = false;
  BatchManagerBase* managerOfWaiter = nullptr;
  BatchManagerBase* manager = nullptr;
  std::thread waiter;
  {
    auto lease = pool.acquire();
    manager = &*lease;
    waiter = std::thread{[&]() {
      auto waitedLease = pool.acquire();
      managerOfWaiter = &*waitedLease;
      acquired = true;
    }};
    std::this_thread::sleep_for(50ms);
    EXPECT_FALSE(acquired);
  }
  waiter.join();
  EXPECT_TRUE(acquired);
  EXPECT_EQ(managerOfWaiter, manager);
}

// _____________________________________________________________________________
// Many threads share few managers, and no manager is in use by two threads at
// the same time.
TEST(BatchManagerPool, noManagerIsUsedByTwoThreadsAtOnce) {
  constexpr size_t numManagers = 3;
  constexpr size_t numThreads = 8;
  constexpr size_t numRoundsPerThread = 200;
  BatchManagerPool pool{numManagers, /*preferIoUring=*/false};

  // Learn the addresses of the managers to index the `inUse` counters.
  std::vector<BatchManagerBase*> managers;
  {
    std::vector<BatchManagerPool::Lease> leases;
    for ([[maybe_unused]] size_t i : ad_utility::integerRange(numManagers)) {
      leases.push_back(pool.acquire());
    }
    managers = ::ranges::to_vector(
        leases |
        ql::views::transform([](const auto& lease) { return &*lease; }));
  }
  auto indexOf = [&managers](BatchManagerBase* manager) {
    auto it = ql::ranges::find(managers, manager);
    if (it == managers.end()) {
      ADD_FAILURE() << "unknown manager";
      return size_t{0};
    }
    return static_cast<size_t>(it - managers.begin());
  };

  std::array<std::atomic<int>, numManagers> inUse{};
  std::atomic<int> overlaps = 0;
  std::vector<std::thread> threads;
  for ([[maybe_unused]] size_t t : ad_utility::integerRange(numThreads)) {
    threads.emplace_back([&]() {
      for ([[maybe_unused]] size_t round :
           ad_utility::integerRange(numRoundsPerThread)) {
        auto lease = pool.acquire();
        auto& counter = inUse[indexOf(&*lease)];
        if (counter.fetch_add(1) != 0) {
          ++overlaps;
        }
        std::this_thread::yield();
        counter.fetch_sub(1);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(overlaps, 0);
}

// _____________________________________________________________________________
// A pool without managers is rejected.
TEST(BatchManagerPool, zeroManagersAreRejected) {
  AD_EXPECT_THROW_WITH_MESSAGE((BatchManagerPool{0, /*preferIoUring=*/false}),
                               ::testing::HasSubstr("at least one manager"));
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::setUpGlobalBatchManagerPool(0),
                               ::testing::HasSubstr("at least one manager"));
}

namespace {

// Exit with code 0 if `check` holds, and with code 1 and the `message` on
// `stderr` otherwise.
void exitWith(bool check, std::string_view message) {
  if (!check) {
    std::cerr << message << std::endl;
    std::exit(1);
  }
}

// The checks of the process-wide pool. Each of them runs in a fresh process
// (see below), because the pool cannot be reset.
void setUpBeforeTheFirstUse() {
  ad_utility::setUpGlobalBatchManagerPool(2);
  auto& pool = ad_utility::globalBatchManagerPool();
  exitWith(pool.size() == 2, "the pool does not have two managers");
  exitWith(&pool == &ad_utility::globalBatchManagerPool(),
           "the pool was created twice");
  // Setting up the same size again is not a conflict.
  ad_utility::setUpGlobalBatchManagerPool(2);
  try {
    ad_utility::setUpGlobalBatchManagerPool(3);
    exitWith(false, "a second size was accepted");
  } catch (const std::exception& e) {
    exitWith(
        std::string_view{e.what()}.find(
            "must be set up before its first use") != std::string_view::npos,
        e.what());
  }
  exitWith(pool.size() == 2, "the pool changed its size");
  TempFile file{"AAAABBBBCCCC"};
  auto lease = pool.acquire();
  exitWith(readFourBytesAt(*lease, file, 8) == "CCCC", "the read failed");
  std::exit(0);
}

void setUpAfterTheFirstUse() {
  const size_t size = ad_utility::globalBatchManagerPool().size();
  exitWith(size >= 1, "the default pool has no manager");
  try {
    ad_utility::setUpGlobalBatchManagerPool(size + 1);
    exitWith(false, "a pool that was used before its set-up was accepted");
  } catch (const std::exception& e) {
    exitWith(
        std::string_view{e.what()}.find(
            "must be set up before its first use") != std::string_view::npos,
        e.what());
  }
  std::exit(0);
}

}  // namespace

// _____________________________________________________________________________
// The process-wide pool gets the size of its set-up, and a set-up with another
// size after the first use throws. Each check runs in a fresh process (the
// "threadsafe" style of death tests re-executes the test binary), so it does
// not depend on whether another test of the same process created the pool.
TEST(BatchManagerPool, globalPoolIsSetUpBeforeItsFirstUse) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_EXIT(setUpBeforeTheFirstUse(), ::testing::ExitedWithCode(0), "");
  EXPECT_EXIT(setUpAfterTheFirstUse(), ::testing::ExitedWithCode(0), "");
}

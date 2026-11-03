// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <range/v3/range/conversion.hpp>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "backports/algorithm.h"
#include "util/BatchManagerPool.h"
#include "util/Exception.h"
#include "util/Views.h"

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
    AD_CORRECTNESS_CHECK(fd_ != -1);
    name_ = name;
    AD_CORRECTNESS_CHECK(::write(fd_, contents.data(), contents.size()) ==
                         static_cast<ssize_t>(contents.size()));
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
    EXPECT_ANY_THROW(*lease);
    EXPECT_ANY_THROW(lease->wait(0));
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
  EXPECT_THROW((BatchManagerPool{0, /*preferIoUring=*/false}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
// The process-wide pool takes its configuration from the calls before its
// creation and keeps it afterwards, when it rejects another configuration.
// NOTE: The process-wide pool cannot be reset. When this test runs in its own
// binary, it is the first user of the pool and creates it with two synchronous
// managers. In the single test binary (`SINGLE_TEST_BINARY=ON`) the pool
// already exists with the default configuration, so only the assertions that
// hold for an existing pool are made.
TEST(BatchManagerPool, globalPoolIsConfiguredBeforeItsCreation) {
  const bool configured = ad_utility::trySetGlobalBatchManagerPoolConfig(
      2, /*preferIoUring=*/false);
  auto& pool = ad_utility::globalBatchManagerPool();
  EXPECT_EQ(&pool, &ad_utility::globalBatchManagerPool());
  if (configured) {
    EXPECT_EQ(pool.size(), 2u);
    EXPECT_FALSE(pool.usesIoUring());
    EXPECT_TRUE(ad_utility::trySetGlobalBatchManagerPoolConfig(
        2, /*preferIoUring=*/false));
    EXPECT_FALSE(ad_utility::trySetGlobalBatchManagerPoolConfig(
        2, /*preferIoUring=*/true));
  }
  const size_t size = pool.size();
  const bool usesIoUring = pool.usesIoUring();
  EXPECT_FALSE(ad_utility::trySetGlobalBatchManagerPoolConfig(
      size + 1, /*preferIoUring=*/false));
  EXPECT_FALSE(ad_utility::trySetGlobalBatchManagerPoolConfig(
      size + 1, /*preferIoUring=*/true));
  EXPECT_EQ(pool.size(), size);
  EXPECT_EQ(pool.usesIoUring(), usesIoUring);
  EXPECT_THROW(ad_utility::trySetGlobalBatchManagerPoolConfig(
                   0, /*preferIoUring=*/false),
               ad_utility::Exception);
  TempFile file{"AAAABBBBCCCC"};
  auto lease = pool.acquire();
  EXPECT_EQ(readFourBytesAt(*lease, file, 8), "CCCC");
}

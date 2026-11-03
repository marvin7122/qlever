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
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "util/BatchManagerPool.h"

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
    EXPECT_NE(fd_, -1);
    name_ = name;
    EXPECT_EQ(::write(fd_, contents.data(), contents.size()),
              static_cast<ssize_t>(contents.size()));
  }
  ~TempFile() {
    ::close(fd_);
    std::remove(name_.c_str());
  }
};

// Read four bytes at `offset` from the `file` via the `manager`.
std::string readFour(BatchManagerBase& manager, const TempFile& file,
                     uint64_t offset) {
  std::string target(4, '\0');
  std::array<size_t, 1> numBytes{4};
  std::array<uint64_t, 1> offsets{offset};
  std::array<char*, 1> buffers{target.data()};
  manager.wait(manager.addBatch(file.fd_, numBytes, offsets, buffers));
  return target;
}

bool isSync(BatchManagerBase& manager) {
  return dynamic_cast<ad_utility::BatchManager<ad_utility::SyncIoPolicy>*>(
             &manager) != nullptr;
}

}  // namespace

// The pool has the requested number of managers, and they perform reads.
TEST(BatchManagerPool, managersReadThroughTheBackend) {
  BatchManagerPool pool{3, /*preferIoUring=*/false};
  EXPECT_EQ(pool.size(), 3u);
  EXPECT_FALSE(pool.usesIoUring());
  TempFile file{"AAAABBBBCCCC"};
  auto lease = pool.acquire();
  EXPECT_TRUE(isSync(*lease));
  EXPECT_EQ(readFour(*lease, file, 4), "BBBB");
  EXPECT_EQ(readFour(*lease, file, 8), "CCCC");
}

// With io_uring preferred, the backend depends on whether io_uring is
// compiled in and works at runtime (it can be blocked, e.g. by seccomp inside
// Docker). Either way, `usesIoUring()` tells which backend the managers have.
TEST(BatchManagerPool, backendMatchesUsesIoUring) {
  BatchManagerPool pool{2, /*preferIoUring=*/true};
  TempFile file{"AAAABBBBCCCC"};
  auto lease = pool.acquire();
  EXPECT_EQ(isSync(*lease), !pool.usesIoUring());
#ifndef QLEVER_HAS_IO_URING
  EXPECT_FALSE(pool.usesIoUring());
#endif
  EXPECT_EQ(readFour(*lease, file, 0), "AAAA");
}

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

// A moved-from lease does not give the manager back; the moved-to lease does.
TEST(BatchManagerPool, leaseIsMovable) {
  BatchManagerPool pool{1, /*preferIoUring=*/false};
  BatchManagerBase* manager = nullptr;
  {
    auto lease = pool.acquire();
    manager = &*lease;
    auto movedLease = std::move(lease);
    EXPECT_EQ(&*movedLease, manager);
  }
  auto lease = pool.acquire();
  EXPECT_EQ(&*lease, manager);
}

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
    for (size_t i = 0; i < numManagers; ++i) {
      leases.push_back(pool.acquire());
      managers.push_back(&*leases.back());
    }
  }
  auto indexOf = [&managers](BatchManagerBase* manager) {
    for (size_t i = 0; i < managers.size(); ++i) {
      if (managers[i] == manager) {
        return i;
      }
    }
    ADD_FAILURE() << "unknown manager";
    return size_t{0};
  };

  std::array<std::atomic<int>, numManagers> inUse{};
  std::atomic<int> overlaps = 0;
  std::vector<std::thread> threads;
  for (size_t t = 0; t < numThreads; ++t) {
    threads.emplace_back([&]() {
      for (size_t round = 0; round < numRoundsPerThread; ++round) {
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

// A pool without managers is rejected.
TEST(BatchManagerPool, zeroManagersAreRejected) {
  EXPECT_ANY_THROW((BatchManagerPool{0, /*preferIoUring=*/false}));
}

// The process-wide pool takes its configuration from the calls before its
// creation, and reports a different configuration afterwards.
TEST(BatchManagerPool, globalPoolIsConfiguredBeforeItsCreation) {
  EXPECT_TRUE(ad_utility::trySetGlobalBatchManagerPoolConfig(
      2, /*preferIoUring=*/false));
  auto& pool = ad_utility::globalBatchManagerPool();
  EXPECT_EQ(pool.size(), 2u);
  EXPECT_FALSE(pool.usesIoUring());
  EXPECT_EQ(&pool, &ad_utility::globalBatchManagerPool());
  EXPECT_TRUE(ad_utility::trySetGlobalBatchManagerPoolConfig(
      2, /*preferIoUring=*/false));
  EXPECT_FALSE(ad_utility::trySetGlobalBatchManagerPoolConfig(
      3, /*preferIoUring=*/false));
  EXPECT_FALSE(ad_utility::trySetGlobalBatchManagerPoolConfig(
      2, /*preferIoUring=*/true));
  EXPECT_ANY_THROW(ad_utility::trySetGlobalBatchManagerPoolConfig(
      0, /*preferIoUring=*/false));
  TempFile file{"AAAABBBBCCCC"};
  auto lease = pool.acquire();
  EXPECT_EQ(readFour(*lease, file, 8), "CCCC");
}

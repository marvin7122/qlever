// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

#include "util/BatchManagerPool.h"
#include "util/IoUringManager.h"

namespace {

using ad_utility::BatchManagerBase;
using ad_utility::BatchManagerPool;
using namespace std::chrono_literals;

// A manager that reads nothing. It only records whether it is in use by more
// than one thread at a time, which the pool must prevent.
class FakeManager : public BatchManagerBase {
 public:
  BatchHandle addBatch(int, ql::span<const size_t>, ql::span<const uint64_t>,
                       ql::span<char*>) override {
    return 0;
  }
  void wait(BatchHandle) override {}

  // Mark the manager as in use; returns false if it already was.
  bool enter() { return !inUse_.exchange(true); }
  void leave() { inUse_ = false; }

 private:
  std::atomic<bool> inUse_ = false;
};

// A factory that counts how many managers it has created.
BatchManagerPool::Factory countingFactory(std::atomic<size_t>& numCreated) {
  return [&numCreated]() -> std::unique_ptr<BatchManagerBase> {
    ++numCreated;
    return std::make_unique<FakeManager>();
  };
}

// The pool creates no manager before the first `acquire`, and only as many as
// are in use at the same time.
TEST(BatchManagerPool, managersAreCreatedOnDemand) {
  std::atomic<size_t> created = 0;
  BatchManagerPool pool{4, countingFactory(created)};
  EXPECT_EQ(created, 0u);
  EXPECT_EQ(pool.numCreated(), 0u);
  {
    auto first = pool.acquire();
    EXPECT_EQ(created, 1u);
    auto second = pool.acquire();
    auto third = pool.acquire();
    EXPECT_EQ(created, 3u);
    EXPECT_EQ(pool.numCreated(), 3u);
    EXPECT_NE(&*first, &*second);
    EXPECT_NE(&*first, &*third);
    EXPECT_NE(&*second, &*third);
  }
  EXPECT_EQ(created, 3u);
}

// A manager that was given back is used again before a new one is created.
TEST(BatchManagerPool, anIdleManagerIsReused) {
  std::atomic<size_t> created = 0;
  BatchManagerPool pool{4, countingFactory(created)};
  BatchManagerBase* first = nullptr;
  {
    auto lease = pool.acquire();
    first = &*lease;
  }
  auto again = pool.acquire();
  EXPECT_EQ(&*again, first);
  EXPECT_EQ(created, 1u);
}

// If `maxManagers` managers are in use, `acquire` waits until one is given back
// and then returns that one.
TEST(BatchManagerPool, acquireWaitsAtTheBound) {
  std::atomic<size_t> created = 0;
  BatchManagerPool pool{2, countingFactory(created)};
  auto first = pool.acquire();
  auto second = pool.acquire();
  BatchManagerBase* firstManager = &*first;

  std::promise<BatchManagerBase*> acquired;
  auto acquiredFuture = acquired.get_future();
  std::thread waiter{[&]() {
    auto lease = pool.acquire();
    acquired.set_value(&*lease);
  }};
  // No manager is free, so the waiter must still wait.
  EXPECT_EQ(acquiredFuture.wait_for(100ms), std::future_status::timeout);
  EXPECT_EQ(created, 2u);

  // Give one back: the waiter gets exactly this manager, no third is created.
  { auto released = std::move(first); }
  ASSERT_EQ(acquiredFuture.wait_for(10s), std::future_status::ready);
  EXPECT_EQ(acquiredFuture.get(), firstManager);
  waiter.join();
  EXPECT_EQ(created, 2u);
}

// A lease gives its manager back when the user throws.
TEST(BatchManagerPool, aManagerIsGivenBackAfterAnException) {
  std::atomic<size_t> created = 0;
  BatchManagerPool pool{1, countingFactory(created)};
  EXPECT_THROW(
      {
        auto lease = pool.acquire();
        throw std::runtime_error{"read failed"};
      },
      std::runtime_error);
  // With a bound of one manager, this would wait forever if it was lost.
  auto lease = pool.acquire();
  EXPECT_EQ(created, 1u);
}

// A failing factory makes `acquire` throw and leaves the pool usable.
TEST(BatchManagerPool, aFailingFactoryLeavesThePoolUnchanged) {
  std::atomic<size_t> calls = 0;
  BatchManagerPool pool{1, [&calls]() -> std::unique_ptr<BatchManagerBase> {
                          if (calls++ == 0) {
                            throw std::runtime_error{"io_uring_setup failed"};
                          }
                          return std::make_unique<FakeManager>();
                        }};
  EXPECT_THROW(pool.acquire(), std::runtime_error);
  EXPECT_EQ(pool.numCreated(), 0u);
  // The failed creation did not use up the only slot.
  auto lease = pool.acquire();
  EXPECT_EQ(pool.numCreated(), 1u);
}

// Many threads share a few managers: no manager is used by two threads at the
// same time, and no more than the bound are created.
TEST(BatchManagerPool, concurrentUsersNeverShareAManager) {
  std::atomic<size_t> created = 0;
  constexpr size_t maxManagers = 3;
  BatchManagerPool pool{maxManagers, countingFactory(created)};
  std::atomic<bool> shared = false;
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < 200; ++i) {
        auto lease = pool.acquire();
        auto& manager = static_cast<FakeManager&>(*lease);
        if (!manager.enter()) {
          shared = true;
        }
        std::this_thread::yield();
        manager.leave();
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_FALSE(shared);
  EXPECT_LE(created, maxManagers);
  EXPECT_GE(created, 1u);
}

// The bound must be positive.
TEST(BatchManagerPool, aBoundOfZeroIsRejected) {
  std::atomic<size_t> created = 0;
  EXPECT_ANY_THROW(BatchManagerPool(0, countingFactory(created)));
}

}  // namespace

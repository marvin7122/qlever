// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "util/Exception.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/Views.h"
#include "util/io_uring/BatchManagerPool.h"

using ad_utility::BatchManagerBase;
using ad_utility::BatchManagerPool;

namespace {

// A file with known contents, removed on destruction.
class TempFile {
 public:
  explicit TempFile(std::string_view contents)
      : path_{absl::StrCat(gtestCurrentTestName(), ".tmp")},
        file_{path_, "w+b"} {
    file_.write(contents.data(), contents.size());
    // Make the bytes visible to the reads on `fd()`.
    file_.flush();
  }
  ~TempFile() {
    file_.close();
    std::remove(path_.c_str());
  }
  int fd() const { return file_.fd(); }

 private:
  std::string path_;
  ad_utility::File file_;
};

// Read four bytes at `offset` from the `file` via the `manager`.
std::string readFourBytesAt(BatchManagerBase& manager, const TempFile& file,
                            uint64_t offset) {
  std::string target(4, '\0');
  std::array<size_t, 1> numBytes{4};
  std::array<uint64_t, 1> offsets{offset};
  std::array<char*, 1> buffers{target.data()};
  manager.wait(manager.addBatch(file.fd(), numBytes, offsets, buffers));
  return target;
}

// Return `true` iff the `manager` reads with the synchronous fallback.
bool usesSyncBackend(BatchManagerBase& manager) {
  return dynamic_cast<ad_utility::BatchManager<ad_utility::SyncIoPolicy>*>(
             &manager) != nullptr;
}

// Create synchronous managers only.
std::unique_ptr<BatchManagerBase> makeSyncManager(bool& preferIoUring) {
  preferIoUring = false;
  return ad_utility::makeBatchManager(preferIoUring);
}

// A manager that performs no reads, but knows the number of its creation and
// whether it stands for an io_uring manager. Its `wait` throws, like a batch
// with a failed read.
struct FakeManager : BatchManagerBase {
  size_t id_;
  bool isIoUring_;
  std::atomic<size_t>* numberOfLiveManagers_;
  FakeManager(size_t id, bool isIoUring,
              std::atomic<size_t>* numberOfLiveManagers)
      : id_{id},
        isIoUring_{isIoUring},
        numberOfLiveManagers_{numberOfLiveManagers} {
    ++*numberOfLiveManagers_;
  }
  ~FakeManager() override { --*numberOfLiveManagers_; }
  BatchHandle addBatch(int, ql::span<const size_t>, ql::span<const uint64_t>,
                       ql::span<char*>) override {
    return 0;
  }
  void wait(BatchHandle) override { throw std::runtime_error{"read failed"}; }
};

// A factory of `FakeManager`s with the protocol of `makeBatchManager`. The
// io_uring setup fails in the call with the number `failingIoUringSetup`, and
// the call with the number `throwingCall` throws.
struct FakeFactory {
  std::optional<size_t> failingIoUringSetup;
  std::optional<size_t> throwingCall;
  size_t numberOfCalls = 0;
  std::atomic<size_t> numberOfLiveManagers = 0;

  BatchManagerPool::MakeManager makeManager() {
    return [this](bool& preferIoUring) -> std::unique_ptr<BatchManagerBase> {
      const size_t call = numberOfCalls++;
      if (call == throwingCall) {
        throw std::runtime_error{"cannot create a manager"};
      }
      if (call == failingIoUringSetup) {
        preferIoUring = false;
      }
      return std::make_unique<FakeManager>(call, preferIoUring,
                                           &numberOfLiveManagers);
    };
  }
};

const FakeManager& fake(const BatchManagerPool::Lease& lease) {
  return dynamic_cast<const FakeManager&>(*lease);
}

// Acquire all managers of the `pool` and return them in a vector.
std::vector<BatchManagerPool::Lease> acquireAll(BatchManagerPool& pool) {
  std::vector<BatchManagerPool::Lease> leases;
  for ([[maybe_unused]] size_t i : ad_utility::integerRange(pool.size())) {
    leases.push_back(pool.acquire());
  }
  return leases;
}

}  // namespace

// _____________________________________________________________________________
// The pool has the requested number of managers, and they perform reads.
TEST(BatchManagerPool, managersReadThroughTheBackend) {
  BatchManagerPool pool{3, makeSyncManager};
  EXPECT_EQ(pool.size(), 3u);
  EXPECT_FALSE(pool.usesIoUring());
  TempFile file{"AAAABBBBCCCC"};
  auto lease = pool.acquire();
  EXPECT_TRUE(usesSyncBackend(*lease));
  EXPECT_EQ(readFourBytesAt(*lease, file, 4), "BBBB");
  EXPECT_EQ(readFourBytesAt(*lease, file, 8), "CCCC");
}

// _____________________________________________________________________________
// With the default factory, the managers use io_uring iff it is compiled in and
// works at runtime (it can be blocked, e.g. by seccomp inside Docker), and
// `usesIoUring()` tells which backend the managers have.
TEST(BatchManagerPool, backendMatchesUsesIoUring) {
  BatchManagerPool pool{2};
  TempFile file{"AAAABBBBCCCC"};
  for (auto& lease : acquireAll(pool)) {
    EXPECT_EQ(usesSyncBackend(*lease), !pool.usesIoUring());
    EXPECT_EQ(readFourBytesAt(*lease, file, 0), "AAAA");
  }
#ifndef QLEVER_HAS_IO_URING
  EXPECT_FALSE(pool.usesIoUring());
#endif
}

// _____________________________________________________________________________
// If all io_uring setups succeed, the pool uses io_uring and creates each
// manager once.
TEST(BatchManagerPool, allIoUringSetupsSucceed) {
  FakeFactory factory;
  BatchManagerPool pool{3, factory.makeManager()};
  EXPECT_TRUE(pool.usesIoUring());
  EXPECT_EQ(factory.numberOfCalls, 3u);
  for (const auto& lease : acquireAll(pool)) {
    EXPECT_TRUE(fake(lease).isIoUring_);
  }
}

// _____________________________________________________________________________
// If the io_uring setup fails after some io_uring managers were created, these
// are replaced, so that all managers are synchronous.
TEST(BatchManagerPool, aFailedIoUringSetupMakesAllManagersSynchronous) {
  FakeFactory factory{.failingIoUringSetup = 2};
  BatchManagerPool pool{4, factory.makeManager()};
  EXPECT_FALSE(pool.usesIoUring());
  // Three calls until the failure and one more for the fourth manager, then
  // four for the synchronous managers that replace them.
  EXPECT_EQ(factory.numberOfCalls, 8u);
  EXPECT_EQ(factory.numberOfLiveManagers, 4u);
  for (const auto& lease : acquireAll(pool)) {
    EXPECT_FALSE(fake(lease).isIoUring_);
  }
}

// _____________________________________________________________________________
// If the very first io_uring setup fails, all managers are synchronous right
// away and nothing has to be replaced.
TEST(BatchManagerPool, aFailedFirstIoUringSetupNeedsNoReplacement) {
  FakeFactory factory{.failingIoUringSetup = 0};
  BatchManagerPool pool{4, factory.makeManager()};
  EXPECT_FALSE(pool.usesIoUring());
  EXPECT_EQ(factory.numberOfCalls, 4u);
  for (const auto& lease : acquireAll(pool)) {
    EXPECT_FALSE(fake(lease).isIoUring_);
  }
}

// _____________________________________________________________________________
// If creating a manager throws, the constructor throws and the managers that
// were already created are destroyed.
TEST(BatchManagerPool, aThrowingFactoryLeavesNoManagerBehind) {
  FakeFactory factory{.throwingCall = 2};
  EXPECT_THROW((BatchManagerPool{4, factory.makeManager()}),
               std::runtime_error);
  EXPECT_EQ(factory.numberOfCalls, 3u);
  EXPECT_EQ(factory.numberOfLiveManagers, 0u);
}

// _____________________________________________________________________________
// A manager goes back to the pool when its lease ends and is handed out again.
// A manager whose lease ends with an exception (its batch may still have reads
// in flight) is destroyed and replaced by a new one.
TEST(BatchManagerPool, aManagerIsReplacedAfterAnException) {
  FakeFactory factory;
  BatchManagerPool pool{1, factory.makeManager()};
  { EXPECT_EQ(fake(pool.acquire()).id_, 0u); }
  { EXPECT_EQ(fake(pool.acquire()).id_, 0u); }
  EXPECT_THROW(
      {
        auto lease = pool.acquire();
        lease->wait(lease->addBatch(0, {}, {}, {}));
      },
      std::runtime_error);
  EXPECT_EQ(factory.numberOfLiveManagers, 1u);
  EXPECT_EQ(fake(pool.acquire()).id_, 1u);
  EXPECT_TRUE(pool.usesIoUring());
}

// _____________________________________________________________________________
// If the io_uring setup fails for the manager that replaces a failed one, the
// pool no longer uses io_uring for all its managers.
TEST(BatchManagerPool, aSynchronousReplacementIsReported) {
  FakeFactory factory{.failingIoUringSetup = 2};
  BatchManagerPool pool{2, factory.makeManager()};
  EXPECT_TRUE(pool.usesIoUring());
  EXPECT_THROW(
      {
        auto lease = pool.acquire();
        throw std::runtime_error{"lookup failed"};
      },
      std::runtime_error);
  EXPECT_FALSE(pool.usesIoUring());
  EXPECT_EQ(factory.numberOfLiveManagers, 2u);
}

// _____________________________________________________________________________
// A lease that is created and ended while an unrelated exception is already in
// flight (in a destructor during unwinding) gives its manager back for reuse.
TEST(BatchManagerPool, aLeaseInsideUnwindingIsNotAFailure) {
  FakeFactory factory;
  BatchManagerPool pool{1, factory.makeManager()};
  struct LeaseInDestructor {
    BatchManagerPool* pool_;
    ~LeaseInDestructor() { [[maybe_unused]] auto lease = pool_->acquire(); }
  };
  EXPECT_THROW(
      {
        LeaseInDestructor leaseInDestructor{&pool};
        throw std::runtime_error{"unrelated"};
      },
      std::runtime_error);
  EXPECT_EQ(fake(pool.acquire()).id_, 0u);
  EXPECT_EQ(factory.numberOfCalls, 1u);
}

// _____________________________________________________________________________
// A moved-from lease does not give the manager back; the moved-to lease does.
TEST(BatchManagerPool, leaseIsMovable) {
  FakeFactory factory;
  BatchManagerPool pool{1, factory.makeManager()};
  {
    auto lease = pool.acquire();
    auto movedLease = std::move(lease);
    EXPECT_TRUE(movedLease.isValid());
    EXPECT_EQ(fake(movedLease).id_, 0u);
    EXPECT_FALSE(lease.isValid());
    AD_EXPECT_THROW_WITH_MESSAGE(*lease, ::testing::HasSubstr("moved-from"));
    AD_EXPECT_THROW_WITH_MESSAGE(lease->wait(0),
                                 ::testing::HasSubstr("moved-from"));
  }
  EXPECT_EQ(fake(pool.acquire()).id_, 0u);
  EXPECT_EQ(factory.numberOfCalls, 1u);
}

// _____________________________________________________________________________
// When all managers are in use, `acquire` waits until one is given back and
// then receives exactly that one.
TEST(BatchManagerPool, acquireWaitsWhileAllManagersAreInUse) {
  using namespace std::chrono_literals;
  FakeFactory factory;
  BatchManagerPool pool{2, factory.makeManager()};
  auto leases = acquireAll(pool);
  const size_t idOfGivenBack = fake(leases.at(1)).id_;
  std::atomic<bool> aboutToAcquire = false;
  std::atomic<bool> acquired = false;
  std::optional<size_t> idOfWaiter;
  std::thread waiter{[&]() {
    aboutToAcquire = true;
    auto lease = pool.acquire();
    idOfWaiter = fake(lease).id_;
    acquired = true;
  }};
  while (!aboutToAcquire) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(acquired);
  leases.pop_back();
  waiter.join();
  EXPECT_TRUE(acquired);
  EXPECT_EQ(idOfWaiter, idOfGivenBack);
}

// _____________________________________________________________________________
// Many threads share few managers, and no manager is in use by two threads at
// the same time.
TEST(BatchManagerPool, noManagerIsUsedByTwoThreadsAtOnce) {
  constexpr size_t numManagers = 3;
  constexpr size_t numThreads = 8;
  constexpr size_t numRoundsPerThread = 200;
  FakeFactory factory;
  BatchManagerPool pool{numManagers, factory.makeManager()};

  std::array<std::atomic<int>, numManagers> inUse{};
  std::atomic<int> overlaps = 0;
  std::vector<std::thread> threads;
  for ([[maybe_unused]] size_t t : ad_utility::integerRange(numThreads)) {
    threads.emplace_back([&]() {
      for ([[maybe_unused]] size_t round :
           ad_utility::integerRange(numRoundsPerThread)) {
        auto lease = pool.acquire();
        auto& counter = inUse.at(fake(lease).id_);
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
  EXPECT_EQ(factory.numberOfCalls, numManagers);
}

// _____________________________________________________________________________
// A pool without managers is rejected.
TEST(BatchManagerPool, zeroManagersAreRejected) {
  AD_EXPECT_THROW_WITH_MESSAGE((BatchManagerPool{0, makeSyncManager}),
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

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cstring>
#include <thread>
#include <vector>

#include "util/ZeroCopySocketSender.h"

using namespace ad_utility;

// _____________________________________________________________________________
TEST(ZeroCopyBufferPoolTest, BasicAcquireAndRelease) {
  constexpr size_t numBuffers = 8;
  constexpr size_t bufferSize = 4096;  // 4KB

  ZeroCopyBufferPool pool(numBuffers, bufferSize);
  EXPECT_EQ(pool.numBuffers(), numBuffers);
  EXPECT_EQ(pool.bufferSizeBytes(), bufferSize);
  EXPECT_EQ(pool.availableSlots(), numBuffers);

  std::vector<uint32_t> acquired;
  for (size_t i = 0; i < numBuffers; ++i) {
    auto slot = pool.acquireSlot();
    ASSERT_TRUE(slot.has_value());
    EXPECT_TRUE(pool.isSlotInUse(slot.value()));
    acquired.push_back(slot.value());
  }

  EXPECT_EQ(pool.availableSlots(), 0u);
  EXPECT_FALSE(pool.acquireSlot().has_value());

  // Test span access
  for (uint32_t slot : acquired) {
    auto span = pool.getSlotSpan(slot);
    EXPECT_EQ(span.size(), bufferSize);
    std::memset(span.data(), 0xAB, span.size());
    EXPECT_EQ(static_cast<unsigned char>(span[0]), 0xAB);
  }

  // Release all slots
  for (uint32_t slot : acquired) {
    pool.releaseSlot(slot);
    EXPECT_FALSE(pool.isSlotInUse(slot));
  }

  EXPECT_EQ(pool.availableSlots(), numBuffers);
}

// _____________________________________________________________________________
TEST(ZeroCopySocketSenderTest, TransmissionOverSocketPair) {
  int sv[2];
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

  // RAII cleanup: without this, an `AD_THROW` below would leak the
  // descriptors (and terminate on the still-joinable receiver thread).
  struct FdGuard {
    int fd = -1;
    ~FdGuard() {
      if (fd >= 0) {
        ::close(fd);
      }
    }
  };
  FdGuard sendGuard{sv[0]};
  FdGuard recvGuard{sv[1]};
  int sendFd = sv[0];
  int recvFd = sv[1];

  // Bound the receiver's blocking `recv`: without a timeout a sender-side
  // failure hangs the test forever instead of failing it.
  struct timeval recvTimeout {};
  recvTimeout.tv_sec = 10;
  ASSERT_EQ(::setsockopt(recvFd, SOL_SOCKET, SO_RCVTIMEO, &recvTimeout,
                         sizeof(recvTimeout)),
            0);

  ZeroCopySenderConfig config;
  config.ringEntries = 16;
  config.numBuffers = 8;
  config.bufferSizeBytes = 4096;
  config.useRegisteredBuffers = true;
  config.useZeroCopy = true;

  ZeroCopySocketSender sender(config);
  EXPECT_EQ(sender.inFlightRequests(), 0u);
  EXPECT_EQ(sender.inFlightBuffers(), 0u);

  constexpr size_t numChunks = 20;
  constexpr size_t chunkSize = 1024;
  std::vector<char> expectedData(numChunks * chunkSize);
  for (size_t i = 0; i < expectedData.size(); ++i) {
    expectedData[i] = static_cast<char>((i * 37 + 13) % 256);
  }

  std::vector<char> receivedData(numChunks * chunkSize, 0);

  // Background thread to receive data
  std::thread receiverThread([&]() {
    size_t totalReceived = 0;
    while (totalReceived < expectedData.size()) {
      ssize_t bytes = ::recv(recvFd, receivedData.data() + totalReceived,
                             expectedData.size() - totalReceived, 0);
      if (bytes <= 0) {
        break;
      }
      totalReceived += static_cast<size_t>(bytes);
    }
  });

  // Sender thread loop
  for (size_t i = 0; i < numChunks; ++i) {
    uint32_t slot = sender.acquireBuffer();
    auto span = sender.getSlotSpan(slot);
    std::memcpy(span.data(), expectedData.data() + (i * chunkSize), chunkSize);
    sender.sendChunk(sendFd, slot, chunkSize);
  }

  // Join on all paths: `flushAndDrainAll` and the `EXPECT`s above can throw
  // (`AD_THROW`), which would otherwise terminate on the joinable thread.
  struct ThreadJoinGuard {
    std::thread& thread;
    ~ThreadJoinGuard() {
      if (thread.joinable()) {
        thread.join();
      }
    }
  };
  ThreadJoinGuard joinGuard{receiverThread};

  sender.flushAndDrainAll();
  EXPECT_EQ(sender.inFlightRequests(), 0u);
  EXPECT_EQ(sender.inFlightBuffers(), 0u);
  EXPECT_EQ(sender.bufferPool().availableSlots(), config.numBuffers);

  receiverThread.join();

  EXPECT_EQ(receivedData, expectedData);
}

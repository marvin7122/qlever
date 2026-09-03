// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <thread>
#include <vector>

#include "util/Invariants.h"
#include "util/ZeroCopySocketSender.h"

using namespace ad_utility;

static_assert(InvariantStatefulClass<ZeroCopyBufferPool>,
              "ZeroCopyBufferPool must satisfy InvariantStatefulClass");
static_assert(InvariantStatefulClass<ZeroCopySocketSender>,
              "ZeroCopySocketSender must satisfy InvariantStatefulClass");

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

  int sendFd = sv[0];
  int recvFd = sv[1];

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

    // Sender loop
  for (size_t i = 0; i < numChunks; ++i) {
    uint32_t slot = sender.acquireBuffer();
    auto span = sender.getSlotSpan(slot);
    std::memcpy(span.data(), expectedData.data() + (i * chunkSize), chunkSize);
    sender.sendChunk(sendFd, slot, chunkSize);
  }

  sender.flushAndDrainAll();
  EXPECT_EQ(sender.inFlightRequests(), 0u);
  EXPECT_EQ(sender.inFlightBuffers(), 0u);
  EXPECT_EQ(sender.bufferPool().availableSlots(), config.numBuffers);

  receiverThread.join();

  ::close(sendFd);
  ::close(recvFd);

  EXPECT_EQ(receivedData, expectedData);
}

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <thread>
#include <tuple>
#include <utility>
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

namespace {
// Closes a file descriptor on scope exit: without this, an `AD_THROW` in the
// sender would leak the descriptors.
struct FdGuard {
  int fd = -1;
  explicit FdGuard(int f) : fd{f} {}
  FdGuard(const FdGuard&) = delete;
  FdGuard& operator=(const FdGuard&) = delete;
  ~FdGuard() {
    if (fd >= 0) {
      ::close(fd);
    }
  }
};

// Joins a thread on scope exit, so that an exception in the sender does not
// terminate the process on a still-joinable receiver thread.
struct ThreadJoinGuard {
  std::thread& thread;
  ~ThreadJoinGuard() {
    if (thread.joinable()) {
      thread.join();
    }
  }
};

// Create a connected TCP connection over the loopback interface and return
// the sending and the receiving end. TCP (unlike a Unix socket pair) supports
// `IORING_OP_SEND_ZC`, so the zero-copy path is really exercised.
std::pair<int, int> makeLoopbackTcpConnection() {
  FdGuard listener{::socket(AF_INET, SOCK_STREAM, 0)};
  AD_CORRECTNESS_CHECK(listener.fd >= 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  socklen_t addrLen = sizeof(addr);
  AD_CORRECTNESS_CHECK(
      ::bind(listener.fd, reinterpret_cast<sockaddr*>(&addr), addrLen) == 0);
  AD_CORRECTNESS_CHECK(::getsockname(listener.fd,
                                     reinterpret_cast<sockaddr*>(&addr),
                                     &addrLen) == 0);
  AD_CORRECTNESS_CHECK(::listen(listener.fd, 1) == 0);
  FdGuard sender{::socket(AF_INET, SOCK_STREAM, 0)};
  AD_CORRECTNESS_CHECK(sender.fd >= 0);
  AD_CORRECTNESS_CHECK(::connect(sender.fd, reinterpret_cast<sockaddr*>(&addr),
                                 sizeof(addr)) == 0);
  int receiver = ::accept(listener.fd, nullptr, nullptr);
  AD_CORRECTNESS_CHECK(receiver >= 0);
  return {std::exchange(sender.fd, -1), receiver};
}

// Send `numChunks` chunks of `chunkSize` bytes through a `ZeroCopySocketSender`
// with the given `config` over a loopback TCP connection, and check that the
// receiver gets exactly the bytes that were sent and that all buffers are
// recycled.
void sendAndCheckRoundTrip(const ZeroCopySenderConfig& config, size_t numChunks,
                           size_t chunkSize) {
  // Plain variables (not structured bindings), because they are captured by
  // the receiver lambda below.
  const auto connection = makeLoopbackTcpConnection();
  const int sendFd = connection.first;
  const int recvFd = connection.second;
  FdGuard sendGuard{sendFd};
  FdGuard recvGuard{recvFd};

  // Bound the receiver's blocking `recv`: without a timeout a sender-side
  // failure hangs the test forever instead of failing it.
  struct timeval recvTimeout {};
  recvTimeout.tv_sec = 10;
  ASSERT_EQ(::setsockopt(recvFd, SOL_SOCKET, SO_RCVTIMEO, &recvTimeout,
                         sizeof(recvTimeout)),
            0);

  ZeroCopySocketSender sender(config);
  EXPECT_EQ(sender.inFlightRequests(), 0u);
  EXPECT_EQ(sender.inFlightBuffers(), 0u);

  std::vector<char> expectedData(numChunks * chunkSize);
  size_t counter = 0;
  std::generate(expectedData.begin(), expectedData.end(), [&counter]() {
    return static_cast<char>((counter++ * 37 + 13) % 256);
  });
  std::vector<char> receivedData(expectedData.size(), 0);

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
  ThreadJoinGuard joinGuard{receiverThread};

  for (size_t offset = 0; offset < expectedData.size(); offset += chunkSize) {
    uint32_t slot = sender.acquireBuffer();
    auto span = sender.getSlotSpan(slot);
    std::memcpy(span.data(), expectedData.data() + offset, chunkSize);
    sender.sendChunk(sendFd, slot, chunkSize);
  }

  sender.flushAndDrainAll();
  EXPECT_EQ(sender.inFlightRequests(), 0u);
  EXPECT_EQ(sender.inFlightBuffers(), 0u);
  EXPECT_EQ(sender.bufferPool().availableSlots(), config.numBuffers);

  receiverThread.join();
  EXPECT_EQ(receivedData, expectedData);
}

// A small configuration with fewer buffers than chunks, so that every buffer
// slot is reused many times.
ZeroCopySenderConfig smallConfig() {
  ZeroCopySenderConfig config;
  config.ringEntries = 16;
  config.numBuffers = 8;
  config.bufferSizeBytes = 4096;
  return config;
}
}  // namespace

// _____________________________________________________________________________
TEST(ZeroCopySocketSenderTest, TransmissionOverSocketPair) {
  auto config = smallConfig();
  config.useRegisteredBuffers = true;
  config.useZeroCopy = true;
  sendAndCheckRoundTrip(config, 20, 1024);
}

// _____________________________________________________________________________
TEST(ZeroCopySocketSenderTest, ManyMoreChunksThanRingEntriesAndBuffers) {
  // Far more sends than `2 * ringEntries`: in-flight bookkeeping must not
  // collide when buffer slots are reused while earlier sends are pending.
  auto config = smallConfig();
  config.ringEntries = 4;
  sendAndCheckRoundTrip(config, 500, 4096);
}

// _____________________________________________________________________________
TEST(ZeroCopySocketSenderTest, PlainIoUringSendWithoutZeroCopy) {
  auto config = smallConfig();
  config.useRegisteredBuffers = false;
  config.useZeroCopy = false;
  sendAndCheckRoundTrip(config, 100, 1024);
}

// _____________________________________________________________________________
TEST(ZeroCopySocketSenderTest, AcquireThrowsWhenAllSlotsAreHeldButUnsent) {
  auto config = smallConfig();
  ZeroCopySocketSender sender(config);
  for (size_t i = 0; i < config.numBuffers; ++i) {
    std::ignore = sender.acquireBuffer();
  }
  // No buffer is in flight, so no completion can free one: waiting would hang.
  EXPECT_ANY_THROW(std::ignore = sender.acquireBuffer());
}

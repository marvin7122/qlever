// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <arpa/inet.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <deque>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "./util/GTestHelpers.h"
#include "util/http/ZeroCopyChunkSender.h"

using namespace ad_utility::httpUtils;

namespace {

// A submitted send as seen by the `FakeBackend`.
struct Submission {
  uint64_t id_;
  // The `iov_base` pointers and the bytes of the `msghdr` at submission.
  std::vector<const char*> bases_;
  std::string bytes_;
};

// Shared state of a `FakeBackend`, so that a test can inspect and drive it
// while the sender owns the backend.
struct FakeState {
  std::vector<Submission> submissions_;
  std::deque<ZeroCopySendEvent> events_;
  std::vector<uint64_t> cancelled_;
  // Called by `waitEvent` when no event is queued (the test "kernel").
  std::function<void()> onWait_;
  size_t numWaits_ = 0;
};

// A send backend under full control of the test: every send is recorded and
// completes only when the test queues its events.
class FakeBackend {
 public:
  std::shared_ptr<FakeState> state_ = std::make_shared<FakeState>();

  void submitSend(uint64_t id, int, const msghdr& message) {
    Submission submission{id, {}, {}};
    for (size_t i = 0; i < message.msg_iovlen; ++i) {
      const auto& iov = message.msg_iov[i];
      submission.bases_.push_back(static_cast<const char*>(iov.iov_base));
      submission.bytes_.append(static_cast<const char*>(iov.iov_base),
                               iov.iov_len);
    }
    state_->submissions_.push_back(std::move(submission));
  }
  ZeroCopySendEvent waitEvent() {
    ++state_->numWaits_;
    if (state_->events_.empty() && state_->onWait_) {
      state_->onWait_();
    }
    AD_CORRECTNESS_CHECK(!state_->events_.empty(), "test kernel stalled");
    auto event = state_->events_.front();
    state_->events_.pop_front();
    return event;
  }
  std::optional<ZeroCopySendEvent> pollEvent() {
    if (state_->events_.empty()) {
      return std::nullopt;
    }
    auto event = state_->events_.front();
    state_->events_.pop_front();
    return event;
  }
  void cancel(uint64_t id) { state_->cancelled_.push_back(id); }
};

ZeroCopySendEvent sent(uint64_t id, int64_t bytes, bool notify = true) {
  ZeroCopySendEvent event;
  event.id_ = id;
  event.result_ = bytes;
  event.notificationFollows_ = notify;
  return event;
}

ZeroCopySendEvent notification(uint64_t id, bool copied = false) {
  ZeroCopySendEvent event;
  event.id_ = id;
  event.isNotification_ = true;
  event.kernelCopied_ = copied;
  return event;
}

// The framed bytes of `body` as one HTTP/1.1 chunk.
std::string framed(const std::string& body) {
  return absl::StrCat(absl::Hex(body.size()), "\r\n", body, "\r\n");
}

}  // namespace

// _____________________________________________________________________________
TEST(ZeroCopyChunkSender, FramingIsOneSendFromTheChunkMemory) {
  FakeBackend backend;
  auto state = backend.state_;
  ZeroCopyChunkSender sender{std::move(backend), 3};
  std::string body(100'000, 'x');
  const char* bodyData = body.data();
  sender.sendChunk(std::move(body));
  ASSERT_EQ(state->submissions_.size(), 1u);
  const auto& submission = state->submissions_[0];
  // Size line, body and CRLF in one `msghdr`, the body sent in place.
  ASSERT_EQ(submission.bases_.size(), 3u);
  EXPECT_EQ(submission.bases_[1], bodyData);
  EXPECT_EQ(submission.bytes_, framed(std::string(100'000, 'x')));

  state->events_ = {sent(0, static_cast<int64_t>(submission.bytes_.size())),
                    notification(0)};
  sender.finish();
  EXPECT_EQ(sender.numInFlightChunks(), 0u);
  EXPECT_EQ(sender.numNotifications(), 1u);
}

// _____________________________________________________________________________
TEST(ZeroCopyChunkSender, ChunkIsReleasedOnlyAfterItsNotification) {
  FakeBackend backend;
  auto state = backend.state_;
  ZeroCopyChunkSender sender{std::move(backend), 3};
  sender.sendChunk("abc");
  const auto size = static_cast<int64_t>(framed("abc").size());
  // The send completed, but the kernel announced a notification: the chunk
  // must stay owned.
  state->events_ = {sent(0, size)};
  sender.sendChunk("def");
  EXPECT_EQ(sender.numInFlightChunks(), 2u);
  EXPECT_FALSE(state->events_.size());
  // The notification of the first chunk releases exactly that chunk; the
  // second chunk completes its send and now awaits its own notification.
  state->events_ = {notification(0), sent(1, size)};
  sender.sendRaw("0\r\n\r\n");
  EXPECT_EQ(sender.numInFlightChunks(), 2u);  // "def" + terminator
  state->events_ = {sent(2, 5, false), notification(1)};
  sender.finish();
  EXPECT_EQ(sender.numInFlightChunks(), 0u);
}

// _____________________________________________________________________________
TEST(ZeroCopyChunkSender, AtMostOneSendIsOutstandingAndOrderIsKept) {
  FakeBackend backend;
  auto state = backend.state_;
  ZeroCopyChunkSender sender{std::move(backend), 3};
  const std::vector<std::string> bodies{"one", "two", "three", "four"};
  // The test kernel completes a send only when the sender waits for it; each
  // wait must happen while exactly the last submitted send is outstanding.
  state->onWait_ = [&]() {
    const auto& last = state->submissions_.back();
    state->events_.push_back(
        sent(last.id_, static_cast<int64_t>(last.bytes_.size()), false));
  };
  for (const auto& body : bodies) {
    const size_t before = state->submissions_.size();
    sender.sendChunk(body);
    EXPECT_EQ(state->submissions_.size(), before + 1);
    EXPECT_TRUE(sender.hasOutstandingSend());
  }
  sender.finish();
  std::string wire;
  for (const auto& submission : state->submissions_) {
    wire += submission.bytes_;
  }
  std::string expected;
  for (const auto& body : bodies) {
    expected += framed(body);
  }
  EXPECT_EQ(wire, expected);
}

// _____________________________________________________________________________
TEST(ZeroCopyChunkSender, PartialSendIsResubmittedBeforeTheNextChunk) {
  FakeBackend backend;
  auto state = backend.state_;
  ZeroCopyChunkSender sender{std::move(backend), 3};
  sender.sendChunk("hello world");
  const std::string first = framed("hello world");
  // Only 6 bytes (the size line "b\r\n" and the first 3 body bytes) went out.
  state->events_ = {sent(0, 6)};
  state->onWait_ = [&]() {
    const auto& last = state->submissions_.back();
    state->events_.push_back(
        sent(last.id_, static_cast<int64_t>(last.bytes_.size()), false));
  };
  sender.sendChunk("next");
  ASSERT_EQ(state->submissions_.size(), 3u);
  EXPECT_EQ(state->submissions_[1].id_, 0u);
  EXPECT_EQ(state->submissions_[1].bytes_, first.substr(6));
  EXPECT_EQ(state->submissions_[2].bytes_, framed("next"));
  // The first send announced a notification with its partial completion.
  state->events_ = {notification(0)};
  sender.finish();
  EXPECT_EQ(sender.numInFlightChunks(), 0u);
  EXPECT_EQ(sender.totalBytesSent(), first.size() + framed("next").size());
}

// _____________________________________________________________________________
TEST(ZeroCopyChunkSender, BackpressureBoundsTheOwnedChunks) {
  FakeBackend backend;
  auto state = backend.state_;
  constexpr size_t maxInFlight = 2;
  ZeroCopyChunkSender sender{std::move(backend), 3, maxInFlight};
  const auto size = static_cast<int64_t>(framed("a").size());
  sender.sendChunk("a");
  state->events_ = {sent(0, size)};
  sender.sendChunk("a");
  EXPECT_EQ(sender.numInFlightChunks(), 2u);
  // Chunk 0 awaits its notification and chunk 1 is being sent. The third
  // `sendChunk` must block: first until the send of chunk 1 completes
  // (ordering), then until a chunk is released (backpressure). The test
  // kernel delivers one event per wait.
  std::deque<ZeroCopySendEvent> script{sent(1, size), notification(0)};
  std::vector<size_t> ownedAtWait;
  state->onWait_ = [&]() {
    ownedAtWait.push_back(sender.numInFlightChunks());
    state->events_.push_back(script.front());
    script.pop_front();
  };
  sender.sendChunk("a");
  EXPECT_THAT(ownedAtWait, ::testing::ElementsAre(2u, 2u));
  EXPECT_EQ(sender.numInFlightChunks(), 2u);
  EXPECT_EQ(state->submissions_.size(), 3u);
  state->onWait_ = nullptr;
  state->events_ = {notification(1), sent(2, size), notification(2)};
  sender.finish();
  EXPECT_EQ(sender.numInFlightChunks(), 0u);
}

// _____________________________________________________________________________
TEST(ZeroCopyChunkSender, AbortCancelsAndWaitsForAllNotifications) {
  FakeBackend backend;
  auto state = backend.state_;
  ZeroCopyChunkSender sender{std::move(backend), 3};
  const auto size = static_cast<int64_t>(framed("a").size());
  sender.sendChunk("a");
  state->events_ = {sent(0, size)};
  sender.sendChunk("b");
  // Chunk 0 awaits its notification, chunk 1 is still being sent.
  ASSERT_EQ(sender.numInFlightChunks(), 2u);
  std::vector<size_t> ownedAtWait;
  state->onWait_ = [&]() {
    ownedAtWait.push_back(sender.numInFlightChunks());
    if (ownedAtWait.size() == 1) {
      // The cancelled send completes with an error but the kernel still
      // holds the pages (a notification follows).
      ZeroCopySendEvent cancelled = sent(1, -ECANCELED);
      state->events_.push_back(cancelled);
    } else {
      state->events_.push_back(notification(ownedAtWait.size() == 2 ? 0 : 1));
    }
  };
  sender.abort();
  EXPECT_THAT(state->cancelled_, ::testing::ElementsAre(1u));
  // Every chunk stayed owned until its notification arrived.
  EXPECT_THAT(ownedAtWait, ::testing::ElementsAre(2u, 2u, 1u));
  EXPECT_EQ(sender.numInFlightChunks(), 0u);
}

// _____________________________________________________________________________
TEST(ZeroCopyChunkSender, SendErrorThrows) {
  FakeBackend backend;
  auto state = backend.state_;
  ZeroCopyChunkSender sender{std::move(backend), 3};
  sender.sendChunk("a");
  state->events_ = {sent(0, -EPIPE, false)};
  AD_EXPECT_THROW_WITH_MESSAGE(sender.finish(),
                               ::testing::HasSubstr("zero-copy send failed"));
  EXPECT_EQ(sender.numInFlightChunks(), 0u);
  AD_EXPECT_THROW_WITH_MESSAGE(sender.sendChunk(""),
                               ::testing::HasSubstr("!body.empty()"));
}

// _____________________________________________________________________________
TEST(ZeroCopyChunkSender, KernelCopiedNotificationsAreCounted) {
  FakeBackend backend;
  auto state = backend.state_;
  ZeroCopyChunkSender sender{std::move(backend), 3};
  const auto size = static_cast<int64_t>(framed("a").size());
  sender.sendChunk("a");
  state->events_ = {sent(0, size), notification(0, true)};
  sender.sendChunk("a");
  state->events_ = {sent(1, size), notification(1, false)};
  sender.finish();
  EXPECT_EQ(sender.numNotifications(), 2u);
  EXPECT_EQ(sender.numKernelCopiedNotifications(), 1u);
}

// _____________________________________________________________________________
// The production backend over a real TCP loopback connection (zero-copy sends
// need TCP; `AF_UNIX` rejects them). With io_uring it uses
// `IORING_OP_SENDMSG_ZC`, otherwise blocking `sendmsg()`; the bytes must be
// identical either way.
TEST(ZeroCopyChunkSender, SocketBackendOverTcpLoopback) {
  int listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listenFd, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(::bind(listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
            0);
  socklen_t addrLen = sizeof(addr);
  ASSERT_EQ(
      ::getsockname(listenFd, reinterpret_cast<sockaddr*>(&addr), &addrLen), 0);
  ASSERT_EQ(::listen(listenFd, 1), 0);
  int sendFd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sendFd, 0);
  ASSERT_EQ(::connect(sendFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
            0);
  int recvFd = ::accept(listenFd, nullptr, nullptr);
  ASSERT_GE(recvFd, 0);
  ::close(listenFd);

  std::vector<std::string> bodies;
  std::string expected;
  for (size_t i = 0; i < 20; ++i) {
    bodies.emplace_back(100'000 + i, static_cast<char>('a' + i));
    expected += framed(bodies.back());
  }
  expected += "0\r\n\r\n";

  std::string received;
  std::thread reader{[&]() {
    std::vector<char> buffer(1 << 16);
    while (received.size() < expected.size()) {
      ssize_t n = ::recv(recvFd, buffer.data(), buffer.size(), 0);
      if (n <= 0) {
        break;
      }
      received.append(buffer.data(), static_cast<size_t>(n));
    }
  }};
  absl::Cleanup cleanupConnection{[&] {
    // EOF unblocks recv if an exception prevents sending all expected bytes.
    ::shutdown(sendFd, SHUT_WR);
    reader.join();
    ::close(sendFd);
    ::close(recvFd);
  }};
  {
    ZeroCopyChunkSender sender{SocketSendBackend{}, sendFd, 4};
    for (auto& body : bodies) {
      sender.sendChunk(std::move(body));
      EXPECT_LE(sender.numInFlightChunks(), 4u);
    }
    sender.sendRaw("0\r\n\r\n");
    sender.finish();
    EXPECT_EQ(sender.numInFlightChunks(), 0u);
    EXPECT_EQ(sender.totalBytesSent(), expected.size());
    if (sender.backend().usesZeroCopy()) {
      // Every zero-copy send announced a notification, all have arrived.
      EXPECT_GT(sender.numNotifications(), 0u);
    }
  }
  std::move(cleanupConnection).Invoke();
  EXPECT_EQ(received, expected);
}

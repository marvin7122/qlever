// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_HTTP_ZEROCOPYCHUNKSENDER_H
#define QLEVER_SRC_UTIL_HTTP_ZEROCOPYCHUNKSENDER_H

#include <absl/strings/str_cat.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/Log.h"

// Use `io_uring` only when CMake found and linked liburing (it then defines
// `QLEVER_HAS_IO_URING`). Without it, every send takes the synchronous
// `sendmsg()` path below, which is byte-identical.
#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
// `IORING_OP_SENDMSG_ZC` and the 64-bit user-data helpers need liburing
// >= 2.5 (the version macros are absent before 2.2).
#if defined(IO_URING_VERSION_MAJOR) && \
    (IO_URING_VERSION_MAJOR > 2 ||     \
     (IO_URING_VERSION_MAJOR == 2 && IO_URING_VERSION_MINOR >= 5))
#define QLEVER_HAS_IO_URING_SENDMSG_ZC 1
#endif
#endif

namespace ad_utility::httpUtils {

// Flag that suppresses `SIGPIPE` for a single send to a closed peer. Not
// available on every platform; there, Boost.Asio sets `SO_NOSIGPIPE`.
#ifdef MSG_NOSIGNAL
inline constexpr int kZeroCopySendNoSignalFlag = MSG_NOSIGNAL;
#else
inline constexpr int kZeroCopySendNoSignalFlag = 0;
#endif

// Upper bound for a single wait on the peer (socket writability or a send
// completion). A stalled peer surfaces as an error instead of blocking the
// sending thread forever.
inline constexpr int kZeroCopyPeerStallTimeoutSeconds = 30;

// One completion reported by a send backend (see `ZeroCopyChunkSender`).
struct ZeroCopySendEvent {
  // The id that was passed to `submitSend`.
  uint64_t id_ = 0;
  // True for the notification that the kernel no longer references the memory
  // of an earlier zero-copy send with this id.
  bool isNotification_ = false;
  // Send completion: the number of bytes sent, or `-errno`.
  int64_t result_ = 0;
  // Send completion: a notification for this send will follow.
  bool notificationFollows_ = false;
  // Notification: the kernel copied the data instead of sending it from the
  // user pages (e.g. over loopback). Only reported by kernels that support
  // `IORING_SEND_ZC_REPORT_USAGE`.
  bool kernelCopied_ = false;
};

// Send HTTP/1.1 body chunks straight from the memory in which they were
// produced, without copying them into a send buffer. Each chunk is moved into
// the sender, which keeps it (and its chunk framing) alive until the kernel
// has sent it AND has reported that it no longer references its pages (the
// zero-copy notification). Invariants:
//
// * Ordering: at most one send is outstanding at any time, so the bytes reach
//   the socket in the order of the `sendChunk` calls. A partial send is
//   resubmitted for the remaining bytes before anything else is sent.
// * Lifetime: a chunk is released only when its send has completed and every
//   notification announced for it has arrived.
// * Backpressure: at most `maxInFlightChunks` chunks are owned at a time;
//   `sendChunk` blocks (waiting for completions) until one is released.
// * Framing: the chunk-size line, the body and the trailing CRLF of a chunk
//   are sent as one `sendmsg` with three `iovec`s.
//
// `Backend` performs the sends. It must provide
//   void submitSend(uint64_t id, int fd, const msghdr& message);
//   ZeroCopySendEvent waitEvent();                 // blocking, may throw
//   std::optional<ZeroCopySendEvent> pollEvent();  // non-blocking
//   void cancel(uint64_t id);                      // best effort
// `message` and the memory it references stay valid until the send and all
// its notifications have completed. See `SocketSendBackend` below for the
// production backend.
//
// Not thread-safe: one sender belongs to one HTTP session.
template <typename Backend>
class ZeroCopyChunkSender {
 public:
  static constexpr size_t kDefaultMaxInFlightChunks = 8;

 private:
  // A chunk owned by the sender while the kernel may still reference it.
  struct InFlightChunk {
    uint64_t id_ = 0;
    std::string body_;
    // Hexadecimal chunk size plus CRLF (at most 16 digits + 2).
    std::array<char, 18> sizeLine_{};
    std::array<iovec, 3> iovecs_{};
    msghdr message_{};
    size_t bytesRemaining_ = 0;
    bool sendPending_ = false;
    size_t pendingNotifications_ = 0;
  };

  Backend backend_;
  int fd_;
  size_t maxInFlightChunks_;
  // Owned chunks in submission order; `unique_ptr` keeps the addresses that
  // the kernel references stable.
  std::deque<std::unique_ptr<InFlightChunk>> inFlight_;
  // The chunk whose send is outstanding, if any (see "Ordering" above).
  InFlightChunk* outstandingSend_ = nullptr;
  uint64_t nextId_ = 0;
  size_t numNotifications_ = 0;
  size_t numKernelCopiedNotifications_ = 0;
  size_t totalBytesSent_ = 0;

 public:
  ZeroCopyChunkSender(Backend backend, int fd,
                      size_t maxInFlightChunks = kDefaultMaxInFlightChunks)
      : backend_{std::move(backend)},
        fd_{fd},
        maxInFlightChunks_{maxInFlightChunks} {
    AD_CONTRACT_CHECK(fd_ >= 0);
    AD_CONTRACT_CHECK(maxInFlightChunks_ > 0);
  }

  // The kernel may reference owned chunks, so the sender must not be copied
  // or moved while chunks are in flight; it lives in place in its session.
  ZeroCopyChunkSender(const ZeroCopyChunkSender&) = delete;
  ZeroCopyChunkSender& operator=(const ZeroCopyChunkSender&) = delete;
  ZeroCopyChunkSender(ZeroCopyChunkSender&&) = delete;
  ZeroCopyChunkSender& operator=(ZeroCopyChunkSender&&) = delete;

  ~ZeroCopyChunkSender() { abort(); }

  // Send `body` as one HTTP/1.1 chunk (size line, body, CRLF). `body` must not
  // be empty: an empty chunk is the end-of-body marker, see `sendRaw`.
  void sendChunk(std::string body) {
    AD_CONTRACT_CHECK(!body.empty());
    enqueue(std::move(body), true);
  }

  // Send `bytes` without chunk framing (used for the terminating `0\r\n\r\n`).
  void sendRaw(std::string bytes) {
    AD_CONTRACT_CHECK(!bytes.empty());
    enqueue(std::move(bytes), false);
  }

  // Block until every send has completed and every chunk was released.
  // Throws on a send error.
  void finish() {
    while (!inFlight_.empty()) {
      handleEvent(backend_.waitEvent());
    }
  }

  // Cancel the outstanding send and wait until the kernel has released every
  // owned chunk; errors are ignored. If the wait itself fails (e.g. the peer
  // stalls beyond the timeout), the chunks are deliberately leaked, because
  // the kernel may still read their memory. Called by the destructor.
  void abort() noexcept {
    if (inFlight_.empty()) {
      return;
    }
    try {
      if (outstandingSend_ != nullptr) {
        backend_.cancel(outstandingSend_->id_);
      }
      while (!inFlight_.empty()) {
        handleEvent(backend_.waitEvent(), false);
      }
    } catch (...) {
      AD_LOG_WARN << "Zero-copy send: " << inFlight_.size()
                  << " chunk(s) could not be released safely and are leaked"
                  << std::endl;
      for (auto& chunk : inFlight_) {
        [[maybe_unused]] auto* leaked = chunk.release();
      }
      inFlight_.clear();
      outstandingSend_ = nullptr;
    }
  }

  // Number of chunks currently owned (sent or not, awaiting notification).
  [[nodiscard]] size_t numInFlightChunks() const noexcept {
    return inFlight_.size();
  }
  [[nodiscard]] bool hasOutstandingSend() const noexcept {
    return outstandingSend_ != nullptr;
  }
  // Zero-copy notifications received, and how many of them reported that
  // the kernel copied the data after all.
  [[nodiscard]] size_t numNotifications() const noexcept {
    return numNotifications_;
  }
  [[nodiscard]] size_t numKernelCopiedNotifications() const noexcept {
    return numKernelCopiedNotifications_;
  }
  [[nodiscard]] size_t totalBytesSent() const noexcept {
    return totalBytesSent_;
  }
  [[nodiscard]] Backend& backend() noexcept { return backend_; }

 private:
  void enqueue(std::string body, bool frame) {
    // Reap what has already completed, without blocking.
    while (auto event = backend_.pollEvent()) {
      handleEvent(event.value());
    }
    // Ordering: wait for the outstanding send before submitting the next.
    while (outstandingSend_ != nullptr) {
      handleEvent(backend_.waitEvent());
    }
    // Backpressure: bound the memory the kernel may still reference.
    while (inFlight_.size() >= maxInFlightChunks_) {
      handleEvent(backend_.waitEvent());
    }

    auto chunk = std::make_unique<InFlightChunk>();
    chunk->id_ = nextId_++;
    chunk->body_ = std::move(body);
    size_t numIovecs = 0;
    auto addIovec = [&](const char* data, size_t size) {
      chunk->iovecs_[numIovecs].iov_base = const_cast<char*>(data);
      chunk->iovecs_[numIovecs].iov_len = size;
      ++numIovecs;
      chunk->bytesRemaining_ += size;
    };
    if (frame) {
      const std::string sizeLine =
          absl::StrCat(absl::Hex(chunk->body_.size()), "\r\n");
      AD_CORRECTNESS_CHECK(sizeLine.size() <= chunk->sizeLine_.size());
      std::memcpy(chunk->sizeLine_.data(), sizeLine.data(), sizeLine.size());
      addIovec(chunk->sizeLine_.data(), sizeLine.size());
      addIovec(chunk->body_.data(), chunk->body_.size());
      static constexpr std::string_view crlf = "\r\n";
      addIovec(crlf.data(), crlf.size());
    } else {
      addIovec(chunk->body_.data(), chunk->body_.size());
    }
    chunk->message_.msg_iov = chunk->iovecs_.data();
    chunk->message_.msg_iovlen = numIovecs;
    InFlightChunk& ref = *chunk;
    inFlight_.push_back(std::move(chunk));
    submit(ref);
  }

  void submit(InFlightChunk& chunk) {
    chunk.sendPending_ = true;
    outstandingSend_ = &chunk;
    try {
      backend_.submitSend(chunk.id_, fd_, chunk.message_);
    } catch (...) {
      // Nothing was submitted, so the kernel does not reference the chunk.
      chunk.sendPending_ = false;
      outstandingSend_ = nullptr;
      releaseIfDone(chunk);
      throw;
    }
  }

  // Advance the `iovec`s of `chunk` past `numBytes` sent bytes.
  static void advance(InFlightChunk& chunk, size_t numBytes) {
    AD_CORRECTNESS_CHECK(numBytes <= chunk.bytesRemaining_);
    chunk.bytesRemaining_ -= numBytes;
    iovec* iov = chunk.message_.msg_iov;
    size_t count = chunk.message_.msg_iovlen;
    while (numBytes > 0) {
      AD_CORRECTNESS_CHECK(count > 0);
      if (numBytes >= iov->iov_len) {
        numBytes -= iov->iov_len;
        ++iov;
        --count;
      } else {
        iov->iov_base = static_cast<char*>(iov->iov_base) + numBytes;
        iov->iov_len -= numBytes;
        numBytes = 0;
      }
    }
    // Skip exhausted `iovec`s so that the next send starts at real data.
    while (count > 0 && iov->iov_len == 0) {
      ++iov;
      --count;
    }
    chunk.message_.msg_iov = iov;
    chunk.message_.msg_iovlen = count;
  }

  void releaseIfDone(InFlightChunk& chunk) {
    if (chunk.sendPending_ || chunk.pendingNotifications_ > 0) {
      return;
    }
    for (auto it = inFlight_.begin(); it != inFlight_.end(); ++it) {
      if (it->get() == &chunk) {
        inFlight_.erase(it);
        return;
      }
    }
    AD_FAIL();
  }

  // Apply one completion. With `throwOnError == false` (used by `abort`),
  // errors are swallowed and partial sends are not resubmitted.
  void handleEvent(const ZeroCopySendEvent& event, bool throwOnError = true) {
    InFlightChunk* chunk = nullptr;
    for (auto& candidate : inFlight_) {
      if (candidate->id_ == event.id_) {
        chunk = candidate.get();
        break;
      }
    }
    AD_CORRECTNESS_CHECK(chunk != nullptr,
                         "completion for a chunk that is not in flight");
    if (event.isNotification_) {
      AD_CORRECTNESS_CHECK(chunk->pendingNotifications_ > 0);
      --chunk->pendingNotifications_;
      ++numNotifications_;
      if (event.kernelCopied_) {
        ++numKernelCopiedNotifications_;
      }
      releaseIfDone(*chunk);
      return;
    }
    AD_CORRECTNESS_CHECK(chunk->sendPending_);
    chunk->sendPending_ = false;
    outstandingSend_ = nullptr;
    if (event.notificationFollows_) {
      ++chunk->pendingNotifications_;
    }
    if (event.result_ < 0) {
      releaseIfDone(*chunk);
      if (throwOnError) {
        AD_THROW(absl::StrCat("zero-copy send failed: ",
                              std::strerror(static_cast<int>(-event.result_))));
      }
      return;
    }
    totalBytesSent_ += static_cast<size_t>(event.result_);
    advance(*chunk, static_cast<size_t>(event.result_));
    if (chunk->bytesRemaining_ > 0) {
      if (event.result_ == 0 || !throwOnError) {
        // No progress (peer gone) or aborting: do not resubmit.
        chunk->bytesRemaining_ = 0;
        releaseIfDone(*chunk);
        if (throwOnError) {
          AD_THROW("zero-copy send made no progress (peer closed)");
        }
        return;
      }
      // Partial send: send the rest before anything else (ordering).
      submit(*chunk);
      return;
    }
    releaseIfDone(*chunk);
  }
};

// The production backend: `IORING_OP_SENDMSG_ZC` on a per-session io_uring
// ring. Falls back to blocking `sendmsg()` (byte-identical, no notifications)
// when liburing is missing, the ring cannot be created, or the kernel does
// not support `IORING_OP_SENDMSG_ZC`.
class SocketSendBackend {
 private:
  // `user_data` of cancellation requests, whose completions are ignored.
  static constexpr uint64_t kCancelTag = ~uint64_t{0};
#ifdef QLEVER_HAS_IO_URING_SENDMSG_ZC
  io_uring ring_{};
#endif
  bool useRing_ = false;
  // Completions of synchronous sends, handed out by `waitEvent`/`pollEvent`.
  std::deque<ZeroCopySendEvent> syncEvents_;

 public:
  explicit SocketSendBackend(unsigned int ringEntries = 16) {
#ifdef QLEVER_HAS_IO_URING_SENDMSG_ZC
    if (io_uring_queue_init(ringEntries, &ring_, 0) < 0) {
      AD_LOG_WARN << "io_uring_queue_init failed, zero-copy sends fall back "
                     "to sendmsg()"
                  << std::endl;
      return;
    }
    io_uring_probe* probe = io_uring_get_probe_ring(&ring_);
    const bool supported =
        probe != nullptr &&
        io_uring_opcode_supported(probe, IORING_OP_SENDMSG_ZC) != 0;
    if (probe != nullptr) {
      io_uring_free_probe(probe);
    }
    if (!supported) {
      AD_LOG_WARN << "The kernel does not support IORING_OP_SENDMSG_ZC, "
                     "zero-copy sends fall back to sendmsg()"
                  << std::endl;
      io_uring_queue_exit(&ring_);
      return;
    }
    useRing_ = true;
#else
    (void)ringEntries;
#endif
  }

  ~SocketSendBackend() {
#ifdef QLEVER_HAS_IO_URING_SENDMSG_ZC
    if (useRing_) {
      io_uring_queue_exit(&ring_);
    }
#endif
  }

  // The ring is owned exclusively; the `ZeroCopyChunkSender` constructor moves
  // the backend in before anything was submitted.
  SocketSendBackend(const SocketSendBackend&) = delete;
  SocketSendBackend& operator=(const SocketSendBackend&) = delete;
  SocketSendBackend(SocketSendBackend&& other) noexcept
      : useRing_{std::exchange(other.useRing_, false)},
        syncEvents_{std::move(other.syncEvents_)} {
#ifdef QLEVER_HAS_IO_URING_SENDMSG_ZC
    ring_ = other.ring_;
    std::memset(&other.ring_, 0, sizeof(other.ring_));
#endif
  }
  SocketSendBackend& operator=(SocketSendBackend&&) = delete;

  // True if sends go through `IORING_OP_SENDMSG_ZC`.
  [[nodiscard]] bool usesZeroCopy() const noexcept { return useRing_; }

  void submitSend(uint64_t id, int fd, const msghdr& message) {
    AD_CONTRACT_CHECK(id != kCancelTag);
#ifdef QLEVER_HAS_IO_URING_SENDMSG_ZC
    if (useRing_) {
      io_uring_sqe* sqe = getSqe();
      io_uring_prep_sendmsg_zc(sqe, fd, &message,
                               MSG_WAITALL | kZeroCopySendNoSignalFlag);
#ifdef IORING_SEND_ZC_REPORT_USAGE
      sqe->ioprio |= IORING_SEND_ZC_REPORT_USAGE;
#endif
      io_uring_sqe_set_data64(sqe, id);
      submitRing();
      return;
    }
#endif
    ZeroCopySendEvent event;
    event.id_ = id;
    event.result_ = static_cast<int64_t>(sendAllSync(fd, message));
    syncEvents_.push_back(event);
  }

  ZeroCopySendEvent waitEvent() {
    if (!syncEvents_.empty()) {
      auto event = syncEvents_.front();
      syncEvents_.pop_front();
      return event;
    }
#ifdef QLEVER_HAS_IO_URING_SENDMSG_ZC
    AD_CONTRACT_CHECK(useRing_, "waitEvent without a pending send");
    while (true) {
      io_uring_cqe* cqe = nullptr;
      __kernel_timespec timeout{};
      timeout.tv_sec = kZeroCopyPeerStallTimeoutSeconds;
      int ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &timeout);
      if (ret == -ETIME) {
        AD_THROW("timed out waiting for a send completion (peer stalled)");
      }
      if (ret == -EINTR) {
        continue;
      }
      if (ret < 0) {
        AD_THROW(
            absl::StrCat("io_uring_wait_cqe failed: ", std::strerror(-ret)));
      }
      if (auto event = consume(cqe)) {
        return event.value();
      }
    }
#else
    AD_FAIL();
#endif
  }

  std::optional<ZeroCopySendEvent> pollEvent() {
    if (!syncEvents_.empty()) {
      auto event = syncEvents_.front();
      syncEvents_.pop_front();
      return event;
    }
#ifdef QLEVER_HAS_IO_URING_SENDMSG_ZC
    if (useRing_) {
      io_uring_cqe* cqe = nullptr;
      while (io_uring_peek_cqe(&ring_, &cqe) == 0) {
        if (auto event = consume(cqe)) {
          return event;
        }
      }
    }
#endif
    return std::nullopt;
  }

  void cancel([[maybe_unused]] uint64_t id) {
#ifdef QLEVER_HAS_IO_URING_SENDMSG_ZC
    if (useRing_) {
      io_uring_sqe* sqe = getSqe();
      io_uring_prep_cancel64(sqe, id, 0);
      io_uring_sqe_set_data64(sqe, kCancelTag);
      submitRing();
    }
#endif
  }

 private:
#ifdef QLEVER_HAS_IO_URING_SENDMSG_ZC
  io_uring_sqe* getSqe() {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      submitRing();
      sqe = io_uring_get_sqe(&ring_);
    }
    AD_CORRECTNESS_CHECK(sqe != nullptr);
    return sqe;
  }

  void submitRing() {
    int ret = io_uring_submit(&ring_);
    if (ret < 0) {
      AD_THROW(absl::StrCat("io_uring_submit failed: ", std::strerror(-ret)));
    }
  }

  // Translate and consume one CQE; cancellation completions yield nothing.
  std::optional<ZeroCopySendEvent> consume(io_uring_cqe* cqe) {
    ZeroCopySendEvent event;
    event.id_ = io_uring_cqe_get_data64(cqe);
    event.isNotification_ = (cqe->flags & IORING_CQE_F_NOTIF) != 0;
    event.notificationFollows_ = (cqe->flags & IORING_CQE_F_MORE) != 0;
    event.result_ = cqe->res;
#ifdef IORING_NOTIF_USAGE_ZC_COPIED
    if (event.isNotification_) {
      event.kernelCopied_ =
          (static_cast<uint32_t>(cqe->res) & IORING_NOTIF_USAGE_ZC_COPIED) != 0;
    }
#endif
    io_uring_cqe_seen(&ring_, cqe);
    if (event.id_ == kCancelTag) {
      return std::nullopt;
    }
    return event;
  }
#endif

  // Blocking `sendmsg` of the whole `message` on a (possibly non-blocking)
  // socket. Returns the number of bytes sent, or `-errno`.
  static int64_t sendAllSync(int fd, const msghdr& message) {
    std::array<iovec, 3> iovecs{};
    AD_CONTRACT_CHECK(message.msg_iovlen <= iovecs.size());
    std::copy(message.msg_iov, message.msg_iov + message.msg_iovlen,
              iovecs.begin());
    msghdr remaining{};
    remaining.msg_iov = iovecs.data();
    remaining.msg_iovlen = message.msg_iovlen;
    int64_t total = 0;
    while (remaining.msg_iovlen > 0) {
      ssize_t sent = ::sendmsg(fd, &remaining, kZeroCopySendNoSignalFlag);
      if (sent < 0) {
        if (errno == EINTR) {
          continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          pollfd pfd{fd, POLLOUT, 0};
          int ret = ::poll(&pfd, 1, kZeroCopyPeerStallTimeoutSeconds * 1000);
          if (ret == 0) {
            return -ETIMEDOUT;
          }
          if (ret < 0 && errno != EINTR) {
            return -errno;
          }
          continue;
        }
        return -errno;
      }
      if (sent == 0) {
        return total;
      }
      total += sent;
      auto numBytes = static_cast<size_t>(sent);
      while (numBytes > 0) {
        iovec& iov = *remaining.msg_iov;
        if (numBytes >= iov.iov_len) {
          numBytes -= iov.iov_len;
          ++remaining.msg_iov;
          --remaining.msg_iovlen;
        } else {
          iov.iov_base = static_cast<char*>(iov.iov_base) + numBytes;
          iov.iov_len -= numBytes;
          numBytes = 0;
        }
      }
      while (remaining.msg_iovlen > 0 && remaining.msg_iov->iov_len == 0) {
        ++remaining.msg_iov;
        --remaining.msg_iovlen;
      }
    }
    return total;
  }
};

}  // namespace ad_utility::httpUtils

#endif  // QLEVER_SRC_UTIL_HTTP_ZEROCOPYCHUNKSENDER_H

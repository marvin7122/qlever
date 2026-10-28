// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_SCATTERGATHERARENASTREAMER_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_SCATTERGATHERARENASTREAMER_H

#include <absl/strings/str_cat.h>
#include <sys/uio.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"

#ifndef UIO_MAXIOV
#define UIO_MAXIOV 1024
#endif

namespace qlever::export_v2 {

// Return the total number of bytes described by `iovecs`.
inline size_t totalIovecBytes(ql::span<const iovec> iovecs) {
  return std::accumulate(
      iovecs.begin(), iovecs.end(), size_t{0},
      [](size_t sum, const iovec& entry) { return sum + entry.iov_len; });
}

class OwnedByteSpan;

// Immutable shared storage for bytes referenced by scatter-gather chunks. The
// constructor takes the string by value and moves it into shared ownership,
// so no mutable alias to the stored bytes exists.
class ImmutableByteBuffer {
 private:
  std::shared_ptr<const std::string> bytes_;

 public:
  explicit ImmutableByteBuffer(std::string bytes)
      : bytes_{std::make_shared<const std::string>(std::move(bytes))} {}

  [[nodiscard]] size_t size() const noexcept { return bytes_->size(); }
  [[nodiscard]] OwnedByteSpan slice(size_t offset, size_t size) const;
};

// A byte range of an `ImmutableByteBuffer` that shares ownership of the whole
// buffer, so the bytes stay alive as long as any chunk references them.
class OwnedByteSpan {
  friend class ImmutableByteBuffer;
  friend class ScatterGatherChunkBuilder;

 private:
  std::shared_ptr<const std::string> owner_;
  size_t offset_ = 0;
  size_t size_ = 0;

  OwnedByteSpan(std::shared_ptr<const std::string> owner, size_t offset,
                size_t size)
      : owner_{std::move(owner)}, offset_{offset}, size_{size} {
    AD_CONTRACT_CHECK(owner_ != nullptr);
    AD_CONTRACT_CHECK(offset_ <= owner_->size());
    AD_CONTRACT_CHECK(size_ <= owner_->size() - offset_);
  }

 public:
  [[nodiscard]] size_t size() const noexcept { return size_; }
  [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
};

inline OwnedByteSpan ImmutableByteBuffer::slice(size_t offset,
                                                size_t size) const {
  return OwnedByteSpan{bytes_, offset, size};
}

// The outcome of writing a chunk: the number of bytes written, and whether
// the write stopped early because of cancellation (then fewer than `size()`).
struct ScatterGatherWriteResult {
  size_t bytesWritten_ = 0;
  bool cancelled_ = false;
};

// The outcome of one `writev`-like call: `bytesWritten_ >= 0` on success,
// otherwise `-1` and the `errno` value in `errorNumber_`.
struct ScatterGatherWriteAttempt {
  ssize_t bytesWritten_ = 0;
  int errorNumber_ = 0;
};

// An immutable chunk whose segments retain all referenced allocations. The
// `iovec` pointers passed to a writer callback are only valid during that
// call; the public interface (`writeToFd`) does not expose them.
class ScatterGatherChunk {
  friend class ScatterGatherChunkBuilder;
  friend class ScatterGatherChunkTestAccess;

 private:
  struct Segment {
    std::shared_ptr<const std::string> owner_;
    size_t offset_ = 0;
    size_t size_ = 0;
  };

  std::vector<Segment> segments_;
  size_t totalBytes_ = 0;

  explicit ScatterGatherChunk(std::vector<Segment> segments, size_t totalBytes)
      : segments_{std::move(segments)}, totalBytes_{totalBytes} {}

  using Writer =
      std::function<ScatterGatherWriteAttempt(ql::span<const iovec>)>;
  using IsCancelled = std::function<bool()>;

  [[nodiscard]] ScatterGatherWriteResult writeWith(
      const Writer& writer, const IsCancelled& isCancelled) const {
    AD_CONTRACT_CHECK(writer != nullptr);
    AD_CONTRACT_CHECK(isCancelled != nullptr);

    size_t segmentIndex = 0;
    size_t segmentOffset = 0;
    size_t totalWritten = 0;
    std::vector<iovec> iovecs;
    iovecs.reserve(std::min<size_t>(segments_.size(), UIO_MAXIOV));

    while (segmentIndex < segments_.size()) {
      if (isCancelled()) {
        return {totalWritten, true};
      }

      iovecs.clear();
      const size_t batchSize =
          std::min<size_t>(segments_.size() - segmentIndex, UIO_MAXIOV);
      // Only the first segment of a batch can be partially written already.
      size_t offset = segmentOffset;
      for (const auto& segment : ql::span<const Segment>{segments_}.subspan(
               segmentIndex, batchSize)) {
        // `iov_base` is `void*`, so exposing the read-only segment bytes
        // requires `const_cast`; `writev` only reads the referenced memory.
        iovecs.push_back({const_cast<char*>(segment.owner_->data() +
                                            segment.offset_ + offset),
                          segment.size_ - offset});
        offset = 0;
      }

      const auto attempt = writer({iovecs.data(), iovecs.size()});
      if (attempt.bytesWritten_ < 0) {
        if (attempt.errorNumber_ == EINTR) {
          continue;
        }
        if (attempt.errorNumber_ == EAGAIN ||
            attempt.errorNumber_ == EWOULDBLOCK) {
          // Non-blocking fd with no progress yet: re-poll cancellation
          // before retrying so a stalled fd cannot spin past cancellation.
          if (isCancelled()) {
            return {totalWritten, true};
          }
          continue;
        }
        AD_THROW(absl::StrCat("scatter-gather write failed: ",
                              std::strerror(attempt.errorNumber_)));
      }
      AD_CONTRACT_CHECK(attempt.errorNumber_ == 0);
      AD_CONTRACT_CHECK(attempt.bytesWritten_ > 0);

      size_t remaining = static_cast<size_t>(attempt.bytesWritten_);
      AD_CONTRACT_CHECK(remaining <= totalIovecBytes(iovecs));
      totalWritten += remaining;

      while (remaining > 0) {
        const auto& segment = segments_[segmentIndex];
        const size_t available = segment.size_ - segmentOffset;
        if (remaining < available) {
          segmentOffset += remaining;
          remaining = 0;
        } else {
          remaining -= available;
          ++segmentIndex;
          segmentOffset = 0;
        }
      }
    }
    return {totalWritten, false};
  }

 public:
  ScatterGatherChunk() = default;

  // Total number of bytes over all segments.
  [[nodiscard]] size_t size() const noexcept { return totalBytes_; }
  [[nodiscard]] bool empty() const noexcept { return segments_.empty(); }
  [[nodiscard]] size_t numSegments() const noexcept { return segments_.size(); }

  // Copy all bytes into one string (for tests and diagnostics).
  [[nodiscard]] std::string toString() const {
    std::string result;
    result.reserve(totalBytes_);
    for (const auto& segment : segments_) {
      result.append(segment.owner_->data() + segment.offset_, segment.size_);
    }
    return result;
  }

  // Write the whole chunk to `fd` with `writev`, at most `UIO_MAXIOV`
  // segments per call. Partial writes, `EINTR`, and `EAGAIN` are retried;
  // `isCancelled` is polled before every call and on `EAGAIN`, and a `true`
  // result stops the write. Any other error throws.
  [[nodiscard]] ScatterGatherWriteResult writeToFd(
      int fd, const IsCancelled& isCancelled = [] { return false; }) const {
    AD_CONTRACT_CHECK(fd >= 0);
    return writeWith(
        [fd](ql::span<const iovec> iovecs) {
          const auto result =
              ::writev(fd, iovecs.data(), static_cast<int>(iovecs.size()));
          return ScatterGatherWriteAttempt{result, result < 0 ? errno : 0};
        },
        isCancelled);
  }
};

// Build a `ScatterGatherChunk` from copied bytes and referenced
// `OwnedByteSpan`s, in append order. `finalize` consumes the builder.
class ScatterGatherChunkBuilder {
 private:
  struct PendingSegment {
    std::shared_ptr<const std::string> owner_;
    size_t offset_ = 0;
    size_t size_ = 0;
    bool copied_ = false;
  };

  std::string copiedBytes_;
  std::vector<PendingSegment> segments_;
  size_t totalBytes_ = 0;

 public:
  // Copy `bytes` into the chunk's own storage (for small, short-lived
  // pieces such as delimiters); adjacent copies share one segment.
  void appendCopy(std::string_view bytes) {
    if (bytes.empty()) {
      return;
    }
    if (!segments_.empty() && segments_.back().copied_) {
      copiedBytes_.append(bytes);
      segments_.back().size_ += bytes.size();
    } else {
      const size_t offset = copiedBytes_.size();
      copiedBytes_.append(bytes);
      segments_.push_back({nullptr, offset, bytes.size(), true});
    }
    totalBytes_ += bytes.size();
  }

  // Reference `bytes` without copying; the chunk shares ownership.
  void appendOwned(OwnedByteSpan bytes) {
    if (bytes.empty()) {
      return;
    }
    totalBytes_ += bytes.size_;
    segments_.push_back(
        {std::move(bytes.owner_), bytes.offset_, bytes.size_, false});
  }

  [[nodiscard]] ScatterGatherChunk finalize() && {
    // Only allocate the shared copied-bytes owner when a copied segment
    // exists; purely referenced chunks finalize with zero allocations here.
    // (`appendCopy` early-returns on empty input, so a null owner can never
    // coincide with a `copied_` segment.)
    std::shared_ptr<const std::string> copiedOwner;
    if (!copiedBytes_.empty()) {
      copiedOwner =
          std::make_shared<const std::string>(std::move(copiedBytes_));
    }
    std::vector<ScatterGatherChunk::Segment> result;
    result.reserve(segments_.size());
    for (auto& segment : segments_) {
      result.push_back(
          {segment.copied_ ? copiedOwner : std::move(segment.owner_),
           segment.offset_, segment.size_});
    }
    const size_t totalBytes = totalBytes_;
    segments_.clear();
    totalBytes_ = 0;
    return ScatterGatherChunk{std::move(result), totalBytes};
  }
};

}  // namespace qlever::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_SCATTERGATHERARENASTREAMER_H

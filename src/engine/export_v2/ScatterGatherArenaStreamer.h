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

#include <sys/uio.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/Invariants.h"

#ifndef UIO_MAXIOV
#define UIO_MAXIOV 1024
#endif

namespace qlever::export_v2 {

class OwnedByteSpan;

// Immutable shared storage for bytes referenced by scatter-gather chunks. The
// constructor takes the input by value and moves it into const shared storage.
class ImmutableByteBuffer {
 private:
  std::shared_ptr<const std::string> bytes_;

 public:
  explicit ImmutableByteBuffer(std::string bytes)
      : bytes_{std::make_shared<const std::string>(std::move(bytes))} {}

  [[nodiscard]] size_t size() const noexcept { return bytes_->size(); }
  [[nodiscard]] OwnedByteSpan slice(size_t offset, size_t size) const;
};

// A byte range that carries the immutable allocation owning its data.
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

struct ScatterGatherWriteResult {
  size_t bytesWritten_ = 0;
  bool cancelled_ = false;
  bool wouldBlock_ = false;
};

struct ScatterGatherWriteAttempt {
  ssize_t bytesWritten_ = 0;
  int errorNumber_ = 0;
};

// An immutable chunk whose segments retain all referenced allocations. Raw
// iovec pointers are valid only for the duration of the writer callback; the
// callback must not retain them.
class ScatterGatherChunk
    : public ad_utility::WithInvariants<ScatterGatherChunk> {
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
      : segments_{std::move(segments)}, totalBytes_{totalBytes} {
    checkInvariants();
  }

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
      const size_t end = std::min(segments_.size(), segmentIndex + UIO_MAXIOV);
      for (size_t index = segmentIndex; index < end; ++index) {
        const auto& segment = segments_[index];
        const size_t offset = index == segmentIndex ? segmentOffset : 0;
        iovecs.push_back({const_cast<char*>(segment.owner_->data() +
                                            segment.offset_ + offset),
                          segment.size_ - offset});
      }

      const auto attempt = writer({iovecs.data(), iovecs.size()});
      if (attempt.bytesWritten_ < 0) {
        if (attempt.errorNumber_ == EINTR) {
          continue;
        }
        if (attempt.errorNumber_ == EAGAIN ||
            attempt.errorNumber_ == EWOULDBLOCK) {
          return {totalWritten, false, true};
        }
        AD_THROW(absl::StrCat("scatter-gather write failed: ",
                              std::strerror(attempt.errorNumber_)));
      }
      AD_CONTRACT_CHECK(attempt.errorNumber_ == 0);
      if (attempt.bytesWritten_ == 0) {
        // A non-progressing write can be transient. Retry unless cancellation
        // was requested, avoiding an invariant assertion on caller behavior.
        if (isCancelled()) {
          return {totalWritten, true};
        }
        continue;
      }

      size_t remaining = static_cast<size_t>(attempt.bytesWritten_);
      size_t offered = 0;
      for (const auto& iovec : iovecs) {
        offered += iovec.iov_len;
      }
      AD_CONTRACT_CHECK(remaining <= offered);
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

  void checkInvariants() const {
    size_t total = 0;
    for (const auto& segment : segments_) {
      AD_CORRECTNESS_CHECK(segment.owner_ != nullptr);
      AD_CORRECTNESS_CHECK(segment.size_ > 0);
      AD_CORRECTNESS_CHECK(segment.offset_ <= segment.owner_->size());
      AD_CORRECTNESS_CHECK(segment.size_ <=
                           segment.owner_->size() - segment.offset_);
      total += segment.size_;
    }
    AD_CORRECTNESS_CHECK(total == totalBytes_);
    AD_CORRECTNESS_CHECK((totalBytes_ == 0) == segments_.empty());
  }

  [[nodiscard]] size_t size() const noexcept { return totalBytes_; }
  [[nodiscard]] bool empty() const noexcept { return segments_.empty(); }
  [[nodiscard]] size_t numSegments() const noexcept { return segments_.size(); }

  [[nodiscard]] std::string toString() const {
    std::string result;
    result.reserve(totalBytes_);
    for (const auto& segment : segments_) {
      result.append(segment.owner_->data() + segment.offset_, segment.size_);
    }
    return result;
  }

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

// Consuming builder that pairs each referenced byte range with its owner.
class ScatterGatherChunkBuilder
    : public ad_utility::WithInvariants<ScatterGatherChunkBuilder> {
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
  void checkInvariants() const {
    size_t total = 0;
    for (const auto& segment : segments_) {
      AD_CORRECTNESS_CHECK(segment.size_ > 0);
      if (segment.copied_) {
        AD_CORRECTNESS_CHECK(segment.owner_ == nullptr);
        AD_CORRECTNESS_CHECK(segment.offset_ <= copiedBytes_.size());
        AD_CORRECTNESS_CHECK(segment.size_ <=
                             copiedBytes_.size() - segment.offset_);
      } else {
        AD_CORRECTNESS_CHECK(segment.owner_ != nullptr);
        AD_CORRECTNESS_CHECK(segment.offset_ <= segment.owner_->size());
        AD_CORRECTNESS_CHECK(segment.size_ <=
                             segment.owner_->size() - segment.offset_);
      }
      total += segment.size_;
    }
    AD_CORRECTNESS_CHECK(total == totalBytes_);
  }

  void appendCopy(std::string_view bytes) {
    auto guard = makeInvariantGuard();
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

  void appendOwned(OwnedByteSpan bytes) {
    auto guard = makeInvariantGuard();
    if (bytes.empty()) {
      return;
    }
    totalBytes_ += bytes.size_;
    segments_.push_back(
        {std::move(bytes.owner_), bytes.offset_, bytes.size_, false});
  }

  [[nodiscard]] ScatterGatherChunk finalize() && {
    auto guard = makeInvariantGuard();
    auto copiedOwner =
        std::make_shared<const std::string>(std::move(copiedBytes_));
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

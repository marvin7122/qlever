
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SCATTERGATHERARENASTREAMER_H
#define QLEVER_SRC_ENGINE_SCATTERGATHERARENASTREAMER_H

#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>


#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "engine/ConstructTypes.h"
#include "engine/export_prototypes/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/Invariants.h"
#include "util/Log.h"

#ifndef UIO_MAXIOV
#define UIO_MAXIOV 1024
#endif

namespace ql::export_streaming {

using ql::export_formatting::ExportFormat;
using ql::export_formatting::ExportStreamSummary;

// Forward declaration
class ScatterGatherChunkStreamer;

// _____________________________________________________________________________
// Configuration options for ScatterGatherChunkStreamer.
struct ScatterGatherConfig {
  size_t maxChunkBytes = 1024 * 1024;        // Target chunk size: 1 MB
  size_t maxIovecs = 1024;                  // Max iovecs per chunk (<= UIO_MAXIOV)
  size_t zeroCopyThresholdBytes = 64;       // Spans >= threshold are zero-copy
  size_t initialHeaderCapacity = 64 * 1024; // 64 KB initial header buffer
};

// _____________________________________________________________________________
// An invariant-proven assembled scatter-gather chunk containing an array of
// `struct iovec` descriptors referencing external arena memory pages alongside
// an owned local header buffer for formatting delimiters and short tokens.
class ScatterGatherChunk : public ad_utility::WithInvariants<ScatterGatherChunk> {
 public:
  // Architecture Standard § 3.1: Passkey Idiom for restricted construction.
  class Passkey {
   private:
    friend class ScatterGatherChunkStreamer;
    explicit Passkey() = default;
  };

 private:
    std::vector<struct iovec> iovecs_; // Descriptors for external arena memory pages
    std::vector<char> headerBuffer_; // Owned buffer for delimiters and short tokens
  size_t totalBytes_ = 0;
  size_t numTriples_ = 0;
  size_t zeroCopySpansCount_ = 0;
  size_t zeroCopyBytes_ = 0;

 public:
  ScatterGatherChunk() = default;

    // Construct via Passkey from ScatterGatherChunkStreamer.
  ScatterGatherChunk(Passkey, std::vector<struct iovec> iovecs,
                     std::vector<char> headerBuffer, size_t totalBytes,
                     size_t numTriples, size_t zeroCopySpansCount,
                     size_t zeroCopyBytes)
      : iovecs_{std::move(iovecs)},
        headerBuffer_{std::move(headerBuffer)},
        totalBytes_{totalBytes},
        numTriples_{numTriples},
        zeroCopySpansCount_{zeroCopySpansCount},
        zeroCopyBytes_{zeroCopyBytes} {
    checkInvariants();
  }

  ~ScatterGatherChunk() = default;

  ScatterGatherChunk(const ScatterGatherChunk&) = delete;
  ScatterGatherChunk& operator=(const ScatterGatherChunk&) = delete;

  ScatterGatherChunk(ScatterGatherChunk&& other) noexcept
      : iovecs_{std::move(other.iovecs_)},
        headerBuffer_{std::move(other.headerBuffer_)},
        totalBytes_{std::exchange(other.totalBytes_, 0)},
        numTriples_{std::exchange(other.numTriples_, 0)},
        zeroCopySpansCount_{std::exchange(other.zeroCopySpansCount_, 0)},
        zeroCopyBytes_{std::exchange(other.zeroCopyBytes_, 0)} {}

  ScatterGatherChunk& operator=(ScatterGatherChunk&& other) noexcept {
    if (this != &other) {
      iovecs_ = std::move(other.iovecs_);
      headerBuffer_ = std::move(other.headerBuffer_);
      totalBytes_ = std::exchange(other.totalBytes_, 0);
      numTriples_ = std::exchange(other.numTriples_, 0);
      zeroCopySpansCount_ = std::exchange(other.zeroCopySpansCount_, 0);
      zeroCopyBytes_ = std::exchange(other.zeroCopyBytes_, 0);
    }
    return *this;
  }

  // ___________________________________________________________________________
    // Verify structural invariants (Law 3 & Architecture Standard § 3).
  void checkInvariants() const {
    if (totalBytes_ == 0) {
      
        computedSum += iov.iov_len;
      }
      AD_CORRECTNESS_CHECK(computedSum == totalBytes_);
      AD_CORRECTNESS_CHECK(zeroCopyBytes_ <= totalBytes_);
    }
  }

  // ___________________________________________________________________________
  // Accessors
  [[nodiscard]] ql::span<const struct iovec> iovecs() const noexcept {
    return {iovecs_.data(), iovecs_.size()};
  }
  [[nodiscard]] size_t totalBytes() const noexcept { return totalBytes_; }
  [[nodiscard]] size_t numSegments() const noexcept { return iovecs_.size(); }
  [[nodiscard]] size_t numTriples() const noexcept { return numTriples_; }
  [[nodiscard]] size_t zeroCopyBytes() const noexcept { return zeroCopyBytes_; }
  [[nodiscard]] size_t zeroCopySpansCount() const noexcept {
    return zeroCopySpansCount_;
  }
  [[nodiscard]] bool empty() const noexcept { return totalBytes_ == 0; }

  // ___________________________________________________________________________
    // Transmit chunk directly via POSIX `writev(2)` in a loop until all bytes are sent.
  [[nodiscard]] ssize_t writeToFd(int fd) const {
    if (empty()) {
      return 0;
    

    while (offset < remainingIov.size()) {
      int count = static_cast<int>(
          std::min<size_t>(remainingIov.size() - offset, UIO_MAXIOV));
      ssize_t bytes = ::writev(fd, remainingIov.data() + offset, count);
      if (bytes < 0) {
        if (errno == EINTR) {
          continue;
        }
        AD_THROW(absl::StrCat("writev failed (errno: ", strerror(errno), ")"));
      }
      totalWritten += bytes;
      size_t remainingToAdvance = static_cast<size_t>(bytes);
      while (offset < remainingIov.size() && remainingToAdvance > 0) {
        if (remainingIov[offset].iov_len <= remainingToAdvance) {
          remainingToAdvance -= remainingIov[offset].iov_len;
          ++offset;
        } else {
          remainingIov[offset].iov_base =
              static_cast<char*>(remainingIov[offset].iov_base) +
              remainingToAdvance;
          remainingIov[offset].iov_len -= remainingToAdvance;
          remainingToAdvance = 0;
        }
      }
    }
    return totalWritten;
  }

  // ___________________________________________________________________________
  // Copy all scatter-gather slices into a contiguous target buffer (for
  // verification, hashing, or baseline comparison).
  void copyToContiguous(ql::span<char> dest) const {
    AD_CONTRACT_CHECK(dest.size() >= totalBytes_);
    char* outPtr = dest.data();
    for (const auto& iov : iovecs_) {
      std::memcpy(outPtr, iov.iov_base, iov.iov_len);
      outPtr += iov.iov_len;
    }
  }

  // ___________________________________________________________________________
    // Flatten chunk into a `std::string`.
  [[nodiscard]] std::string toString() const {
    std::string result;
    result.resize(totalBytes_);
    copyToContiguous(ql::span<char>(result.data(), result.size()));
    return result;
  }
};


class ScatterGatherChunkStreamer
    : public ad_utility::WithInvariants<ScatterGatherChunkStreamer> {
 public:
  using ChunkSink = std::function<void(ScatterGatherChunk)>;

 private:
  struct SliceRecord {
    bool isArena = false;
    const char* arenaPtr = nullptr;
    size_t headerOffset = 0;
    size_t len = 0;
  };

  ScatterGatherConfig config_;
  ChunkSink sink_;
  bool isStreaming_ = false;

  std::vector<SliceRecord> currentSlices_;
  
  uint64_t totalTriples_ = 0;
  uint64_t totalBytesWritten_ = 0;
  uint64_t chunksEmitted_ = 0;
  uint64_t totalZeroCopySpans_ = 0;
  uint64_t totalZeroCopyBytes_ = 0;

 public:
  // ___________________________________________________________________________
  // Construct in streaming mode with an output sink callback.
  explicit ScatterGatherChunkStreamer(ChunkSink sink,
                                      ScatterGatherConfig config = {})
      : config_{config}, sink_{std::move(sink)}, isStreaming_{true} {
    AD_CONTRACT_CHECK(sink_ != nullptr);
    currentHeaderBuffer_.reserve(config_.initialHeaderCapacity);
  }

  // ___________________________________________________________________________
  // Construct in accumulating batch mode (where chunks are returned via flush()).
  explicit ScatterGatherChunkStreamer(ScatterGatherConfig config = {})
      : config_{config}, sink_{nullptr}, isStreaming_{false} {
    currentHeaderBuffer_.reserve(config_.initialHeaderCapacity);
  }

  // ___________________________________________________________________________
    // Verify invariants (Law 3 & Architecture Standard § 3).
  void checkInvariants() const {
    size_t computedBytes = 0;
    size_t computedZcBytes = 0;
    size_t computedZcSpans = 0;
    for (const auto& slice : currentSlices_) {
      AD_CORRECTNESS_CHECK(slice.len > 0);
      computedBytes += slice.len;
      if (slice.isArena) {
        AD_CORRECTNESS_CHECK(slice.arenaPtr != nullptr);
        computedZcBytes += slice.len;
        ++computedZcSpans;
      } else {
        AD_CORRECTNESS_CHECK(slice.headerOffset + slice.len <=
                             currentHeaderBuffer_.size());
      }
    }
    AD_CORRECTNESS_CHECK(computedBytes == currentChunkBytes_);
    AD_CORRECTNESS_CHECK(computedZcBytes == currentZeroCopyBytes_);
    AD_CORRECTNESS_CHECK(computedZcSpans == currentZeroCopySpans_);
  }

  // ___________________________________________________________________________
  // Append raw formatting string into chunk header buffer with auto-coalescing.
  void writeRawHeader(std::string_view sv) {
    if (sv.empty()) {
      return;
    }

    if (!currentSlices_.empty() &&
        (currentChunkBytes_ + sv.size() > config_.maxChunkBytes ||
         currentSlices_.size() >= config_.maxIovecs)) {
      flush();
    }

    // Coalesce adjacent header slices into a single contiguous slice
    if (!currentSlices_.empty() && !currentSlices_.back().isArena) {
      auto& last = currentSlices_.back();
      currentHeaderBuffer_.insert(currentHeaderBuffer_.end(), sv.begin(),
                                  
                                           .arenaPtr = nullptr,
                                           .headerOffset = offset,
                                           .len = sv.size()});
    }
    currentChunkBytes_ += sv.size();
  }

  // ___________________________________________________________________________
  
  void writeChar(char c) { writeRawHeader(std::string_view(&c, 1)); }

  // ___________________________________________________________________________
  // Write an integer directly without intermediate heap allocations.
  template <typename IntegerType>
  requires std::is_integral_v<IntegerType>
  void writeInteger(IntegerType value) {
    std::array<char, 32> buf;
    auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value);
    AD_CORRECTNESS_CHECK(ec == std::errc{});
    writeRawHeader(std::string_view(buf.data(), ptr - buf.data()));
  }

  // ___________________________________________________________________________
    // Append an arena span, zero-copying it if it exceeds the threshold,
  // otherwise copying it into the header buffer to avoid iovec explosion.
  void writeArenaSpan(ql::span<const char> span) {
    if (span.empty()) {
      return;
    }

    

    if (!currentSlices_.empty() &&
        (currentChunkBytes_ + span.size() > config_.maxChunkBytes ||
         currentSlices_.size() >= config_.maxIovecs)) {
      flush();
    }

    currentSlices_.push_back(SliceRecord{.isArena = true,
                                         .arenaPtr = span.data(),
                                         .headerOffset = 0,
                                         .len = span.size()});
    currentChunkBytes_ += span.size();
    currentZeroCopyBytes_ += span.size();
    ++currentZeroCopySpans_;
  
    }
    std::string_view sv(iri.data(), iri.size());
    if ((ql::starts_with(sv, '<') && ql::ends_with(sv, '>')) ||
        ql::starts_with(sv, "_:")) {
      writeArenaSpan(iri);
    } else {
      writeChar('<');
      writeArenaSpan(iri);
      writeChar('>');
    }
  }

  // ___________________________________________________________________________
  // Write an RDF literal with optional datatype or language tag.
  void writeLiteral(ql::span<const char> content,
                    std::string_view datatype = "",
                    std::string_view langTag = "") {
    writeChar('"');
    writeArenaSpan(content);
    writeChar('"');
    if (!langTag.empty()) {
      if (!ql::starts_with(langTag, '@')) {
        writeChar('@');
      }
      writeRawHeader(langTag);
    } else if (!datatype.empty()) {
      writeRawHeader("^^<");
      writeRawHeader(datatype);
      writeChar('>');
    }
  }

  // Boolean literals are stored as "1"/"0" (length 1) or "true"/"false"
  // (length > 1). Only the latter can be emitted without the datatype
  // in Turtle/CSV/TSV, so we use this predicate to select the short form.
  static bool isBooleanShortForm(std::string_view sv) {
    return sv.length() > 1;
  }

  // _________________________________________________________________________
  
  void writeTerm(const qlever::constructExport::EvaluatedTermData& term,
                 ExportFormat format) {
    if (term.rdfTermDataType_ == nullptr) {
      // IRI, blank node, or vocab-indexed literal
      writeArenaSpan(ql::span<const char>(term.rdfTermString_.data(),
                                          term.rdfTermString_.size()));
      return;
    }

        // Encoded literal value (e.g. integer, decimal, boolean, double)
    // TODO<marvin> Extend short-form handling to double/float and other XSD numeric types.
    const bool includeDataType = (format == ExportFormat::NTriples);
    const auto* i = static_cast<const char*>(XSD_INT_TYPE);
    const auto* d = static_cast<const char*>(XSD_DECIMAL_TYPE);
    const auto* b = static_cast<const char*>(XSD_BOOLEAN_TYPE);

    const bool isShortForm =
        !includeDataType &&
        (term.rdfTermDataType_ == i || term.rdfTermDataType_ == d ||
         (term.rdfTermDataType_ == b && term.rdfTermString_.length() > 1));

    if (isShortForm) {
      writeRawHeader(term.rdfTermString_);
    } else {
      writeChar('"');
      writeRawHeader(term.rdfTermString_);
      writeRawHeader("\"^^<");
      writeRawHeader(term.rdfTermDataType_);
      writeChar('>');
    }
  }

  // ___________________________________________________________________________
    // Write a complete RDF triple from raw spans, zero-copying large spans
  // and copying small spans into the header buffer.
  void writeTriple(ExportFormat format, ql::span<const char> subject,
                   ql::span<const char> predicate,
                   ql::span<const char> objectLiteral,
                   std::string_view datatype = "",
                   std::string_view langTag = "") {
    auto guard = makeInvariantGuard();

    auto [delim, trailer] = formatDelimiterAndTrailer(format);
    if (format == ExportFormat::Turtle || format == ExportFormat::NTriples) {
      writeIri(subject);
      writeChar(delim);
      writeIri(predicate);
      writeChar(delim);
      writeLiteral(objectLiteral, datatype, langTag);
    } else {
      writeArenaSpan(subject);
      writeChar(delim);
      writeArenaSpan(predicate);
      writeChar(delim);
      writeLiteral(objectLiteral, datatype, langTag);
    }
    writeRawHeader(trailer);
    ++currentChunkTriples_;
    ++totalTriples_;
  }

  // ___________________________________________________________________________
    // Write a complete triple using `EvaluatedTermData` references.
  void writeTriple(ExportFormat format,
                   const qlever::constructExport::EvaluatedTermData& s,
                   const qlever::constructExport::EvaluatedTermData& p,
                   const qlever::constructExport::EvaluatedTermData& o) {
    auto guard = makeInvariantGuard();

    const char delim = (format == ExportFormat::Csv)
                           ? ','
                           : ((format == ExportFormat::Tsv) ? '\t' : ' ');
    writeTerm(s, format);
    writeChar(delim);
    writeTerm(p, format);
    writeChar(delim);
    writeTerm(o, format);
    if (format == ExportFormat::Turtle || format == ExportFormat::NTriples) {
      writeRawHeader(" .\n");
    } else {
      writeChar('\n');
    }
    ++currentChunkTriples_;
    ++totalTriples_;
  }

  // ___________________________________________________________________________
  // Write a tabular row of cell spans (CSV / TSV format).
  void writeRow(ExportFormat format,
                ql::span<const ql::span<const char>> cells) {
    auto guard = makeInvariantGuard();
    const char delimiter = (format == ExportFormat::Csv) ? ',' : '\t';
    bool first = true;
    for (const auto& cell : cells) {
      if (!first) {
        writeChar(delimiter);
      }
      first = false;
      writeArenaSpan(cell);
    }
    writeChar('\n');
  }

  // ___________________________________________________________________________
  
  // In streaming mode, forward chunk to sink and return `std::nullopt`.
  // In batch mode, return the assembled `ScatterGatherChunk`.
  std::optional<ScatterGatherChunk> flush() {
    if (currentSlices_.empty()) {
      return std::nullopt;
    }

    std::vector<struct iovec> iovecs;
    iovecs.reserve(currentSlices_.size());

    for (const auto& slice : currentSlices_) {
      const void* ptr =
          slice.isArena
              ? static_cast<const void*>(slice.arenaPtr)
              : static_cast<const void*>(currentHeaderBuffer_.data() +
                                         slice.headerOffset);
      iovecs.push_back(
          struct iovec{.iov_base = const_cast<void*>(ptr),
                       .iov_len = slice.len});
    }

    ScatterGatherChunk chunk(
        ScatterGatherChunk::Passkey{}, std::move(iovecs),
        std::move(currentHeaderBuffer_), currentChunkBytes_,
        currentChunkTriples_, currentZeroCopySpans_, currentZeroCopyBytes_);

    totalBytesWritten_ += currentChunkBytes_;
    totalZeroCopySpans_ += currentZeroCopySpans_;
    totalZeroCopyBytes_ += currentZeroCopyBytes_;
    ++chunksEmitted_;

    currentSlices_.clear();
    currentHeaderBuffer_.clear();
    currentHeaderBuffer_.reserve(config_.initialHeaderCapacity);
    currentChunkBytes_ = 0;
    currentChunkTriples_ = 0;
    currentZeroCopySpans_ = 0;
    currentZeroCopyBytes_ = 0;

    if (isStreaming_ && sink_) {
      sink_(std::move(chunk));
      return std::nullopt;
    }

    return chunk;
  }

  // ___________________________________________________________________________
    // Finalize the typestate transition (Law 2 / Law 3 & Architecture Standard § 3).
  // Consumes the streamer, flushes remaining chunk, and returns summary metrics.
  [[nodiscard]] ExportStreamSummary finalize() && {
    flush();
    ExportStreamSummary summary{totalTriples_, totalBytesWritten_,
                                chunksEmitted_};
    currentSlices_.clear();
    currentHeaderBuffer_.clear();
    currentChunkBytes_ = 0;
    currentChunkTriples_ = 0;
    return summary;
  }

  // ___________________________________________________________________________
  // Metrics & State Inspection
  [[nodiscard]] size_t currentChunkBytes() const noexcept {
    return currentChunkBytes_;
  }
  [[nodiscard]] size_t currentChunkTriples() const noexcept {
    return currentChunkTriples_;
  }
  [[nodiscard]] size_t currentNumSlices() const noexcept {
    return currentSlices_.size();
  }
  [[nodiscard]] uint64_t totalTriples() const noexcept { return totalTriples_; }
  [[nodiscard]] uint64_t totalBytesWritten() const noexcept {
    return totalBytesWritten_ + currentChunkBytes_;
  }
  [[nodiscard]] uint64_t chunksEmitted() const noexcept {
    return chunksEmitted_;
  }
  [[nodiscard]] uint64_t totalZeroCopySpans() const noexcept {
    return totalZeroCopySpans_ + currentZeroCopySpans_;
  }
  [[nodiscard]] uint64_t totalZeroCopyBytes() const noexcept {
    return totalZeroCopyBytes_ + currentZeroCopyBytes_;
  }
};

}  // namespace ql::export_streaming

#endif  // QLEVER_SRC_ENGINE_SCATTERGATHERARENASTREAMER_H

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SCATTERGATHERARENASTREAMER_H
#define QLEVER_SRC_ENGINE_SCATTERGATHERARENASTREAMER_H

#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "engine/ConstructTypes.h"
#include "engine/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "util/Exception.h"
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
  size_t maxChunkBytes = 1024 * 1024;  // Target chunk size: 1 MB
  size_t maxIovecs = 1024;             // Max iovecs per chunk (<= UIO_MAXIOV)
  size_t zeroCopyThresholdBytes = 64;  // Spans >= threshold are zero-copy
  size_t initialHeaderCapacity = 64 * 1024;  // 64 KB initial header buffer
};

// _____________________________________________________________________________
// An invariant-proven assembled scatter-gather chunk containing an array of
// `struct iovec` descriptors referencing external arena memory pages alongside
// an owned local header buffer for formatting delimiters and short tokens.
class ScatterGatherChunk {
 public:
  // Architecture Standard § 3.1: Passkey Idiom for restricted construction.
  class Passkey {
   private:
    friend class ScatterGatherChunkStreamer;
    explicit Passkey() = default;
  };

 private:
  std::vector<struct iovec> iovecs_;
  std::vector<char> headerBuffer_;
  size_t totalBytes_ = 0;
  size_t numTriples_ = 0;
  size_t zeroCopySpansCount_ = 0;
  size_t zeroCopyBytes_ = 0;

 public:
  ScatterGatherChunk() = default;

  // Restricted constructor via Passkey from ScatterGatherChunkStreamer.
  ScatterGatherChunk(Passkey, std::vector<struct iovec> iovecs,
                     std::vector<char> headerBuffer, size_t totalBytes,
                     size_t numTriples, size_t zeroCopySpansCount,
                     size_t zeroCopyBytes)
      : iovecs_{std::move(iovecs)},
        headerBuffer_{std::move(headerBuffer)},
        totalBytes_{totalBytes},
        numTriples_{numTriples},
        zeroCopySpansCount_{zeroCopySpansCount},
        zeroCopyBytes_{zeroCopyBytes} {}

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
  // Transmit chunk directly via POSIX writev(2) in a loop until all bytes are
  // sent.
  [[nodiscard]] ssize_t writeToFd(int fd) const {
    if (empty()) {
      return 0;
    }
    AD_CONTRACT_CHECK(fd >= 0);

    // The compile-time UIO_MAXIOV (default 1024 above) can exceed the
    // runtime sysconf(_SC_IOV_MAX) on some platforms, where an oversized
    // writev fails with EINVAL. Clamp each call to the runtime limit.
    static const size_t maxIovecsPerWritev = []() -> size_t {
      const long sysMax = ::sysconf(_SC_IOV_MAX);
      if (sysMax <= 0) {
        return 16;  // POSIX _XOPEN_IOV_MAX minimum guarantee.
      }
      return static_cast<size_t>(sysMax);
    }();

    std::vector<struct iovec> remainingIov = iovecs_;
    size_t offset = 0;
    ssize_t totalWritten = 0;

    while (offset < remainingIov.size()) {
      int count = static_cast<int>(
          std::min({remainingIov.size() - offset,
                    static_cast<size_t>(UIO_MAXIOV), maxIovecsPerWritev}));
      ssize_t bytes = ::writev(fd, remainingIov.data() + offset, count);
      if (bytes < 0) {
        if (errno == EINTR) {
          continue;
        }
        AD_THROW(absl::StrCat("writev failed (errno: ", strerror(errno), ")"));
      }
      // A 0 return for a nonzero request is unreachable on blocking fds;
      // fail loudly instead of spinning forever (e.g. on a non-blocking fd).
      AD_CORRECTNESS_CHECK(bytes != 0);
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
  // Flatten chunk into a std::string.
  [[nodiscard]] std::string toString() const {
    std::string result;
    result.resize(totalBytes_);
    copyToContiguous(ql::span<char>(result.data(), result.size()));
    return result;
  }
};

// _____________________________________________________________________________
// Deep Module: ScatterGatherChunkStreamer
//
// Assembles export chunks as a combination of fixed-size formatting headers and
// direct zero-copy `ql::span<const char>` pointers to existing arena memory
// pages. Automatically coalesces adjacent formatting tokens into unified header
// iovecs, manages chunk limits (bytes & max iovecs), and emits
// `ScatterGatherChunk`s.
class ScatterGatherChunkStreamer {
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
  std::vector<char> currentHeaderBuffer_;
  size_t currentChunkBytes_ = 0;
  size_t currentChunkTriples_ = 0;
  size_t currentZeroCopySpans_ = 0;
  size_t currentZeroCopyBytes_ = 0;

  // Aggregated export lifetime metrics
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
  // Construct in accumulating batch mode (where chunks are returned via
  // flush()).
  explicit ScatterGatherChunkStreamer(ScatterGatherConfig config = {})
      : config_{config}, sink_{nullptr}, isStreaming_{false} {
    currentHeaderBuffer_.reserve(config_.initialHeaderCapacity);
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
                                  sv.end());
      last.len += sv.size();
    } else {
      size_t offset = currentHeaderBuffer_.size();
      currentHeaderBuffer_.insert(currentHeaderBuffer_.end(), sv.begin(),
                                  sv.end());
      currentSlices_.push_back(SliceRecord{false, nullptr, offset, sv.size()});
    }
    currentChunkBytes_ += sv.size();
  }

  // ___________________________________________________________________________
  // Append a single character to the formatting header.
  void writeChar(char c) {
    const std::array<char, 1> buffer{c};
    writeRawHeader(std::string_view(buffer.data(), buffer.size()));
  }

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
  // Append a zero-copy arena span referencing external memory pages.
  void writeArenaSpan(ql::span<const char> span) {
    if (span.empty()) {
      return;
    }

    // Short strings are copied directly into the header buffer to avoid iovec
    // explosion
    if (span.size() < config_.zeroCopyThresholdBytes) {
      writeRawHeader(std::string_view(span.data(), span.size()));
      return;
    }

    if (!currentSlices_.empty() &&
        (currentChunkBytes_ + span.size() > config_.maxChunkBytes ||
         currentSlices_.size() >= config_.maxIovecs)) {
      flush();
    }

    currentSlices_.push_back(SliceRecord{true, span.data(), 0, span.size()});
    currentChunkBytes_ += span.size();
    currentZeroCopyBytes_ += span.size();
    ++currentZeroCopySpans_;
  }

  // ___________________________________________________________________________
  // Write an RDF IRI (wraps with <...> if not already enclosed or blank node).
  void writeIri(ql::span<const char> iri) {
    if (iri.empty()) {
      return;
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
  // Write an RDF literal with optional datatype or language tag. `content`
  // is transported verbatim (zero-copy); format-specific escaping is the
  // caller's responsibility, see `FastExportStreamFormatter::writeEscaped*`.
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

  // ___________________________________________________________________________
  // Write a single EvaluatedTermData term.
  void writeTerm(const qlever::constructExport::EvaluatedTermData& term,
                 ExportFormat format) {
    if (term.rdfTermDataType_ == nullptr) {
      // IRI, blank node, or vocab-indexed literal
      writeArenaSpan(ql::span<const char>(term.rdfTermString_.data(),
                                          term.rdfTermString_.size()));
      return;
    }

    // Encoded literal value (e.g. integer, decimal, boolean, double)
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
  // Write a complete RDF triple from raw spans with zero-copy literal
  // streaming.
  void writeTriple(ExportFormat format, ql::span<const char> subject,
                   ql::span<const char> predicate,
                   ql::span<const char> objectLiteral,
                   std::string_view datatype = "",
                   std::string_view langTag = "") {
    if (format == ExportFormat::Turtle || format == ExportFormat::NTriples) {
      writeIri(subject);
      writeChar(' ');
      writeIri(predicate);
      writeChar(' ');
      writeLiteral(objectLiteral, datatype, langTag);
      writeRawHeader(" .\n");
    } else if (format == ExportFormat::Csv) {
      writeArenaSpan(subject);
      writeChar(',');
      writeArenaSpan(predicate);
      writeChar(',');
      writeLiteral(objectLiteral, datatype, langTag);
      writeChar('\n');
    } else {
      AD_CORRECTNESS_CHECK(format == ExportFormat::Tsv);
      writeArenaSpan(subject);
      writeChar('\t');
      writeArenaSpan(predicate);
      writeChar('\t');
      writeLiteral(objectLiteral, datatype, langTag);
      writeChar('\n');
    }
    ++currentChunkTriples_;
    ++totalTriples_;
  }

  // ___________________________________________________________________________
  // Write a complete triple using EvaluatedTermData references.
  void writeTriple(ExportFormat format,
                   const qlever::constructExport::EvaluatedTermData& s,
                   const qlever::constructExport::EvaluatedTermData& p,
                   const qlever::constructExport::EvaluatedTermData& o) {
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
    const char delimiter = (format == ExportFormat::Csv) ? ',' : '\t';
    for (size_t i = 0; i < cells.size(); ++i) {
      if (i > 0) {
        writeChar(delimiter);
      }
      writeArenaSpan(cells[i]);
    }
    writeChar('\n');
  }

  // ___________________________________________________________________________
  // Flush current accumulated chunk.
  // In streaming mode, forwards chunk to sink and returns std::nullopt.
  // In batch mode, returns the assembled ScatterGatherChunk.
  std::optional<ScatterGatherChunk> flush() {
    if (currentSlices_.empty()) {
      return std::nullopt;
    }

    std::vector<struct iovec> iovecs;
    iovecs.reserve(currentSlices_.size());

    for (const auto& slice : currentSlices_) {
      const void* ptr =
          slice.isArena ? static_cast<const void*>(slice.arenaPtr)
                        : static_cast<const void*>(currentHeaderBuffer_.data() +
                                                   slice.headerOffset);
      // Positional construction: designated initializers are C++20-only.
      iovecs.push_back(iovec{const_cast<void*>(ptr), slice.len});
    }

    ScatterGatherChunk chunk(ScatterGatherChunk::Passkey{}, std::move(iovecs),
                             std::move(currentHeaderBuffer_),
                             currentChunkBytes_, currentChunkTriples_,
                             currentZeroCopySpans_, currentZeroCopyBytes_);

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
  // Finalizing typestate transition (Law 2 / Law 3 & Architecture Standard §
  // 3). Consumes the streamer, flushes remaining chunk, and returns summary
  // metrics.
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

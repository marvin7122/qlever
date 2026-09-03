// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_PROTOTYPES_FASTEXPORTSTREAMFORMATTER_H
#define QLEVER_SRC_ENGINE_EXPORT_PROTOTYPES_FASTEXPORTSTREAMFORMATTER_H

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "backports/span.h"
#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructTypes.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/Invariants.h"
#include "util/http/MediaTypes.h"

namespace ql::export_formatting {

// Export serialization formats supported by FastExportStreamFormatter.
enum class ExportFormat {
  Turtle,
  NTriples,
  Csv,
  Tsv
};

// Summary metrics returned upon finalizing an export stream.
struct ExportStreamSummary {
  uint64_t totalTriples_ = 0;
  uint64_t totalBytesWritten_ = 0;
  uint64_t chunksEmitted_ = 0;
};

namespace detail {

// Fast 256-entry lookup tables for character class checks.
constexpr std::array<bool, 256> makeCsvSpecialTable() {
  std::array<bool, 256> table{};
  table[static_cast<uint8_t>(',')] = true;
  table[static_cast<uint8_t>('"')] = true;
  table[static_cast<uint8_t>('\r')] = true;
  table[static_cast<uint8_t>('\n')] = true;
  return table;
}

constexpr std::array<bool, 256> makeTsvSpecialTable() {
  std::array<bool, 256> table{};
  table[static_cast<uint8_t>('\t')] = true;
  table[static_cast<uint8_t>('\n')] = true;
  table[static_cast<uint8_t>('\r')] = true;
  return table;
}

constexpr std::array<bool, 256> makeTurtleSpecialTable() {
  std::array<bool, 256> table{};
  table[static_cast<uint8_t>('\\')] = true;
  table[static_cast<uint8_t>('"')] = true;
  table[static_cast<uint8_t>('\n')] = true;
  table[static_cast<uint8_t>('\r')] = true;
  return table;
}

inline constexpr auto csvSpecialTable = makeCsvSpecialTable();
inline constexpr auto tsvSpecialTable = makeTsvSpecialTable();
inline constexpr auto turtleSpecialTable = makeTurtleSpecialTable();

// High-throughput 64-bit unrolled SWAR lookup scanner.
// Scans 8 bytes at a time; compilers auto-vectorize this loop effortlessly.
template <const std::array<bool, 256>& Table>
[[nodiscard]] inline bool hasSpecialCharacters(std::string_view sv) noexcept {
  const char* ptr = sv.data();
  size_t len = sv.size();
  while (len >= 8) {
    uint64_t val;
    std::memcpy(&val, ptr, sizeof(val));
    if (Table[static_cast<uint8_t>(val)] ||
        Table[static_cast<uint8_t>(val >> 8)] ||
        Table[static_cast<uint8_t>(val >> 16)] ||
        Table[static_cast<uint8_t>(val >> 24)] ||
        Table[static_cast<uint8_t>(val >> 32)] ||
        Table[static_cast<uint8_t>(val >> 40)] ||
        Table[static_cast<uint8_t>(val >> 48)] ||
        Table[static_cast<uint8_t>(val >> 56)]) {
      return true;
    }
    ptr += 8;
    len -= 8;
  }
  while (len > 0) {
    if (Table[static_cast<uint8_t>(*ptr)]) {
      return true;
    }
    ++ptr;
    --len;
  }
  return false;
}

}  // namespace detail

// _____________________________________________________________________________
// A streaming formatter that formats RDF terms, triples, and tabular rows
// directly into a reusable memory chunk.
//
// Formatting does not construct temporary std::string objects. All escaping,
// URI quoting, and datatype suffixes are written directly into the active
// chunk buffer; the buffer may grow when a single field exceeds its capacity.
class FastExportStreamFormatter
    : public ad_utility::WithInvariants<FastExportStreamFormatter> {
 public:
  using ChunkSink = std::function<void(std::string_view)>;
  static constexpr size_t DEFAULT_CHUNK_SIZE = 1024 * 1024;  // 1 MB
  static constexpr size_t SAFETY_WATERMARK = 4096;           // 4 KB margin

 private:
  // Internal managed chunk buffer (when in streaming mode)
  std::vector<char> managedBuffer_;
  // Non-owning view of current output memory
  char* bufferPtr_ = nullptr;
  size_t bufferCapacity_ = 0;
  size_t writePos_ = 0;

  // Stream chunk consumer
  ChunkSink sink_;
  bool isStreaming_ = false;

  // Aggregated export statistics
  uint64_t totalTriples_ = 0;
  uint64_t totalBytesWritten_ = 0;
  uint64_t chunksEmitted_ = 0;

 public:
  // ___________________________________________________________________________
  // Construct in streaming mode with an internal reusable chunk buffer and sink.
  explicit FastExportStreamFormatter(ChunkSink sink,
                                     size_t chunkSize = DEFAULT_CHUNK_SIZE)
      : managedBuffer_(std::max(chunkSize, SAFETY_WATERMARK * 2)),
        bufferPtr_(managedBuffer_.data()),
        bufferCapacity_(managedBuffer_.size()),
        writePos_(0),
        sink_(std::move(sink)),
        isStreaming_(true) {
    AD_CONTRACT_CHECK(sink_ != nullptr);
  }

  // ___________________________________________________________________________
  // Construct in fixed-span mode writing directly into a caller's buffer.
  explicit FastExportStreamFormatter(ql::span<char> targetSpan)
      : bufferPtr_(targetSpan.data()),
        bufferCapacity_(targetSpan.size()),
        writePos_(0),
        sink_(nullptr),
        isStreaming_(false) {
    AD_CONTRACT_CHECK(bufferPtr_ != nullptr || bufferCapacity_ == 0);
  }

  // ___________________________________________________________________________
  // Invariant verification required by WithInvariants<Derived>.
  void checkInvariants() const {
    AD_CORRECTNESS_CHECK(writePos_ <= bufferCapacity_);
    AD_CORRECTNESS_CHECK(bufferPtr_ != nullptr || bufferCapacity_ == 0);
  }

  // ___________________________________________________________________________
  // Reserve guaranteed contiguous write space, auto-flushing if streaming.
  void ensureAvailable(size_t bytesNeeded) {
    if (writePos_ + bytesNeeded <= bufferCapacity_) {
      return;
    }
    if (isStreaming_) {
      flush();
      if (bytesNeeded > bufferCapacity_) {
        // Expand chunk capacity to fit extra-large single field
        managedBuffer_.resize(bytesNeeded + SAFETY_WATERMARK);
        bufferPtr_ = managedBuffer_.data();
        bufferCapacity_ = managedBuffer_.size();
        writePos_ = 0;
      }
    } else {
      AD_THROW(absl::StrCat("FastExportStreamFormatter buffer overflow: needed ",
                            bytesNeeded, " bytes, remaining capacity ",
                            bufferCapacity_ - writePos_));
    }
  }

  // ___________________________________________________________________________
  // Directly append a raw character.
  void writeChar(char c) noexcept {
    ensureAvailable(1);
    bufferPtr_[writePos_++] = c;
  }

  // ___________________________________________________________________________
  // Directly append a raw string slice without escaping.
  void writeRaw(std::string_view sv) noexcept {
    if (sv.empty()) {
      return;
    }
    ensureAvailable(sv.size());
    std::memcpy(bufferPtr_ + writePos_, sv.data(), sv.size());
    writePos_ += sv.size();
  }

  // ___________________________________________________________________________
  // Write an integer directly without heap allocation.
  template <typename IntegerType>
  requires std::is_integral_v<IntegerType>
  void writeInteger(IntegerType value) noexcept {
    ensureAvailable(32);
    auto [ptr, ec] = std::to_chars(
        bufferPtr_ + writePos_, bufferPtr_ + bufferCapacity_, value);
    AD_CORRECTNESS_CHECK(ec == std::errc{});
    writePos_ = static_cast<size_t>(ptr - bufferPtr_);
  }

  // ___________________________________________________________________________
  // Write an IRI: wraps with <...> if not already enclosed.
  void writeIri(std::string_view iri) {
    if (ql::starts_with(iri, '<') && ql::ends_with(iri, '>')) {
      writeRaw(iri);
    } else if (ql::starts_with(iri, "_:")) {
      // Blank node IRI
      writeRaw(iri);
    } else {
      writeChar('<');
      writeRaw(iri);
      writeChar('>');
    }
  }

  // ___________________________________________________________________________
  // Write a blank node with prefix, ID, and optional suffix.
  void writeBlankNode(std::string_view prefix, uint64_t id,
                      std::string_view suffix = "") {
    writeRaw(prefix);
    writeInteger(id);
    writeRaw(suffix);
  }

  // ___________________________________________________________________________
  // Write a literal with optional datatype or language tag.
  void writeLiteral(std::string_view content, std::string_view datatype = "",
                    std::string_view langTag = "") {
    writeChar('"');
    writeRaw(content);
    writeChar('"');
    if (!langTag.empty()) {
      if (!ql::starts_with(langTag, '@')) {
        writeChar('@');
      }
      writeRaw(langTag);
    } else if (!datatype.empty()) {
      writeRaw("^^<");
      writeRaw(datatype);
      writeChar('>');
    }
  }

  // ___________________________________________________________________________
  // Fast RFC 4180 CSV field serializer with branchless escape scanning.
  void writeEscapedCsv(std::string_view field) {
    if (!detail::hasSpecialCharacters<detail::csvSpecialTable>(field)) {
      writeRaw(field);
      return;
    }

    // Must be quoted and double-quoted
    writeChar('"');
    size_t i = 0;
    while (i < field.size()) {
      size_t nextQuote = field.find('"', i);
      if (nextQuote == std::string_view::npos) {
        writeRaw(field.substr(i));
        break;
      }
      writeRaw(field.substr(i, nextQuote - i));
      writeRaw("\"\"");
      i = nextQuote + 1;
    }
    writeChar('"');
  }

  // ___________________________________________________________________________
  // Fast IANA-TSV field serializer.
  void writeEscapedTsv(std::string_view field) {
    if (!detail::hasSpecialCharacters<detail::tsvSpecialTable>(field)) {
      writeRaw(field);
      return;
    }

    for (char c : field) {
      if (c == '\t') {
        writeChar(' ');
      } else if (c == '\n') {
        writeRaw("\\n");
      } else if (c == '\r') {
        writeRaw("\\r");
      } else {
        writeChar(c);
      }
    }
  }

  // ___________________________________________________________________________
  // Fast Turtle / NTriples normalized literal serializer.
  // Escapes backslashes, quotes, and newlines inside the literal content.
  void writeEscapedTurtleLiteral(std::string_view normLiteral) {
    AD_CONTRACT_CHECK(ql::starts_with(normLiteral, '"'));
    size_t posSecondQuote = normLiteral.find('"', 1);
    AD_CONTRACT_CHECK(posSecondQuote != std::string_view::npos);
    size_t posLastQuote = normLiteral.rfind('"');

    // If no internal special chars, write directly
    if (posSecondQuote == posLastQuote &&
        !detail::hasSpecialCharacters<detail::turtleSpecialTable>(normLiteral)) {
      writeRaw(normLiteral);
      return;
    }

    // Write opening quote
    writeChar('"');
    std::string_view content = normLiteral.substr(1, posLastQuote - 1);
    for (char c : content) {
      if (c == '\\') {
        writeRaw("\\\\");
      } else if (c == '"') {
        writeRaw("\\\"");
      } else if (c == '\n') {
        writeRaw("\\n");
      } else if (c == '\r') {
        writeRaw("\\r");
      } else {
        writeChar(c);
      }
    }
    // Write closing quote and any trailing lang/datatype suffix
    writeRaw(normLiteral.substr(posLastQuote));
  }

  // ___________________________________________________________________________
  // Write a single EvaluatedTermData term directly according to ExportFormat.
  void writeTerm(const qlever::constructExport::EvaluatedTermData& term,
                 ExportFormat format) {
    if (term.rdfTermDataType_ == nullptr) {
      // IRI, blank node, or vocab-indexed literal
      if (format == ExportFormat::Turtle || format == ExportFormat::NTriples) {
        if (ql::starts_with(term.rdfTermString_, '"')) {
          writeEscapedTurtleLiteral(term.rdfTermString_);
        } else {
          writeRaw(term.rdfTermString_);
        }
      } else if (format == ExportFormat::Csv) {
        writeEscapedCsv(term.rdfTermString_);
      } else {
        AD_CORRECTNESS_CHECK(format == ExportFormat::Tsv);
        writeEscapedTsv(term.rdfTermString_);
      }
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
      if (format == ExportFormat::Csv) {
        writeEscapedCsv(term.rdfTermString_);
      } else if (format == ExportFormat::Tsv) {
        writeEscapedTsv(term.rdfTermString_);
      } else {
        writeRaw(term.rdfTermString_);
      }
    } else {
      // Fully-qualified form: "value"^^<datatype>
      writeChar('"');
      writeRaw(term.rdfTermString_);
      writeRaw("\"^^<");
      writeRaw(term.rdfTermDataType_);
      writeChar('>');
    }
  }

  // ___________________________________________________________________________
  // Write a complete EvaluatedTriple directly to the active chunk.
  void writeTriple(ExportFormat format,
                   const qlever::constructExport::EvaluatedTermData& s,
                   const qlever::constructExport::EvaluatedTermData& p,
                   const qlever::constructExport::EvaluatedTermData& o) {
    auto guard = makeInvariantGuard();

    if (format == ExportFormat::Turtle || format == ExportFormat::NTriples) {
      writeTerm(s, format);
      writeChar(' ');
      writeTerm(p, format);
      writeChar(' ');
      writeTerm(o, format);
      writeRaw(" .\n");
    } else if (format == ExportFormat::Csv) {
      writeTerm(s, format);
      writeChar(',');
      writeTerm(p, format);
      writeChar(',');
      writeTerm(o, format);
      writeChar('\n');
    } else {
      AD_CORRECTNESS_CHECK(format == ExportFormat::Tsv);
      writeTerm(s, format);
      writeChar('\t');
      writeTerm(p, format);
      writeChar('\t');
      writeTerm(o, format);
      writeChar('\n');
    }
    ++totalTriples_;
  }

  // ___________________________________________________________________________
  // Convenience overload for EvaluatedTriple struct.
  void writeTriple(ExportFormat format,
                   const qlever::constructExport::EvaluatedTriple& triple) {
    AD_CONTRACT_CHECK(triple.subject_ != nullptr);
    AD_CONTRACT_CHECK(triple.predicate_ != nullptr);
    AD_CONTRACT_CHECK(triple.object_ != nullptr);
    writeTriple(format, *triple.subject_, *triple.predicate_, *triple.object_);
  }

  // ___________________________________________________________________________
  // Write a tabular row for SELECT query export.
  void writeRow(ExportFormat format, ql::span<const std::string_view> cells) {
    auto guard = makeInvariantGuard();
    const char delimiter = (format == ExportFormat::Csv) ? ',' : '\t';
    for (size_t i = 0; i < cells.size(); ++i) {
      if (i > 0) {
        writeChar(delimiter);
      }
      if (format == ExportFormat::Csv) {
        writeEscapedCsv(cells[i]);
      } else {
        writeEscapedTsv(cells[i]);
      }
    }
    writeChar('\n');
  }

  // ___________________________________________________________________________
  // Flush current chunk to sink in streaming mode.
  void flush() {
    if (writePos_ == 0) {
      return;
    }
    if (isStreaming_ && sink_) {
      sink_(std::string_view(bufferPtr_, writePos_));
      ++chunksEmitted_;
    }
    totalBytesWritten_ += writePos_;
    writePos_ = 0;
  }

  // ___________________________________________________________________________
  // Finalizing typestate transition (Law 2 / Law 3).
  // Consumes the formatter, flushes remaining content, and returns summary.
  [[nodiscard]] ExportStreamSummary finalize() && {
    flush();
    ExportStreamSummary summary{totalTriples_, totalBytesWritten_,
                                chunksEmitted_};
    // Invalidate buffer
    bufferPtr_ = nullptr;
    bufferCapacity_ = 0;
    writePos_ = 0;
    return summary;
  }

  // ___________________________________________________________________________
  // Inspect current buffer slice.
  [[nodiscard]] std::string_view currentChunk() const noexcept {
    return std::string_view(bufferPtr_, writePos_);
  }

  [[nodiscard]] size_t bytesBuffered() const noexcept { return writePos_; }
  [[nodiscard]] uint64_t totalBytesWritten() const noexcept {
    return totalBytesWritten_ + writePos_;
  }
  [[nodiscard]] uint64_t totalTriples() const noexcept { return totalTriples_; }
};

// Helper function to map ad_utility::MediaType to ExportFormat.
[[nodiscard]] inline ExportFormat toExportFormat(ad_utility::MediaType mediaType) {
  using enum ad_utility::MediaType;
  switch (mediaType) {
    case turtle:
      return ExportFormat::Turtle;
    case ntriples:
      return ExportFormat::NTriples;
    case csv:
      return ExportFormat::Csv;
    case tsv:
      return ExportFormat::Tsv;
    default:
      AD_THROW(absl::StrCat("Unsupported media type for export formatter: ",
                            ad_utility::toString(mediaType)));
  }
}

}  // namespace ql::export_formatting

#endif  // QLEVER_SRC_ENGINE_EXPORT_PROTOTYPES_FASTEXPORTSTREAMFORMATTER_H

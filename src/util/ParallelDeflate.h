// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELDEFLATE_H
#define QLEVER_SRC_UTIL_PARALLELDEFLATE_H

#include <cstdint>
#include <string>
#include <string_view>

#include "util/http/ContentEncodingHelper.h"

// Parallel HTTP response compression in the style of pigz.
//
// Each block of the response is compressed independently (on any thread) to a
// raw deflate stream that ends with a sync flush: the block is byte-aligned
// and not final, so the compressed blocks can be concatenated as they are. A
// single `ParallelDeflateStream` then frames the concatenation as ONE valid
// zlib (`Content-Encoding: deflate`) or gzip stream: one header, the blocks
// in emission order, a final empty deflate block, and the trailer with the
// checksum combined from the per-block checksums (`adler32_combine` /
// `crc32_combine`). The result is a single-member stream, so clients that
// stop after the first gzip member still read everything.
namespace ad_utility::streams {

using ad_utility::content_encoding::CompressionMethod;

// One independently compressed block of a parallel deflate stream.
struct DeflateBlock {
  // Raw deflate data (no zlib/gzip framing). Byte-aligned and not final. Empty
  // iff `uncompressedSize_ == 0`.
  std::string compressed_;
  // Adler-32 (DEFLATE) or CRC-32 (GZIP) of the uncompressed input.
  uint32_t check_ = 0;
  uint64_t uncompressedSize_ = 0;

  [[nodiscard]] bool empty() const noexcept { return uncompressedSize_ == 0; }
};

// zlib level 1, the level of the serial `compressStream`.
inline constexpr int kParallelDeflateLevel = 1;

// Compress `input` as one independent block for `method` (DEFLATE or GZIP).
// Uses no shared state, so it is safe to call on many threads at once. An
// empty input yields an empty block.
DeflateBlock compressDeflateBlock(std::string_view input,
                                  CompressionMethod method,
                                  int level = kParallelDeflateLevel);

// The framing of one parallel deflate stream. Emit `header()`, then the
// `compressed_` bytes of every block in the order passed to `append`, then
// `trailer()`.
class ParallelDeflateStream {
 public:
  // `method` must be DEFLATE or GZIP.
  explicit ParallelDeflateStream(CompressionMethod method);

  // The zlib (RFC 1950) or gzip (RFC 1952) header.
  [[nodiscard]] std::string header() const;

  // Account for `block`, which the caller emits next.
  void append(const DeflateBlock& block);

  // A final empty deflate block plus the checksum trailer over all appended
  // blocks.
  [[nodiscard]] std::string trailer() const;

  [[nodiscard]] uint64_t uncompressedSize() const noexcept {
    return uncompressedSize_;
  }

 private:
  CompressionMethod method_;
  uint32_t check_;
  uint64_t uncompressedSize_ = 0;
};

}  // namespace ad_utility::streams

#endif  // QLEVER_SRC_UTIL_PARALLELDEFLATE_H

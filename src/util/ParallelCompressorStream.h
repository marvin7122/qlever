// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELCOMPRESSORSTREAM_H
#define QLEVER_SRC_UTIL_PARALLELCOMPRESSORSTREAM_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>

#include "backports/asio.h"
#include "util/Generator.h"
#include "util/http/ContentEncodingHelper.h"

namespace ad_utility::streams {

// Like `compressStream` (see `CompressorStream.h`), the concatenation of the
// yielded strings is the concatenation of the strings of the `range`,
// compressed with the `compressionMethod` (or unchanged for
// `content_encoding::CompressionMethod::NONE`). The difference is that the
// compression runs on the `executor`, for up to `maxNumberOfBlocksInFlight`
// blocks at the same time, in the way of `pigz`:
//
// 1. The input is cut into blocks of at least
//    `MIN_NUMBER_OF_BYTES_PER_COMPRESSION_BLOCK` bytes (except the last one).
// 2. Each block is compressed on its own into raw deflate data that ends with
//    a sync flush (so it is byte-aligned and not final). The last 32 KiB of
//    the input before the block are its dictionary, so the compression may
//    refer back into the previous block, as a sequential compressor would.
// 3. The output is ONE zlib (`DEFLATE`) or gzip (`GZIP`) stream: the header,
//    the blocks in their original order, an empty final deflate block, and
//    the trailer, whose checksum is combined from the checksums of the blocks.
//    It is not a sequence of gzip members (which some clients do not read past
//    the first one), and the zlib format has no members at all.
//
// The compressed bytes differ from those of `compressStream`, but both
// decompress to the same output.
//
// IMPORTANT: As for `AsyncTransformView`, this generator must not be consumed
// on a thread of the `executor`, and the execution context of the `executor`
// has to outlive the generator. The `range` itself is read on the thread that
// consumes the generator. `maxNumberOfBlocksInFlight` has to be at least 1.
cppcoro::generator<std::string> compressStreamInParallel(
    cppcoro::generator<std::string> range,
    content_encoding::CompressionMethod compressionMethod,
    ql::any_io_executor executor, size_t maxNumberOfBlocksInFlight);

// The minimal size of a block (except the last one) that is compressed on its
// own. It is the default block size of `pigz`. The response bodies of the
// exports come in pieces of about 1 MiB (the buffer of the
// `stream_generator`), so there, a block is typically one such piece.
constexpr size_t MIN_NUMBER_OF_BYTES_PER_COMPRESSION_BLOCK = 128 * 1024;

namespace detail::parallelCompression {
// The parts of `compressStreamInParallel`, exposed for testing.

// The size of the deflate window, which is the maximal distance of a
// reference back into the previous input.
constexpr size_t NUMBER_OF_BYTES_IN_DICTIONARY = 32 * 1024;
static_assert(MIN_NUMBER_OF_BYTES_PER_COMPRESSION_BLOCK >=
                  NUMBER_OF_BYTES_IN_DICTIONARY,
              "The dictionary of a block must lie in the previous block");

// The two formats around a deflate stream that `compressStreamInParallel`
// writes (see `CompressionMethod`).
enum class StreamFormat { Zlib, Gzip };

// A block of the input, together with the (at most 32 KiB of) input right
// before it.
struct UncompressedBlock {
  std::string bytes_;
  std::string dictionary_;
};

// The checksum of uncompressed bytes that the trailer of a stream stores: a
// CRC-32 for gzip, an Adler-32 for zlib.
struct Crc32 {
  uint32_t value_;
};
struct Adler32 {
  uint32_t value_;
};
using Checksum = std::variant<Crc32, Adler32>;

// A block, compressed into raw deflate data that ends with a sync flush, and
// the checksum of its uncompressed bytes.
struct CompressedBlock {
  std::string bytes_;
  Checksum checksum_;
  size_t numberOfUncompressedBytes_;
};

// Compress the `block` (see `CompressedBlock`).
CompressedBlock compressBlock(const UncompressedBlock& block,
                              StreamFormat format);

// The header of the stream, and the trailer for the checksum and the total
// number of bytes of all the blocks, which are combined while the blocks are
// emitted.
class StreamFrame {
  // The checksum of all the blocks so far, which also determines the format.
  Checksum checksum_;
  // Only the gzip trailer stores the size (modulo 2^32).
  uint64_t numberOfUncompressedBytes_ = 0;

 public:
  explicit StreamFrame(StreamFormat format);
  // The header of the stream.
  std::string header() const;
  // Account for the `block`, which is the next one in the stream. Its checksum
  // must be of the kind of the format of the stream.
  void add(const CompressedBlock& block);
  // The empty final deflate block and the trailer of the stream, for all the
  // blocks that have been added.
  std::string finalBlockAndTrailer() const;
};
}  // namespace detail::parallelCompression
}  // namespace ad_utility::streams

#endif  // QLEVER_SRC_UTIL_PARALLELCOMPRESSORSTREAM_H

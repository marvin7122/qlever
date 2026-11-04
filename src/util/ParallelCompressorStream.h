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

#include "util/Executor.h"
#include "util/Generator.h"
#include "util/MemorySize/MemorySize.h"
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
    qlever::Executor executor, size_t maxNumberOfBlocksInFlight);

// The minimal size of a block (except the last one) that is compressed on its
// own. It is the default block size of `pigz`. A piece of the input that is at
// least this large becomes one block on its own (for example, each piece of
// about 1 MiB of an export, the buffer of the `stream_generator`); smaller
// pieces are joined until the block reaches this size.
constexpr size_t MIN_NUMBER_OF_BYTES_PER_COMPRESSION_BLOCK = 128 * 1024;

namespace detail::parallelCompression {
// The parts of `compressStreamInParallel`, exposed for testing.

// The size of the deflate window (32 KiB), which is the maximal distance of a
// reference back into the previous input. (It is spelled in bytes, because the
// `_kB` literal of `MemorySize` means 1000 bytes.)
constexpr ad_utility::MemorySize DICTIONARY_SIZE =
    ad_utility::MemorySize::bytes(32 * 1024);
static_assert(MIN_NUMBER_OF_BYTES_PER_COMPRESSION_BLOCK >=
                  DICTIONARY_SIZE.getBytes(),
              "The dictionary of a block must lie in the previous block");

// The two formats around a deflate stream that `compressStreamInParallel`
// writes (see `CompressionMethod`).
enum class StreamFormat { Zlib, Gzip };

// A block of the input, together with the input right before it.
struct UncompressedBlock {
  std::string bytes_;
  // The (at most `DICTIONARY_SIZE`) bytes of the input right before the
  // `bytes_`, to which the compression of the block may refer back. Empty for
  // the first block. `compressBlock` checks the bound.
  std::string dictionary_;
};

// The checksum of uncompressed bytes that the trailer of a stream stores: a
// CRC-32 for gzip, an Adler-32 for zlib.
struct Crc32 {
  uint32_t value_ = 0;
};
struct Adler32 {
  uint32_t value_ = 0;
};
struct Checksum {
  std::variant<Crc32, Adler32> value_;

  // The format whose trailer stores this kind of checksum.
  StreamFormat format() const noexcept {
    return std::holds_alternative<Crc32>(value_) ? StreamFormat::Gzip
                                                 : StreamFormat::Zlib;
  }
};

// Raw deflate data (without the header and trailer of a zlib or gzip stream)
// that ends with a sync flush, so it is byte-aligned and not final.
struct RawDeflateData {
  std::string bytes_;
};

// A block, compressed into raw deflate data, and the checksum and the size of
// its uncompressed bytes.
struct CompressedBlock {
  RawDeflateData deflateData_;
  Checksum checksum_;
  ad_utility::MemorySize uncompressedSize_;
};

// Compress the `block` into raw deflate data that ends with a sync flush, using
// its `dictionary_` (which must have at most `DICTIONARY_SIZE` bytes, else an
// `AD_CORRECTNESS_CHECK` fails) as the preset dictionary. The deflate data
// does not depend on the `format`, which only selects the kind of the checksum
// of the result.
CompressedBlock compressBlock(const UncompressedBlock& block,
                              StreamFormat format);

// The frame around the blocks of a stream: its header, and its trailer with
// the checksum (and, for gzip, the size) of all the blocks, which are combined
// while the blocks are added.
class StreamFrame {
  // The checksum of all the blocks so far, which also determines the format.
  Checksum checksum_;
  // The size of all the blocks so far. Only the gzip trailer stores it (modulo
  // 2^32).
  ad_utility::MemorySize uncompressedSize_;

 public:
  explicit StreamFrame(StreamFormat format) noexcept;

  // The header of the stream (the same for all streams of a format).
  std::string header() const;

  // Account for the `block`, which is the next one in the stream. Its checksum
  // must be of the kind of the format of the stream, else an
  // `AD_CORRECTNESS_CHECK` fails.
  void add(const CompressedBlock& block);

  // The empty final deflate block and the trailer of the stream, for all the
  // blocks that have been added.
  std::string finalBlockAndTrailer() const;
};
}  // namespace detail::parallelCompression
}  // namespace ad_utility::streams

#endif  // QLEVER_SRC_UTIL_PARALLELCOMPRESSORSTREAM_H

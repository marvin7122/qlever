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
#include <optional>
#include <string>
#include <variant>

#include "util/Allocator.h"
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
// 1. The input is cut into blocks of at least `minBlockSize` (except the last
//    one), see `DEFAULT_COMPRESSION_BLOCK_SIZE` for what a block is.
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
// consumes the generator. `maxNumberOfBlocksInFlight` has to be at least 1,
// and `minBlockSize` has to lie between `MIN_COMPRESSION_BLOCK_SIZE` and
// `MAX_COMPRESSION_BLOCK_SIZE` (else an `AD_CONTRACT_CHECK` fails).
//
// The memory of the blocks in flight is counted against the `allocator` (that
// of the query): for each block, the bytes of its input (and dictionary) from
// the moment it is cut until it has been compressed, the `deflateBound` of its
// input while it is compressed, and the size of its output until it has been
// yielded. If the memory limit of the `allocator` would be exceeded, the
// generator throws the usual `AllocationExceedsLimitException`. All the memory
// is released again on every path, including exceptions and the early
// destruction of the generator. A `CompressionMethod::NONE` holds no blocks.
cppcoro::generator<std::string> compressStreamInParallel(
    cppcoro::generator<std::string> range,
    content_encoding::CompressionMethod compressionMethod,
    qlever::Executor executor, size_t maxNumberOfBlocksInFlight,
    ad_utility::MemorySize minBlockSize, qlever::Allocator<char> allocator);

namespace detail::parallelCompression {
// The size of the deflate window (32 KiB), which is the maximal distance of a
// reference back into the previous input. (It is spelled in bytes, because the
// `_kB` literal of `MemorySize` means 1000 bytes.)
constexpr ad_utility::MemorySize DICTIONARY_SIZE =
    ad_utility::MemorySize::bytes(32 * 1024);
}  // namespace detail::parallelCompression

// A compression block is a run of consecutive pieces of the input that is
// compressed on its own, by one task on the executor.
// `compressStreamInParallel` joins pieces until the block has at least the
// block size (a piece that is that large becomes one block on its own, for
// example, each piece of about 1 MiB of an export, the buffer of the
// `stream_generator`), so only the last block is smaller. The default block
// size is the default of `pigz`, which balances three things:
// - Compression ratio: a block starts with the last 32 KiB of the previous
//   block as its dictionary, so a block boundary costs little, and the loss
//   per boundary is bounded by that window. The relative loss is the larger,
//   the closer the block size is to the window.
// - Parallelism: the input of a response is compressed in parallel only if it
//   has several blocks, so a smaller block size makes a short response use more
//   threads.
// - Memory: at most `maxNumberOfBlocksInFlight` blocks are in flight, each
//   holding its input and an output buffer of `deflateBound` bytes, so the
//   memory is about `maxNumberOfBlocksInFlight * (block size + compressed
//   bound)`. It is counted against the memory limit of the query (see
//   `compressStreamInParallel`).
// It is the runtime parameter `parallel-response-compression-block-size`. The
// output is one valid zlib or gzip stream for every block size.
constexpr ad_utility::MemorySize DEFAULT_COMPRESSION_BLOCK_SIZE =
    ad_utility::MemorySize::bytes(128 * 1024);

// The smallest valid block size: the dictionary of a block must lie in the
// previous block, so every block that has a successor needs at least
// `DICTIONARY_SIZE` bytes.
constexpr ad_utility::MemorySize MIN_COMPRESSION_BLOCK_SIZE =
    detail::parallelCompression::DICTIONARY_SIZE;

// The largest valid block size. A larger block does not improve the ratio
// noticeably (the window is only 32 KiB), but delays the first byte of the
// response and multiplies the memory in flight: with the default of 6 threads
// per query, 12 blocks of 16 MiB hold about 400 MiB.
constexpr ad_utility::MemorySize MAX_COMPRESSION_BLOCK_SIZE =
    ad_utility::MemorySize::bytes(16 * 1024 * 1024);

static_assert(DEFAULT_COMPRESSION_BLOCK_SIZE.getBytes() >=
                  MIN_COMPRESSION_BLOCK_SIZE.getBytes() &&
              DEFAULT_COMPRESSION_BLOCK_SIZE.getBytes() <=
                  MAX_COMPRESSION_BLOCK_SIZE.getBytes());

namespace detail::parallelCompression {
// The parts of `compressStreamInParallel`, exposed for testing.

// The two formats around a deflate stream that `compressStreamInParallel`
// writes (see `CompressionMethod`).
enum class StreamFormat { Zlib, Gzip };

// A reservation of a number of bytes of the memory budget of an allocator (the
// memory limit of a query) for its lifetime. The bytes are obtained from the
// `allocator` and returned by the destructor, but they are never accessed, so
// they only take address space. The constructor throws the usual
// `AllocationExceedsLimitException` if the budget does not suffice. A default
// constructed reservation holds nothing.
class MemoryReservation {
  std::optional<qlever::Allocator<char>> allocator_;
  char* data_ = nullptr;
  size_t numberOfBytes_ = 0;

 public:
  MemoryReservation() = default;
  MemoryReservation(qlever::Allocator<char> allocator, size_t numberOfBytes);
  MemoryReservation(MemoryReservation&& other) noexcept;
  MemoryReservation& operator=(MemoryReservation&& other) noexcept;
  MemoryReservation(const MemoryReservation&) = delete;
  MemoryReservation& operator=(const MemoryReservation&) = delete;
  ~MemoryReservation();

  // The number of reserved bytes.
  size_t numberOfBytes() const noexcept { return numberOfBytes_; }

 private:
  // Return the reserved bytes to the allocator (if any).
  void release() noexcept;
};

// A block of the input, together with the input right before it.
struct UncompressedBlock {
  std::string bytes_;
  // The (at most `DICTIONARY_SIZE`) bytes of the input right before the
  // `bytes_`, to which the compression of the block may refer back. Empty for
  // the first block. `compressBlock` checks the bound.
  std::string dictionary_;
  // The memory of the `bytes_` and the `dictionary_`, which is released when
  // the block is destroyed.
  MemoryReservation reservation_ = {};
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
  // The memory of the `deflateData_`, which is released when the block is
  // destroyed.
  MemoryReservation reservation_ = {};
};

// Compress the `block` into raw deflate data that ends with a sync flush, using
// its `dictionary_` (which must have at most `DICTIONARY_SIZE` bytes, else an
// `AD_CORRECTNESS_CHECK` fails) as the preset dictionary. The deflate data
// does not depend on the `format`, which only selects the kind of the checksum
// of the result. While the block is compressed, `deflateBound` bytes of the
// output buffer are reserved at the `allocator`; the reservation of the result
// is its size.
CompressedBlock compressBlock(const UncompressedBlock& block,
                              StreamFormat format,
                              const qlever::Allocator<char>& allocator);

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

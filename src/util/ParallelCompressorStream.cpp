// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ParallelCompressorStream.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <zlib.h>

#include <algorithm>
#include <limits>
#include <variant>

#include "util/Exception.h"
#include "util/views/AsyncTransformView.h"

namespace ad_utility::streams {
namespace detail::parallelCompression {

namespace {
// The largest number of bytes that zlib reads or writes in one call.
constexpr size_t MAX_NUMBER_OF_BYTES_PER_ZLIB_CALL =
    std::numeric_limits<uInt>::max();

// Append the four bytes of the `value`, least significant byte first (the byte
// order of the gzip trailer).
void appendLittleEndian(std::string& out, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}
// Append the four bytes of the `value`, most significant byte first (the byte
// order of the zlib trailer).
void appendBigEndian(std::string& out, uint32_t value) {
  for (size_t i = 4; i > 0; --i) {
    out.push_back(static_cast<char>((value >> (8 * (i - 1))) & 0xFF));
  }
}

// The checksum of the `format` for the `bytes`.
Checksum checksum(StreamFormat format, std::string_view bytes) {
  const auto* data = reinterpret_cast<const Bytef*>(bytes.data());
  if (format == StreamFormat::Gzip) {
    return Crc32{static_cast<uint32_t>(
        crc32_z(crc32(0, Z_NULL, 0), data, bytes.size()))};
  }
  return Adler32{static_cast<uint32_t>(
      adler32_z(adler32(0, Z_NULL, 0), data, bytes.size()))};
}
}  // namespace

// _____________________________________________________________________________
CompressedBlock compressBlock(const UncompressedBlock& block,
                              StreamFormat format) {
  z_stream stream{};
  // Raw deflate (negative window bits) with the same level, window, and memory
  // level as the sequential `compressStream`.
  int result = deflateInit2(&stream, Z_BEST_SPEED, Z_DEFLATED, -MAX_WBITS, 8,
                            Z_DEFAULT_STRATEGY);
  if (result != Z_OK) {
    AD_THROW(
        absl::StrCat("Initializing the deflate compression of an HTTP "
                     "response failed with zlib error ",
                     result));
  }
  absl::Cleanup endStream{[&stream]() { deflateEnd(&stream); }};
  if (!block.dictionary_.empty()) {
    AD_CORRECTNESS_CHECK(block.dictionary_.size() <=
                         NUMBER_OF_BYTES_IN_DICTIONARY);
    result = deflateSetDictionary(
        &stream, reinterpret_cast<const Bytef*>(block.dictionary_.data()),
        static_cast<uInt>(block.dictionary_.size()));
    AD_CORRECTNESS_CHECK(result == Z_OK);
  }

  std::string out;
  out.resize(deflateBound(&stream, block.bytes_.size()) + 16);
  std::string_view remainingInput = block.bytes_;
  // The input is passed in pieces that fit into the `uInt` of zlib, the last
  // one with `Z_SYNC_FLUSH`. For each piece, `deflate` is called until it
  // leaves space in the output, which means that it has consumed the piece
  // (and, for the last one, completed the flush).
  bool isLastPiece = false;
  while (!isLastPiece) {
    size_t numberOfBytesInPiece =
        std::min(remainingInput.size(), MAX_NUMBER_OF_BYTES_PER_ZLIB_CALL);
    isLastPiece = numberOfBytesInPiece == remainingInput.size();
    stream.next_in =
        reinterpret_cast<Bytef*>(const_cast<char*>(remainingInput.data()));
    stream.avail_in = static_cast<uInt>(numberOfBytesInPiece);
    remainingInput.remove_prefix(numberOfBytesInPiece);
    do {
      if (stream.total_out == out.size()) {
        out.resize(2 * out.size());
      }
      stream.next_out = reinterpret_cast<Bytef*>(out.data() + stream.total_out);
      stream.avail_out = static_cast<uInt>(std::min(
          out.size() - stream.total_out, MAX_NUMBER_OF_BYTES_PER_ZLIB_CALL));
      result = deflate(&stream, isLastPiece ? Z_SYNC_FLUSH : Z_NO_FLUSH);
      // `Z_BUF_ERROR` only means that no progress was possible in this call,
      // which is not an error for the loop.
      AD_CORRECTNESS_CHECK(result == Z_OK || result == Z_BUF_ERROR);
    } while (stream.avail_out == 0);
    AD_CORRECTNESS_CHECK(stream.avail_in == 0);
  }
  // The buffer was sized for incompressible input, so it is shrunk, as the
  // block may wait in memory until it is its turn to be sent.
  out.resize(stream.total_out);
  out.shrink_to_fit();
  return {std::move(out), checksum(format, block.bytes_), block.bytes_.size()};
}

// _____________________________________________________________________________
StreamFrame::StreamFrame(StreamFormat format)
    : checksum_{checksum(format, {})} {}

// _____________________________________________________________________________
std::string StreamFrame::header() const {
  if (std::holds_alternative<Crc32>(checksum_)) {
    // ID1, ID2, CM = deflate, FLG = none, MTIME = 0 (unknown), XFL = 4
    // (fastest compression), OS = 255 (unknown), see RFC 1952, section 2.3.
    return {"\x1f\x8b\x08\x00\x00\x00\x00\x00\x04\xff", 10};
  }
  // CMF = deflate with a 32 KiB window, FLG = fastest compression, no preset
  // dictionary, and the check bits, see RFC 1950, section 2.2.
  return {"\x78\x01", 2};
}

// _____________________________________________________________________________
void StreamFrame::add(const CompressedBlock& block) {
  AD_CORRECTNESS_CHECK(block.checksum_.index() == checksum_.index(),
                       "A block was compressed for another stream format");
  auto length = static_cast<z_off_t>(block.numberOfUncompressedBytes_);
  if (auto* crc = std::get_if<Crc32>(&checksum_)) {
    crc->value_ = static_cast<uint32_t>(crc32_combine(
        crc->value_, std::get<Crc32>(block.checksum_).value_, length));
  } else {
    auto& adler = std::get<Adler32>(checksum_);
    adler.value_ = static_cast<uint32_t>(adler32_combine(
        adler.value_, std::get<Adler32>(block.checksum_).value_, length));
  }
  numberOfUncompressedBytes_ += block.numberOfUncompressedBytes_;
}

// _____________________________________________________________________________
std::string StreamFrame::finalBlockAndTrailer() const {
  // An empty deflate block with fixed Huffman codes and the final bit set
  // (which is what zlib writes for `Z_FINISH` without input). It starts on a
  // byte boundary, because every block ends with a sync flush.
  std::string out{"\x03\x00", 2};
  if (const auto* crc = std::get_if<Crc32>(&checksum_)) {
    appendLittleEndian(out, crc->value_);
    appendLittleEndian(out, static_cast<uint32_t>(numberOfUncompressedBytes_));
  } else {
    appendBigEndian(out, std::get<Adler32>(checksum_).value_);
  }
  return out;
}

namespace {
// Cut the `range` into `UncompressedBlock`s of at least
// `MIN_NUMBER_OF_BYTES_PER_COMPRESSION_BLOCK` bytes (except the last one),
// each with the last `NUMBER_OF_BYTES_IN_DICTIONARY` bytes of the previous
// block as its dictionary.
cppcoro::generator<UncompressedBlock> cutIntoBlocks(
    cppcoro::generator<std::string> range) {
  std::string dictionary;
  std::string bytes;
  auto makeBlock = [&dictionary, &bytes]() {
    // Each block that has a successor has at least
    // `NUMBER_OF_BYTES_IN_DICTIONARY` bytes, so the dictionary of the next
    // block is the end of this one.
    std::string nextDictionary{
        bytes.end() - static_cast<ptrdiff_t>(std::min(
                          bytes.size(), NUMBER_OF_BYTES_IN_DICTIONARY)),
        bytes.end()};
    UncompressedBlock block{std::move(bytes), std::move(dictionary)};
    dictionary = std::move(nextDictionary);
    bytes = std::string{};
    return block;
  };
  for (std::string& piece : range) {
    // Typically, a piece is a whole block, which is then not copied.
    if (bytes.empty()) {
      bytes = std::move(piece);
    } else {
      bytes.append(piece);
    }
    if (bytes.size() >= MIN_NUMBER_OF_BYTES_PER_COMPRESSION_BLOCK) {
      co_yield makeBlock();
    }
  }
  if (!bytes.empty()) {
    co_yield makeBlock();
  }
}
}  // namespace
}  // namespace detail::parallelCompression

// _____________________________________________________________________________
cppcoro::generator<std::string> compressStreamInParallel(
    cppcoro::generator<std::string> range, CompressionMethod compressionMethod,
    ql::any_io_executor executor, size_t maxNumberOfBlocksInFlight) {
  using namespace detail::parallelCompression;
  AD_CONTRACT_CHECK(maxNumberOfBlocksInFlight >= 1);
  if (compressionMethod == CompressionMethod::NONE) {
    for (std::string& piece : range) {
      co_yield piece;
    }
    co_return;
  }
  const StreamFormat format = compressionMethod == CompressionMethod::GZIP
                                  ? StreamFormat::Gzip
                                  : StreamFormat::Zlib;
  StreamFrame frame{format};
  co_yield frame.header();
  // When this generator is destroyed early (for example, after a client
  // disconnect), the view waits for the blocks in flight.
  ad_utility::AsyncTransformView compressedBlocks{
      cutIntoBlocks(std::move(range)),
      [format](const UncompressedBlock& block) {
        return compressBlock(block, format);
      },
      maxNumberOfBlocksInFlight, std::move(executor)};
  for (CompressedBlock& block : compressedBlocks) {
    frame.add(block);
    co_yield block.bytes_;
  }
  co_yield frame.finalBlockAndTrailer();
}
}  // namespace ad_utility::streams

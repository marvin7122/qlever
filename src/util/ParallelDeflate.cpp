// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ParallelDeflate.h"

#include <zlib.h>

#include <algorithm>

#include "util/Exception.h"

namespace ad_utility::streams {

namespace {

// zlib counts in `uInt`; feed and drain in pieces of at most this size.
constexpr size_t kMaxZlibChunk = size_t{1} << 30;

bool isCompressing(CompressionMethod method) {
  return method == CompressionMethod::DEFLATE ||
         method == CompressionMethod::GZIP;
}

// The checksum of the empty input.
uint32_t initialCheck(CompressionMethod method) {
  return method == CompressionMethod::GZIP
             ? static_cast<uint32_t>(crc32_z(0, Z_NULL, 0))
             : static_cast<uint32_t>(adler32_z(0, Z_NULL, 0));
}

// The checksum of `input` (the `_z` variants take a `size_t` length).
uint32_t checksum(CompressionMethod method, std::string_view input) {
  uLong check = initialCheck(method);
  const auto* data = reinterpret_cast<const Bytef*>(input.data());
  if (method == CompressionMethod::GZIP) {
    check = crc32_z(check, data, input.size());
  } else {
    check = adler32_z(check, data, input.size());
  }
  return static_cast<uint32_t>(check);
}

// Owns a raw deflate `z_stream` and ends it on every exit path.
class RawDeflater {
 public:
  explicit RawDeflater(int level) {
    // windowBits -15: raw deflate (no zlib/gzip framing), 32 KiB window.
    const int ret = deflateInit2(&stream_, level, Z_DEFLATED, -15,
                                 /*memLevel=*/8, Z_DEFAULT_STRATEGY);
    AD_CORRECTNESS_CHECK(ret == Z_OK, "deflateInit2 failed");
  }
  ~RawDeflater() { deflateEnd(&stream_); }
  RawDeflater(const RawDeflater&) = delete;
  RawDeflater& operator=(const RawDeflater&) = delete;

  // Compress all of `input`, ending with a sync flush (byte-aligned, not
  // final).
  std::string compress(std::string_view input) {
    std::string out;
    out.resize(deflateBound(&stream_, input.size()) + 16);
    size_t consumed = 0;
    while (true) {
      const size_t piece = std::min(input.size() - consumed, kMaxZlibChunk);
      stream_.next_in =
          reinterpret_cast<Bytef*>(const_cast<char*>(input.data() + consumed));
      stream_.avail_in = static_cast<uInt>(piece);
      consumed += piece;
      const bool last = consumed == input.size();
      // With `Z_SYNC_FLUSH` the flush is complete once `deflate` returns with
      // output space left; with `Z_NO_FLUSH` all input is consumed then.
      drain(out, last ? Z_SYNC_FLUSH : Z_NO_FLUSH);
      if (last) {
        break;
      }
    }
    AD_CORRECTNESS_CHECK(stream_.avail_in == 0);
    out.resize(stream_.total_out);
    return out;
  }

 private:
  void drain(std::string& out, int flush) {
    do {
      if (stream_.total_out == out.size()) {
        out.resize(std::max<size_t>(2 * out.size(), 64));
      }
      stream_.next_out = reinterpret_cast<Bytef*>(out.data()) +
                         static_cast<size_t>(stream_.total_out);
      stream_.avail_out = static_cast<uInt>(
          std::min(out.size() - stream_.total_out, kMaxZlibChunk));
      const int ret = deflate(&stream_, flush);
      // `Z_BUF_ERROR` only means "no progress possible", which the loop
      // condition handles.
      AD_CORRECTNESS_CHECK(ret == Z_OK || ret == Z_BUF_ERROR, "deflate failed");
    } while (stream_.avail_out == 0);
  }

  z_stream stream_{};
};

void appendBigEndian32(std::string& out, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xFF));
  }
}

void appendLittleEndian32(std::string& out, uint32_t value) {
  for (int shift = 0; shift <= 24; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xFF));
  }
}

}  // namespace

// _____________________________________________________________________________
DeflateBlock compressDeflateBlock(std::string_view input,
                                  CompressionMethod method, int level) {
  AD_CONTRACT_CHECK(isCompressing(method));
  DeflateBlock block;
  block.check_ = checksum(method, input);
  block.uncompressedSize_ = input.size();
  if (!input.empty()) {
    block.compressed_ = RawDeflater{level}.compress(input);
  }
  return block;
}

// _____________________________________________________________________________
ParallelDeflateStream::ParallelDeflateStream(CompressionMethod method)
    : method_{method}, check_{initialCheck(method)} {
  AD_CONTRACT_CHECK(isCompressing(method));
}

// _____________________________________________________________________________
std::string ParallelDeflateStream::header() const {
  if (method_ == CompressionMethod::GZIP) {
    // ID1 ID2 CM=deflate FLG=0 MTIME=0 XFL=4 (fastest) OS=3 (Unix).
    return std::string{"\x1f\x8b\x08\x00\x00\x00\x00\x00\x04\x03", 10};
  }
  // CMF: deflate with a 32 KiB window. FLG: FLEVEL 0 (fastest), no
  // dictionary, FCHECK such that (CMF * 256 + FLG) % 31 == 0.
  return std::string{"\x78\x01", 2};
}

// _____________________________________________________________________________
void ParallelDeflateStream::append(const DeflateBlock& block) {
  if (block.empty()) {
    return;
  }
  const auto len2 = static_cast<z_off_t>(block.uncompressedSize_);
  check_ =
      static_cast<uint32_t>(method_ == CompressionMethod::GZIP
                                ? crc32_combine(check_, block.check_, len2)
                                : adler32_combine(check_, block.check_, len2));
  uncompressedSize_ += block.uncompressedSize_;
}

// _____________________________________________________________________________
std::string ParallelDeflateStream::trailer() const {
  // A final (BFINAL=1) empty block with fixed Huffman codes. Every appended
  // block ends byte-aligned, so these two bytes start on a byte boundary.
  std::string out{"\x03\x00", 2};
  if (method_ == CompressionMethod::GZIP) {
    appendLittleEndian32(out, check_);
    // ISIZE: the uncompressed size modulo 2^32.
    appendLittleEndian32(out, static_cast<uint32_t>(uncompressedSize_));
  } else {
    appendBigEndian32(out, check_);
  }
  return out;
}

}  // namespace ad_utility::streams

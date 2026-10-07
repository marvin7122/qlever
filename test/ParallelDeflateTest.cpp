// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <zlib.h>

#include <random>
#include <string>
#include <thread>
#include <vector>

#include "util/ParallelDeflate.h"

using ad_utility::content_encoding::CompressionMethod;
using ad_utility::streams::compressDeflateBlock;
using ad_utility::streams::DeflateBlock;
using ad_utility::streams::ParallelDeflateStream;

namespace {

// Inflate `compressed` with zlib's `inflate` and the given `windowBits`
// (15 = zlib, 31 = gzip, 47 = auto-detect zlib or gzip). Expects exactly one
// complete stream: `Z_STREAM_END` with no trailing bytes.
std::string inflateAll(std::string_view compressed, int windowBits) {
  z_stream stream{};
  EXPECT_EQ(inflateInit2(&stream, windowBits), Z_OK);
  stream.next_in =
      reinterpret_cast<Bytef*>(const_cast<char*>(compressed.data()));
  stream.avail_in = static_cast<uInt>(compressed.size());
  std::string out;
  int ret = Z_OK;
  while (ret == Z_OK) {
    char buffer[16384];
    stream.next_out = reinterpret_cast<Bytef*>(buffer);
    stream.avail_out = sizeof(buffer);
    ret = inflate(&stream, Z_NO_FLUSH);
    out.append(buffer, sizeof(buffer) - stream.avail_out);
  }
  EXPECT_EQ(ret, Z_STREAM_END) << "windowBits " << windowBits;
  EXPECT_EQ(stream.avail_in, 0u) << "trailing bytes after the stream";
  inflateEnd(&stream);
  return out;
}

// Compress every part as its own block and frame them as one stream.
std::string compressParts(const std::vector<std::string>& parts,
                          CompressionMethod method) {
  ParallelDeflateStream stream{method};
  std::string out = stream.header();
  for (const auto& part : parts) {
    auto block = compressDeflateBlock(part, method);
    stream.append(block);
    out += block.compressed_;
  }
  out += stream.trailer();
  return out;
}

std::string concat(const std::vector<std::string>& parts) {
  std::string out;
  for (const auto& part : parts) {
    out += part;
  }
  return out;
}

// Pseudo-random CSV-like text: compressible, but not trivially.
std::string randomText(std::mt19937& rng, size_t size) {
  static constexpr std::string_view alphabet =
      "abcdefghij,\n\"0123456789 <http://www.wikidata.org/entity/Q>@en";
  std::string out(size, ' ');
  for (auto& c : out) {
    c = alphabet[rng() % alphabet.size()];
  }
  return out;
}

// The windowBits values under which a stream of `method` must inflate.
std::vector<int> windowBitsFor(CompressionMethod method) {
  return method == CompressionMethod::GZIP ? std::vector<int>{31, 47}
                                           : std::vector<int>{15, 47};
}

class ParallelDeflateTest : public ::testing::TestWithParam<CompressionMethod> {
 protected:
  void expectRoundTrip(const std::vector<std::string>& parts) {
    const auto compressed = compressParts(parts, GetParam());
    const auto expected = concat(parts);
    for (int windowBits : windowBitsFor(GetParam())) {
      EXPECT_EQ(inflateAll(compressed, windowBits), expected)
          << "windowBits " << windowBits;
    }
  }
};

}  // namespace

// _____________________________________________________________________________
TEST_P(ParallelDeflateTest, ManyBlocksRoundTrip) {
  std::mt19937 rng{42};
  std::vector<std::string> parts;
  for (size_t i = 0; i < 64; ++i) {
    parts.push_back(randomText(rng, rng() % 100'000));
  }
  expectRoundTrip(parts);
}

// _____________________________________________________________________________
TEST_P(ParallelDeflateTest, EmptyResult) {
  expectRoundTrip({});
  // The empty stream is just header, final empty block, and trailer.
  const auto compressed = compressParts({}, GetParam());
  ParallelDeflateStream stream{GetParam()};
  EXPECT_EQ(compressed, stream.header() + stream.trailer());
}

// _____________________________________________________________________________
TEST_P(ParallelDeflateTest, OneBlock) { expectRoundTrip({"?x,?y\na,b\n"}); }

// _____________________________________________________________________________
TEST_P(ParallelDeflateTest, ZeroByteBlocks) {
  auto empty = compressDeflateBlock("", GetParam());
  EXPECT_TRUE(empty.empty());
  EXPECT_TRUE(empty.compressed_.empty());
  expectRoundTrip({"", "x\n", "", "", "y\n", ""});
  expectRoundTrip({"", "", ""});
}

// _____________________________________________________________________________
TEST_P(ParallelDeflateTest, LargeIncompressibleBlock) {
  // Random bytes do not compress, so the output buffer must grow past the
  // initial `deflateBound` estimate path without corrupting the stream.
  std::mt19937 rng{7};
  std::string bytes(3'000'000, '\0');
  for (auto& c : bytes) {
    c = static_cast<char>(rng());
  }
  expectRoundTrip({bytes, "tail\n"});
}

// _____________________________________________________________________________
TEST_P(ParallelDeflateTest, ChecksumAndSizeMatchSerialComputation) {
  std::mt19937 rng{3};
  std::vector<std::string> parts;
  for (size_t i = 0; i < 10; ++i) {
    parts.push_back(randomText(rng, 1000 + i));
  }
  ParallelDeflateStream stream{GetParam()};
  for (const auto& part : parts) {
    stream.append(compressDeflateBlock(part, GetParam()));
  }
  const auto all = concat(parts);
  EXPECT_EQ(stream.uncompressedSize(), all.size());
  const auto* data = reinterpret_cast<const Bytef*>(all.data());
  const auto trailer = stream.trailer();
  ASSERT_GE(trailer.size(), 6u);
  auto byteAt = [&](size_t i) {
    return static_cast<uint32_t>(static_cast<unsigned char>(trailer[i]));
  };
  if (GetParam() == CompressionMethod::GZIP) {
    ASSERT_EQ(trailer.size(), 10u);
    const uint32_t crc =
        byteAt(2) | byteAt(3) << 8 | byteAt(4) << 16 | byteAt(5) << 24;
    const uint32_t isize =
        byteAt(6) | byteAt(7) << 8 | byteAt(8) << 16 | byteAt(9) << 24;
    EXPECT_EQ(crc, crc32(0, data, all.size()));
    EXPECT_EQ(isize, all.size());
  } else {
    ASSERT_EQ(trailer.size(), 6u);
    const uint32_t adler =
        byteAt(2) << 24 | byteAt(3) << 16 | byteAt(4) << 8 | byteAt(5);
    EXPECT_EQ(adler, adler32(1, data, all.size()));
  }
}

// _____________________________________________________________________________
TEST_P(ParallelDeflateTest, BlocksCompressedOnThreads) {
  // The blocks are compressed concurrently; only the framing is sequential.
  std::mt19937 rng{11};
  std::vector<std::string> parts;
  for (size_t i = 0; i < 16; ++i) {
    parts.push_back(randomText(rng, 50'000 + 1000 * i));
  }
  std::vector<DeflateBlock> blocks(parts.size());
  {
    std::vector<std::jthread> threads;
    for (size_t i = 0; i < parts.size(); ++i) {
      threads.emplace_back(
          [&, i]() { blocks[i] = compressDeflateBlock(parts[i], GetParam()); });
    }
  }
  // Emit in reverse ("completion") order: any order works as long as the
  // emitted bytes and the checksum follow the same order.
  ParallelDeflateStream stream{GetParam()};
  std::string compressed = stream.header();
  std::string expected;
  for (size_t i = parts.size(); i-- > 0;) {
    stream.append(blocks[i]);
    compressed += blocks[i].compressed_;
    expected += parts[i];
  }
  compressed += stream.trailer();
  for (int windowBits : windowBitsFor(GetParam())) {
    EXPECT_EQ(inflateAll(compressed, windowBits), expected);
  }
}

// _____________________________________________________________________________
TEST(ParallelDeflate, HeadersAreTheStandardOnes) {
  ParallelDeflateStream deflate{CompressionMethod::DEFLATE};
  EXPECT_EQ(deflate.header(), std::string("\x78\x01", 2));
  ParallelDeflateStream gzip{CompressionMethod::GZIP};
  const auto header = gzip.header();
  ASSERT_EQ(header.size(), 10u);
  EXPECT_EQ(header.substr(0, 3), std::string("\x1f\x8b\x08", 3));
}

// _____________________________________________________________________________
TEST(ParallelDeflate, NoneIsAContractViolation) {
  EXPECT_ANY_THROW(ParallelDeflateStream{CompressionMethod::NONE});
  EXPECT_ANY_THROW(compressDeflateBlock("x", CompressionMethod::NONE));
}

INSTANTIATE_TEST_SUITE_P(CompressionMethods, ParallelDeflateTest,
                         ::testing::Values(CompressionMethod::DEFLATE,
                                           CompressionMethod::GZIP));

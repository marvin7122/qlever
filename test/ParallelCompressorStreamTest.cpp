// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <zlib.h>

#include <boost/asio/thread_pool.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "util/ParallelCompressorStream.h"

namespace {
using ad_utility::content_encoding::CompressionMethod;
using ad_utility::streams::compressStreamInParallel;
using ad_utility::streams::MIN_NUMBER_OF_BYTES_PER_COMPRESSION_BLOCK;
namespace pc = ad_utility::streams::detail::parallelCompression;
using pc::NUMBER_OF_BYTES_IN_DICTIONARY;
constexpr size_t minBlock = MIN_NUMBER_OF_BYTES_PER_COMPRESSION_BLOCK;

// Yield the `data` in pieces of `pieceSize` bytes (the last one may be
// shorter). Nothing is yielded for empty `data`.
cppcoro::generator<std::string> inPieces(std::string data, size_t pieceSize) {
  for (size_t i = 0; i < data.size(); i += pieceSize) {
    co_yield data.substr(i, pieceSize);
  }
}

// Yield the `pieces` as they are (including empty ones).
cppcoro::generator<std::string> yieldAll(std::vector<std::string> pieces) {
  for (auto& piece : pieces) {
    co_yield piece;
  }
}

// `numberOfBytes` of text that looks like a CSV export (random words from a
// small vocabulary, separators and newlines), so that it compresses well and
// has many references back into earlier input.
std::string csvLikeText(size_t numberOfBytes, unsigned seed = 42) {
  static const std::vector<std::string> words{
      "\"Berlin\"",
      "<http://www.wikidata.org/entity/Q64>",
      "\"Freiburg\"@de",
      "42",
      "\"3.14\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
      "_:b17",
      "\"\"",
      "\"Bad \"\"quoted\"\" text\""};
  std::mt19937 random{seed};
  std::uniform_int_distribution<size_t> word{0, words.size() - 1};
  std::uniform_int_distribution<int> separator{0, 3};
  std::string text;
  while (text.size() < numberOfBytes) {
    text += words[word(random)];
    text += separator(random) == 0 ? '\n' : ',';
  }
  text.resize(numberOfBytes);
  return text;
}

// `numberOfBytes` random bytes, which do not compress.
std::string randomBytes(size_t numberOfBytes, unsigned seed = 7) {
  std::mt19937 random{seed};
  std::uniform_int_distribution<int> byte{0, 255};
  std::string bytes(numberOfBytes, '\0');
  for (char& c : bytes) {
    c = static_cast<char>(byte(random));
  }
  return bytes;
}

std::string concatenate(cppcoro::generator<std::string> generator) {
  std::string result;
  for (const std::string& piece : generator) {
    result += piece;
  }
  return result;
}

// Decompress the `compressed` stream with zlib, which also checks the
// trailer (the checksum and, for gzip, the size), as `gzip -t` does. Only one
// gzip member (`windowBits` 15 + 16) or one zlib stream (`windowBits` 15) is
// accepted, and the stream has to end exactly at the end of the `compressed`
// bytes. Return the decompressed bytes.
std::string inflateStrictly(std::string_view compressed,
                            CompressionMethod method) {
  z_stream stream{};
  int windowBits = method == CompressionMethod::GZIP ? 15 + 16 : 15;
  EXPECT_EQ(inflateInit2(&stream, windowBits), Z_OK);
  stream.next_in =
      reinterpret_cast<Bytef*>(const_cast<char*>(compressed.data()));
  stream.avail_in = static_cast<uInt>(compressed.size());
  std::string result;
  int status = Z_OK;
  while (status == Z_OK) {
    result.resize(result.size() + 1024 * 1024);
    stream.next_out =
        reinterpret_cast<Bytef*>(result.data() + stream.total_out);
    stream.avail_out = static_cast<uInt>(result.size() - stream.total_out);
    status = inflate(&stream, Z_NO_FLUSH);
  }
  EXPECT_EQ(status, Z_STREAM_END)
      << (stream.msg != nullptr ? stream.msg : "no zlib message");
  EXPECT_EQ(stream.avail_in, 0u) << "trailing bytes after the stream";
  result.resize(stream.total_out);
  inflateEnd(&stream);
  return result;
}

// Decompress with Boost.Iostreams, as `CompressorStreamTest` does.
std::string decompressWithBoost(std::string_view compressed,
                                CompressionMethod method) {
  namespace io = boost::iostreams;
  std::string result;
  {
    io::filtering_ostream filterStream;
    if (method == CompressionMethod::GZIP) {
      filterStream.push(io::gzip_decompressor());
    } else {
      filterStream.push(io::zlib_decompressor());
    }
    filterStream.push(io::back_inserter(result));
    filterStream.write(compressed.data(),
                       static_cast<std::streamsize>(compressed.size()));
  }
  return result;
}

class ParallelCompressorStream
    : public ::testing::TestWithParam<CompressionMethod> {
 protected:
  boost::asio::thread_pool pool_{4};

  // Compress the `input` on the `pool_`, and check that both decoders give
  // the `expected` bytes. Return the compressed bytes.
  std::string compressAndCheck(cppcoro::generator<std::string> input,
                               const std::string& expected,
                               size_t maxNumberOfBlocksInFlight = 8) {
    auto compressed = concatenate(compressStreamInParallel(
        std::move(input), GetParam(), pool_.get_executor(),
        maxNumberOfBlocksInFlight));
    EXPECT_EQ(inflateStrictly(compressed, GetParam()), expected);
    EXPECT_EQ(decompressWithBoost(compressed, GetParam()), expected);
    return compressed;
  }
};

// _____________________________________________________________________________
TEST_P(ParallelCompressorStream, emptyInput) {
  compressAndCheck(yieldAll({}), "");
  compressAndCheck(yieldAll({"", "", ""}), "");
}

// _____________________________________________________________________________
TEST_P(ParallelCompressorStream, oneByte) {
  compressAndCheck(yieldAll({"x"}), "x");
  compressAndCheck(yieldAll({"", "x", ""}), "x");
}

// _____________________________________________________________________________
TEST_P(ParallelCompressorStream, sizesAroundTheBlockSize) {
  for (size_t size :
       {NUMBER_OF_BYTES_IN_DICTIONARY - 1, NUMBER_OF_BYTES_IN_DICTIONARY,
        minBlock - 1, minBlock, minBlock + 1, 2 * minBlock - 1, 2 * minBlock,
        2 * minBlock + 1, 5 * minBlock + 17}) {
    auto text = csvLikeText(size);
    // One piece, pieces of exactly one block, pieces that do not divide the
    // block size, and many small pieces.
    for (size_t pieceSize :
         {size, minBlock, minBlock - 1, minBlock + 1, size_t{1000}}) {
      compressAndCheck(inPieces(text, pieceSize), text);
    }
  }
}

// _____________________________________________________________________________
TEST_P(ParallelCompressorStream, randomBytesDoNotCompress) {
  auto bytes = randomBytes(3 * minBlock + 5);
  compressAndCheck(inPieces(bytes, 1 << 20), bytes);
  compressAndCheck(inPieces(bytes, 4096), bytes);
}

// _____________________________________________________________________________
TEST_P(ParallelCompressorStream, large) {
  // 16 MiB in the 1 MiB pieces of an export, on 4 threads.
  auto text = csvLikeText(16 << 20);
  for (size_t maxNumberOfBlocksInFlight : {1, 2, 8}) {
    compressAndCheck(inPieces(text, 1 << 20), text, maxNumberOfBlocksInFlight);
  }
  // Pieces that are much larger than a block.
  compressAndCheck(inPieces(text, 10 << 20), text);
}

// The blocks refer back into the previous block through their dictionary: for
// a random string of 16 KiB that is repeated, only the first block contains a
// copy of the string; every other block consists of references back into the
// input before it. Without the dictionary, each of the 10 blocks would contain
// one full copy. (The string is shorter than the window, because deflate finds
// no match at a distance of almost 32 KiB.)
TEST_P(ParallelCompressorStream, blocksUseTheDictionary) {
  auto unit = randomBytes(NUMBER_OF_BYTES_IN_DICTIONARY / 2);
  std::string text;
  while (text.size() < 10 * minBlock) {
    text += unit;
  }
  auto compressed = compressAndCheck(inPieces(text, minBlock), text);
  // The same blocks, each compressed without a dictionary.
  size_t numberOfBytesWithoutDictionary = 0;
  auto format = GetParam() == CompressionMethod::GZIP ? pc::StreamFormat::Gzip
                                                      : pc::StreamFormat::Zlib;
  for (size_t i = 0; i < text.size(); i += minBlock) {
    numberOfBytesWithoutDictionary +=
        pc::compressBlock({text.substr(i, minBlock), std::string{}}, format)
            .bytes_.size();
  }
  EXPECT_GT(numberOfBytesWithoutDictionary, 10 * unit.size());
  EXPECT_LT(3 * compressed.size(), numberOfBytesWithoutDictionary);
}

// _____________________________________________________________________________
TEST_P(ParallelCompressorStream, noneIsTheIdentity) {
  auto text = csvLikeText(3 * minBlock);
  auto output = concatenate(compressStreamInParallel(
      inPieces(text, 1000), CompressionMethod::NONE, pool_.get_executor(), 2));
  EXPECT_EQ(output, text);
}

// An exception from the input (in the server, for example, the cancellation
// of the query) reaches the consumer, also while blocks are in flight.
TEST_P(ParallelCompressorStream, exceptionInTheInputIsPropagated) {
  auto throwing = []() -> cppcoro::generator<std::string> {
    for (size_t i = 0; i < 10; ++i) {
      co_yield csvLikeText(minBlock);
    }
    throw std::runtime_error{"Operation was manually cancelled."};
  };
  auto generator =
      compressStreamInParallel(throwing(), GetParam(), pool_.get_executor(), 4);
  EXPECT_THROW(concatenate(std::move(generator)), std::runtime_error);
}

// An endless input of 1 MiB pieces, which counts the pieces that are read.
cppcoro::generator<std::string> endlessInput(size_t& numberOfPiecesRead) {
  while (true) {
    ++numberOfPiecesRead;
    co_yield csvLikeText(1 << 20);
  }
}

// The generator can be destroyed while blocks are in flight (in the server,
// after a client disconnect), and it reads only a bounded number of pieces
// ahead of its consumer.
TEST_P(ParallelCompressorStream, earlyDestruction) {
  size_t numberOfPiecesRead = 0;
  {
    auto generator = compressStreamInParallel(
        endlessInput(numberOfPiecesRead), GetParam(), pool_.get_executor(), 4);
    // The header, and then two blocks.
    auto it = generator.begin();
    for (size_t i = 0; i < 2; ++i) {
      ASSERT_NE(it, generator.end());
      ++it;
    }
    ASSERT_NE(it, generator.end());
  }
  // The two yielded blocks and the (at most) four in flight, one piece each.
  EXPECT_LE(numberOfPiecesRead, 2u + 4u);
  EXPECT_GE(numberOfPiecesRead, 2u);
}

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(DeflateAndGzip, ParallelCompressorStream,
                         ::testing::Values(CompressionMethod::DEFLATE,
                                           CompressionMethod::GZIP));

// The stream around the blocks: the header and trailer of an empty stream are
// exactly what zlib writes for empty input at level 1 (except for the OS field
// of the gzip header, which zlib sets to the actual system).
TEST(ParallelCompressorStreamFrame, emptyStream) {
  using namespace std::string_literals;
  pc::StreamFrame zlib{pc::StreamFormat::Zlib};
  EXPECT_EQ(zlib.header() + zlib.finalBlockAndTrailer(),
            "\x78\x01\x03\x00\x00\x00\x00\x01"s);
  pc::StreamFrame gzip{pc::StreamFormat::Gzip};
  EXPECT_EQ(gzip.header() + gzip.finalBlockAndTrailer(),
            "\x1f\x8b\x08\x00\x00\x00\x00\x00\x04\xff"
            "\x03\x00"
            "\x00\x00\x00\x00\x00\x00\x00\x00"s);
}

// The combined checksum and the size in the trailer are those of the whole
// input.
TEST(ParallelCompressorStreamFrame, trailerOfSeveralBlocks) {
  auto text = csvLikeText(3 * minBlock + 123);
  for (auto format : {pc::StreamFormat::Zlib, pc::StreamFormat::Gzip}) {
    pc::StreamFrame frame{format};
    std::string_view rest = text;
    for (size_t blockSize : {minBlock, size_t{1}, 2 * minBlock}) {
      frame.add(pc::compressBlock(
          {std::string{rest.substr(0, blockSize)}, std::string{}}, format));
      rest.remove_prefix(blockSize);
    }
    frame.add(pc::compressBlock({std::string{rest}, std::string{}}, format));
    auto trailer = frame.finalBlockAndTrailer();
    const auto* data = reinterpret_cast<const Bytef*>(text.data());
    auto byte = [&trailer](size_t i) {
      return static_cast<uint32_t>(static_cast<unsigned char>(trailer[i]));
    };
    if (format == pc::StreamFormat::Gzip) {
      ASSERT_EQ(trailer.size(), 2u + 8u);
      uint32_t crc = byte(2) | byte(3) << 8 | byte(4) << 16 | byte(5) << 24;
      uint32_t size = byte(6) | byte(7) << 8 | byte(8) << 16 | byte(9) << 24;
      EXPECT_EQ(crc, crc32(0, data, static_cast<uInt>(text.size())));
      EXPECT_EQ(size, text.size());
    } else {
      ASSERT_EQ(trailer.size(), 2u + 4u);
      uint32_t adler = byte(2) << 24 | byte(3) << 16 | byte(4) << 8 | byte(5);
      EXPECT_EQ(adler, adler32(1, data, static_cast<uInt>(text.size())));
    }
  }
}

// The size in the gzip trailer is the number of uncompressed bytes modulo
// 2^32, and the full sizes of the blocks enter the combination of the
// checksums. The blocks are made up, as no test should compress 4 GiB.
TEST(ParallelCompressorStreamFrame, sizeModulo2To32) {
  auto byte = [](const std::string& trailer, size_t i) {
    return static_cast<uint32_t>(static_cast<unsigned char>(trailer[i]));
  };
  constexpr uint64_t numberOfBytesPerBlock = (uint64_t{1} << 31) + 3;
  constexpr uint32_t blockChecksum = 0x12345678;
  auto length = static_cast<z_off_t>(numberOfBytesPerBlock);
  for (auto format : {pc::StreamFormat::Zlib, pc::StreamFormat::Gzip}) {
    bool isGzip = format == pc::StreamFormat::Gzip;
    pc::StreamFrame frame{format};
    // Two blocks of 2^31 + 3 bytes, so 2^32 + 6 bytes in total.
    for (size_t i = 0; i < 2; ++i) {
      frame.add({std::string{},
                 isGzip ? pc::Checksum{pc::Crc32{blockChecksum}}
                        : pc::Checksum{pc::Adler32{blockChecksum}},
                 numberOfBytesPerBlock});
    }
    auto trailer = frame.finalBlockAndTrailer();
    if (isGzip) {
      auto expected = crc32_combine(crc32_combine(0, blockChecksum, length),
                                    blockChecksum, length);
      ASSERT_EQ(trailer.size(), 2u + 8u);
      EXPECT_EQ(byte(trailer, 2) | byte(trailer, 3) << 8 |
                    byte(trailer, 4) << 16 | byte(trailer, 5) << 24,
                expected);
      EXPECT_EQ(byte(trailer, 6) | byte(trailer, 7) << 8 |
                    byte(trailer, 8) << 16 | byte(trailer, 9) << 24,
                6u);
    } else {
      auto expected = adler32_combine(adler32_combine(1, blockChecksum, length),
                                      blockChecksum, length);
      ASSERT_EQ(trailer.size(), 2u + 4u);
      EXPECT_EQ(byte(trailer, 2) << 24 | byte(trailer, 3) << 16 |
                    byte(trailer, 4) << 8 | byte(trailer, 5),
                expected);
    }
  }
}

// A block that was compressed for one format cannot be added to a stream of
// the other format.
TEST(ParallelCompressorStreamFrame, formatsCannotBeMixed) {
  pc::StreamFrame zlib{pc::StreamFormat::Zlib};
  EXPECT_ANY_THROW(zlib.add(
      pc::compressBlock({"abc", std::string{}}, pc::StreamFormat::Gzip)));
}

// A compressed block is raw deflate data that ends with a sync flush (the
// empty stored block `00 00 FF FF`), so it is byte-aligned and not final.
TEST(ParallelCompressorStreamFrame, blockEndsWithSyncFlush) {
  for (auto format : {pc::StreamFormat::Zlib, pc::StreamFormat::Gzip}) {
    auto block = pc::compressBlock({csvLikeText(1000), std::string{}}, format);
    EXPECT_THAT(block.bytes_,
                ::testing::EndsWith(std::string{"\0\0\xff\xff", 4}));
    EXPECT_EQ(block.numberOfUncompressedBytes_, 1000u);
  }
}
}  // namespace

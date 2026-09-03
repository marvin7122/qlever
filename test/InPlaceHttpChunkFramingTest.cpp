// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>
#include <unistd.h>

#include <charconv>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include "backports/span.h"
#include "engine/InPlaceHttpChunkFraming.h"
#include "util/Exception.h"
#include "util/Invariants.h"

using namespace ad_utility::http;

// Compile-time verification that the classes satisfy the invariant concept.
static_assert(ad_utility::InvariantStatefulClass<InPlaceHttpChunk>);
static_assert(ad_utility::InvariantStatefulClass<InPlaceHttpChunkStreamer>);

TEST(InPlaceHttpChunkFramingTest, ConceptComplianceAndBasicFraming) {
  InPlaceHttpChunk chunk(1024);
  EXPECT_TRUE(chunk.isOwner());
  EXPECT_EQ(chunk.maxPayloadCapacity(), 1024);
  EXPECT_FALSE(chunk.isFinalized());

  const std::string payload = "Hello, QLever Chunked HTTP Stream!";
  ASSERT_LE(payload.size(), chunk.maxPayloadCapacity());

  std::memcpy(chunk.payloadData(), payload.data(), payload.size());

  auto framedSpan = chunk.finalizeChunk(payload.size());
  EXPECT_TRUE(chunk.isFinalized());
  EXPECT_EQ(chunk.lastPayloadBytes(), payload.size());

  // Verify that the payload size is 0x22 (34 bytes).
  std::string_view framed(framedSpan.data(), framedSpan.size());
  EXPECT_EQ(framed, "22\r\nHello, QLever Chunked HTTP Stream!\r\n");

  // Zero-copy verification: the framed span starts in the header immediately before the payload.
  EXPECT_EQ(framedSpan.data(), chunk.payloadData() - 4);  // "22\r\n" = 4 bytes
  EXPECT_EQ(chunk.lastFinalizedSpan().data(), framedSpan.data());
  EXPECT_EQ(chunk.lastFinalizedSpan().size(), framedSpan.size());
}

TEST(InPlaceHttpChunkFramingTest, TerminatingChunk) {
  InPlaceHttpChunk chunk(1024);

  // Verify that a zero-payload terminating chunk is `0\r\n\r\n` (5 bytes).
  auto framedSpan = chunk.createFinalChunk();
  EXPECT_TRUE(chunk.isFinalized());
  EXPECT_EQ(chunk.lastPayloadBytes(), 0);

  std::string_view framed(framedSpan.data(), framedSpan.size());
  EXPECT_EQ(framed, "0\r\n\r\n");
  EXPECT_EQ(framedSpan.size(), 5);
}

TEST(InPlaceHttpChunkFramingTest, HexLengthFormattingExhaustive) {
  const std::vector<size_t> testSizes = {
      0, 1, 2, 9, 10, 15, 16, 31, 32, 255, 256, 1023, 1024,
      4095, 4096, 65535, 65536, 1048576, 2000000};

  for (size_t size : testSizes) {
    InPlaceHttpChunk chunk(size);
    EXPECT_EQ(chunk.maxPayloadCapacity(), size);

        for (size_t i = 0; i < size; ++i) {
      chunk.payloadData()[i] = static_cast<char>('a' + (i % 26));
    }

    auto framedSpan = chunk.finalizeChunk(size);
    std::string_view framed(framedSpan.data(), framedSpan.size());

    // Extract hex header before first \r\n
    size_t crlfPos = framed.find("\r\n");
    ASSERT_NE(crlfPos, std::string_view::npos);
    std::string_view hexHeader = framed.substr(0, crlfPos);

    size_t parsedSize = 0;
    auto [ptr, ec] = std::from_chars(hexHeader.data(),
                                     hexHeader.data() + hexHeader.size(),
                                     parsedSize, 16);
    ASSERT_EQ(ec, std::errc{});
    EXPECT_EQ(ptr, hexHeader.data() + hexHeader.size());
    EXPECT_EQ(parsedSize, size);

    // Verify payload and tail CRLF
    std::string_view extractedPayload = framed.substr(crlfPos + 2, size);
    for (size_t i = 0; i < size; ++i) {
      EXPECT_EQ(extractedPayload[i], static_cast<char>('a' + (i % 26)));
    }

    std::string_view tailCrlf = framed.substr(crlfPos + 2 + size);
    EXPECT_EQ(tailCrlf, "\r\n");
    EXPECT_EQ(framed.size(), hexHeader.size() + 2 + size + 2);
  }
}

TEST(InPlaceHttpChunkFramingTest, NonOwningExternalBuffer) {
  constexpr size_t payloadCapacity = 512;
  constexpr size_t totalBufferBytes =
      InPlaceHttpChunk::TOTAL_OVERHEAD_BYTES + payloadCapacity;

  std::vector<char> externalStorage(totalBufferBytes);
  InPlaceHttpChunk chunk(ql::span<char>(externalStorage.data(), externalStorage.size()));

  EXPECT_FALSE(chunk.isOwner());
  EXPECT_EQ(chunk.maxPayloadCapacity(), payloadCapacity);
  EXPECT_EQ(chunk.totalCapacity(), totalBufferBytes);

  std::string testData = "Zero-copy HTTP framing using external memory!";
  std::memcpy(chunk.payloadData(), testData.data(), testData.size());

  auto framedSpan = chunk.finalizeChunk(testData.size());
  std::string_view framed(framedSpan.data(), framedSpan.size());

  // 45 bytes = 0x2d
  EXPECT_EQ(framed, "2d\r\nZero-copy HTTP framing using external memory!\r\n");
}

TEST(InPlaceHttpChunkFramingTest, StreamerAutoChunkingAndFlush) {
  std::vector<std::string> emittedChunks;
  constexpr size_t chunkPayloadCap = 100;

  InPlaceHttpChunkStreamer streamer(
      [&](ql::span<const char> chunk) {
        emittedChunks.emplace_back(chunk.data(), chunk.size());
      },
      chunkPayloadCap, true);

  // Write 250 bytes in pieces
  std::string part1(60, 'A');
  std::string part2(70, 'B');
  std::string part3(120, 'C');

  streamer.write(part1);
  EXPECT_EQ(streamer.currentBufferedBytes(), 60);
  EXPECT_EQ(emittedChunks.size(), 0);

  streamer.write(part2);
  // Total 130 bytes: 100 bytes emitted as first chunk, 30 bytes buffered
  EXPECT_EQ(streamer.currentBufferedBytes(), 30);
  EXPECT_EQ(emittedChunks.size(), 1);

  streamer.write(part3);
  // Total 30 + 120 = 150 bytes: 100 bytes emitted as second chunk, 50 bytes buffered
  EXPECT_EQ(streamer.currentBufferedBytes(), 50);
  EXPECT_EQ(emittedChunks.size(), 2);

  auto summary = std::move(streamer).finalize();
  // 50 bytes flushed + terminating chunk emitted -> 4 chunks total
  EXPECT_EQ(emittedChunks.size(), 4);
  EXPECT_EQ(summary.chunksEmitted_, 4);
  EXPECT_EQ(summary.totalPayloadBytes_, 250);

  // Validate chunk 1: 100 bytes ('A' * 60 + 'B' * 40)
  EXPECT_TRUE(emittedChunks[0].starts_with("64\r\n"));  // 100 = 0x64
  EXPECT_TRUE(emittedChunks[0].ends_with("\r\n"));
  EXPECT_EQ(emittedChunks[0].size(), 4 + 100 + 2);

  // Validate chunk 2: 100 bytes ('B' * 30 + 'C' * 70)
  EXPECT_TRUE(emittedChunks[1].starts_with("64\r\n"));
  EXPECT_TRUE(emittedChunks[1].ends_with("\r\n"));
  EXPECT_EQ(emittedChunks[1].size(), 4 + 100 + 2);

  // Validate chunk 3: 50 bytes ('C' * 50)
  EXPECT_TRUE(emittedChunks[2].starts_with("32\r\n"));  // 50 = 0x32
  EXPECT_TRUE(emittedChunks[2].ends_with("\r\n"));
  EXPECT_EQ(emittedChunks[2].size(), 4 + 50 + 2);

    EXPECT_EQ(emittedChunks[3], "0\r\n\r\n");

  // Reconstruct full payload
  std::string reconstructedPayload;
  for (size_t i = 0; i < 3; ++i) {
    size_t pos = emittedChunks[i].find("\r\n");
    size_t endPos = emittedChunks[i].rfind("\r\n");
    reconstructedPayload += emittedChunks[i].substr(pos + 2, endPos - (pos + 2));
  }
  EXPECT_EQ(reconstructedPayload, part1 + part2 + part3);
}

TEST(InPlaceHttpChunkFramingTest, StreamerLargeSingleWrite) {
  std::vector<std::string> emittedChunks;
  constexpr size_t chunkPayloadCap = 1000;

  InPlaceHttpChunkStreamer streamer(
      [&](ql::span<const char> chunk) {
        emittedChunks.emplace_back(chunk.data(), chunk.size());
      },
      chunkPayloadCap, true);

  std::string largeData(2500, 'X');
  streamer.write(largeData);

  auto summary = std::move(streamer).finalize();
  // Three payload chunks (1000 + 1000 + 500 bytes) plus one terminating chunk.
  EXPECT_EQ(emittedChunks.size(), 4);
  EXPECT_EQ(summary.totalPayloadBytes_, 2500);

  EXPECT_EQ(emittedChunks[3], "0\r\n\r\n");
}

TEST(InPlaceHttpChunkFramingTest, PipeTransmission) {
  int pipeFds[2];
  ASSERT_EQ(::pipe(pipeFds), 0);

  InPlaceHttpChunk chunk(2048);
  std::string payload = "Testing direct kernel transmission over POSIX pipe!";
  std::memcpy(chunk.payloadData(), payload.data(), payload.size());

  auto framedSpan = chunk.finalizeChunk(payload.size());

  ssize_t bytesWritten = ::write(pipeFds[1], framedSpan.data(), framedSpan.size());
  EXPECT_EQ(bytesWritten, static_cast<ssize_t>(framedSpan.size()));
  ::close(pipeFds[1]);

  std::string readBuf(framedSpan.size(), '\0');
  ssize_t bytesRead = ::read(pipeFds[0], readBuf.data(), readBuf.size());
  ::close(pipeFds[0]);

  EXPECT_EQ(bytesRead, static_cast<ssize_t>(framedSpan.size()));
  EXPECT_EQ(readBuf, std::string_view(framedSpan.data(), framedSpan.size()));
}

TEST(InPlaceHttpChunkFramingTest, ResetAndReuse) {
  InPlaceHttpChunk chunk(512);

  for (size_t iteration = 0; iteration < 10; ++iteration) {
    chunk.reset();
    EXPECT_FALSE(chunk.isFinalized());

    std::string msg = "Iteration " + std::to_string(iteration);
    std::memcpy(chunk.payloadData(), msg.data(), msg.size());

    auto framed = chunk.finalizeChunk(msg.size());
    EXPECT_TRUE(chunk.isFinalized());

    std::string_view sv(framed.data(), framed.size());
    EXPECT_TRUE(sv.find(msg) != std::string_view::npos);
    EXPECT_TRUE(sv.ends_with("\r\n"));
  }
}

TEST(InPlaceHttpChunkFramingTest, MoveSemantics) {
  InPlaceHttpChunk c1(1024);
  std::string msg = "Move test";
  std::memcpy(c1.payloadData(), msg.data(), msg.size());
  auto f1 = c1.finalizeChunk(msg.size());

  InPlaceHttpChunk c2 = std::move(c1);
  EXPECT_TRUE(c2.isFinalized());
  EXPECT_EQ(c2.framedLength(), f1.size());
  EXPECT_EQ(c2.lastPayloadBytes(), msg.size());

  InPlaceHttpChunk c3(100);
  c3 = std::move(c2);
  EXPECT_TRUE(c3.isFinalized());
  EXPECT_EQ(c3.framedLength(), f1.size());
}

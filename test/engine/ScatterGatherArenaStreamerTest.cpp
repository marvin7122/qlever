// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>
#include <unistd.h>

#include <string>
#include <string_view>
#include <vector>

#include "backports/span.h"
#include "engine/ConstructTypes.h"
#include "engine/FastExportStreamFormatter.h"
#include "engine/ScatterGatherArenaStreamer.h"
#include "global/Constants.h"

using namespace ql::export_streaming;
using ql::export_formatting::ExportFormat;
using qlever::constructExport::EvaluatedTermData;

// Zero-copy thresholds used below. Each sits below its test's payload size
// (zero-copy path), except kThresholdAboveShortLiteral which forces the
// header-copy path.
constexpr size_t kThresholdBelowLargePayload = 10;
constexpr size_t kThresholdBelowSmallPayload = 16;
constexpr size_t kThresholdBelowLargeLiteral = 32;
constexpr size_t kThresholdAboveShortLiteral = 64;
constexpr size_t kTinyMaxChunkBytes = 100;

// RAII holder for pipe file descriptors: closes both ends on scope exit,
// including ASSERT-failure paths that return from the test early.
struct ScopedPipeFds {
  int readFd = -1;
  int writeFd = -1;
  ~ScopedPipeFds() {
    if (readFd >= 0) {
      ::close(readFd);
    }
    if (writeFd >= 0) {
      ::close(writeFd);
    }
  }
};

TEST(ScatterGatherArenaStreamerTest, BasicHeaderAndSpanCoalescing) {
  ScatterGatherConfig config;
  // Below the 64-byte arena literal, so the literal takes the zero-copy path.
  config.zeroCopyThresholdBytes = kThresholdBelowLargeLiteral;

  std::vector<ScatterGatherChunk> chunks;
  ScatterGatherChunkStreamer streamer(
      [&](ScatterGatherChunk chunk) { chunks.push_back(std::move(chunk)); },
      config);

  std::string arenaLiteral =
      "This is a large literal string residing inside the memory arena.";
  ASSERT_GE(arenaLiteral.size(), 32);

  // A span deduced from a string literal includes the NUL terminator, so
  // build the IRI spans from string_views to pass exactly the IRI characters.
  constexpr std::string_view kSubject = "<http://example.org/sub>";
  constexpr std::string_view kPredicate = "<http://example.org/pred>";
  streamer.writeIri(ql::span<const char>(kSubject.data(), kSubject.size()));
  streamer.writeChar(' ');
  streamer.writeIri(ql::span<const char>(kPredicate.data(), kPredicate.size()));
  streamer.writeChar(' ');
  streamer.writeLiteral(
      ql::span<const char>(arenaLiteral.data(), arenaLiteral.size()),
      "http://www.w3.org/2001/XMLSchema#string");
  streamer.writeRawHeader(" .\n");

  auto summary = std::move(streamer).finalize();
  EXPECT_EQ(summary.chunksEmitted_, 1);
  ASSERT_EQ(chunks.size(), 1);

  const auto& chunk = chunks[0];
  EXPECT_GT(chunk.totalBytes(), 0);
  EXPECT_EQ(chunk.zeroCopySpansCount(), 1);
  EXPECT_EQ(chunk.zeroCopyBytes(), arenaLiteral.size());

  // Coalescing check:
  // 1: "<http://example.org/sub> <http://example.org/pred> \"" (header)
  // 2: arenaLiteral (zero-copy arena pointer)
  // 3: "\"^^<http://www.w3.org/2001/XMLSchema#string> .\n" (header)
  EXPECT_EQ(chunk.numSegments(), 3);

  // Verify that the second iovec points directly to the arena memory
  auto iovs = chunk.iovecs();
  EXPECT_EQ(iovs[1].iov_base, arenaLiteral.data());
  EXPECT_EQ(iovs[1].iov_len, arenaLiteral.size());

  // Verify flattened string representation
  std::string fullStr = chunk.toString();
  EXPECT_EQ(
      fullStr,
      "<http://example.org/sub> <http://example.org/pred> "
      "\"This is a large literal string residing inside the memory arena.\""
      "^^<http://www.w3.org/2001/XMLSchema#string> .\n");
}

TEST(ScatterGatherArenaStreamerTest, ShortStringsCopiedToHeader) {
  ScatterGatherConfig config;
  // Above the 5-byte literal, forcing the header-copy path.
  config.zeroCopyThresholdBytes = kThresholdAboveShortLiteral;

  std::vector<ScatterGatherChunk> chunks;
  ScatterGatherChunkStreamer streamer(
      [&](ScatterGatherChunk chunk) { chunks.push_back(std::move(chunk)); },
      config);

  std::string shortLiteral = "short";
  streamer.writeLiteral(
      ql::span<const char>(shortLiteral.data(), shortLiteral.size()));

  auto summary = std::move(streamer).finalize();
  ASSERT_EQ(chunks.size(), 1);

  const auto& chunk = chunks[0];
  EXPECT_EQ(chunk.zeroCopySpansCount(), 0);
  EXPECT_EQ(chunk.zeroCopyBytes(), 0);
  // Entire literal was coalesced into 1 contiguous header slice: "\"short\""
  EXPECT_EQ(chunk.numSegments(), 1);
  EXPECT_EQ(chunk.toString(), "\"short\"");
}

TEST(ScatterGatherArenaStreamerTest, AutoFlushOnChunkByteLimit) {
  ScatterGatherConfig config;
  config.maxChunkBytes = kTinyMaxChunkBytes;
  // Below the 36-byte payload, so payloads take the zero-copy path.
  config.zeroCopyThresholdBytes = kThresholdBelowSmallPayload;

  std::vector<ScatterGatherChunk> chunks;
  ScatterGatherChunkStreamer streamer(
      [&](ScatterGatherChunk chunk) { chunks.push_back(std::move(chunk)); },
      config);

  std::string payload = "0123456789abcdefghijklmnopqrstuvwxyz";  // 36 bytes

  for (size_t i = 0; i < 5; ++i) {
    streamer.writeArenaSpan(
        ql::span<const char>(payload.data(), payload.size()));
  }

  auto summary = std::move(streamer).finalize();
  // 5 x 36 = 180 bytes with greedy packing into 100-byte chunks: a third
  // payload would exceed the limit, so chunks hold 72 + 72 + 36 bytes.
  EXPECT_EQ(summary.chunksEmitted_, 3);
  EXPECT_EQ(chunks.size(), summary.chunksEmitted_);

  size_t totalReceivedBytes = 0;
  for (const auto& chk : chunks) {
    totalReceivedBytes += chk.totalBytes();
  }
  EXPECT_EQ(totalReceivedBytes, 5 * payload.size());
}

TEST(ScatterGatherArenaStreamerTest, WriteTripleFormats) {
  ScatterGatherConfig config;
  // Below the object literal length, so it takes the zero-copy path.
  config.zeroCopyThresholdBytes = kThresholdBelowLargePayload;

  std::string subj = "<http://subj>";
  std::string pred = "<http://pred>";
  std::string obj = "Detailed literal description exceeding 10 bytes.";

  // 1. Turtle format
  {
    ScatterGatherChunkStreamer streamer(config);
    streamer.writeTriple(ExportFormat::Turtle,
                         ql::span<const char>(subj.data(), subj.size()),
                         ql::span<const char>(pred.data(), pred.size()),
                         ql::span<const char>(obj.data(), obj.size()));
    auto chunkOpt = streamer.flush();
    ASSERT_TRUE(chunkOpt.has_value());
    EXPECT_EQ(chunkOpt->toString(),
              "<http://subj> <http://pred> \"Detailed literal description "
              "exceeding 10 bytes.\" .\n");
  }

  // 2. CSV format
  {
    ScatterGatherChunkStreamer streamer(config);
    streamer.writeTriple(ExportFormat::Csv,
                         ql::span<const char>(subj.data(), subj.size()),
                         ql::span<const char>(pred.data(), pred.size()),
                         ql::span<const char>(obj.data(), obj.size()));
    auto chunkOpt = streamer.flush();
    ASSERT_TRUE(chunkOpt.has_value());
    EXPECT_EQ(chunkOpt->toString(),
              "<http://subj>,<http://pred>,\"Detailed literal description "
              "exceeding 10 bytes.\"\n");
  }

  // 3. TSV format
  {
    ScatterGatherChunkStreamer streamer(config);
    streamer.writeTriple(ExportFormat::Tsv,
                         ql::span<const char>(subj.data(), subj.size()),
                         ql::span<const char>(pred.data(), pred.size()),
                         ql::span<const char>(obj.data(), obj.size()));
    auto chunkOpt = streamer.flush();
    ASSERT_TRUE(chunkOpt.has_value());
    EXPECT_EQ(chunkOpt->toString(),
              "<http://subj>\t<http://pred>\t\"Detailed literal description "
              "exceeding 10 bytes.\"\n");
  }
}

TEST(ScatterGatherArenaStreamerTest, EvaluatedTermDataOverload) {
  ScatterGatherConfig config;
  // Below the IRI lengths, so terms take the zero-copy path.
  config.zeroCopyThresholdBytes = kThresholdBelowLargePayload;

  EvaluatedTermData s("<http://s>", nullptr);
  EvaluatedTermData p("<http://p>", nullptr);
  EvaluatedTermData o("42", XSD_INT_TYPE);

  ScatterGatherChunkStreamer streamer(config);
  streamer.writeTriple(ExportFormat::Turtle, s, p, o);

  auto chunkOpt = streamer.flush();
  ASSERT_TRUE(chunkOpt.has_value());
  EXPECT_EQ(chunkOpt->toString(), "<http://s> <http://p> 42 .\n");
}

TEST(ScatterGatherArenaStreamerTest, WriteToPipeFd) {
  ScopedPipeFds pipe;
  int rawFds[2];
  ASSERT_EQ(::pipe(rawFds), 0);
  pipe.readFd = rawFds[0];
  pipe.writeFd = rawFds[1];

  ScatterGatherConfig config;
  // Below the payload length, so it takes the zero-copy path.
  config.zeroCopyThresholdBytes = kThresholdBelowLargePayload;

  ScatterGatherChunkStreamer streamer(config);
  std::string s = "<http://s>";
  std::string p = "<http://p>";
  std::string o = "A large payload to stream through pipe.";
  streamer.writeTriple(ExportFormat::Turtle,
                       ql::span<const char>(s.data(), s.size()),
                       ql::span<const char>(p.data(), p.size()),
                       ql::span<const char>(o.data(), o.size()));

  auto chunkOpt = streamer.flush();
  ASSERT_TRUE(chunkOpt.has_value());

  ssize_t written = chunkOpt->writeToFd(pipe.writeFd);
  EXPECT_EQ(written, static_cast<ssize_t>(chunkOpt->totalBytes()));

  std::string readBuf(chunkOpt->totalBytes(), '\0');
  ssize_t bytesRead = ::read(pipe.readFd, readBuf.data(), readBuf.size());

  EXPECT_EQ(bytesRead, static_cast<ssize_t>(chunkOpt->totalBytes()));
  EXPECT_EQ(readBuf, chunkOpt->toString());
}

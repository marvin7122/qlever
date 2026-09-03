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
#include "engine/ScatterGatherArenaStreamer.h"
#include "engine/export_prototypes/FastExportStreamFormatter.h"

using namespace ql::export_streaming;
using ql::export_formatting::ExportFormat;
using qlever::constructExport::EvaluatedTermData;

// Verify the `InvariantStatefulClass` concepts.
static_assert(ad_utility::InvariantStatefulClass<ScatterGatherChunk>);
static_assert(ad_utility::InvariantStatefulClass<ScatterGatherChunkStreamer>);

TEST(ScatterGatherArenaStreamerTest, BasicHeaderAndSpanCoalescing) {
  ScatterGatherConfig config;
  config.zeroCopyThresholdBytes = 32;

  std::vector<ScatterGatherChunk> chunks;
  ScatterGatherChunkStreamer streamer(
      [&](ScatterGatherChunk chunk) { chunks.push_back(std::move(chunk)); },
      config);

    std::string arenaLiteral = "This is a large literal string used as an external zero-copy buffer.";
  ASSERT_GE(arenaLiteral.size(), 32);

  streamer.writeIri(ql::span<const char>("<http://example.org/sub>"));
  streamer.writeChar(' ');
  streamer.writeIri(ql::span<const char>("<http://example.org/pred>"));
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

  // Verify coalescing:
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
  config.zeroCopyThresholdBytes = 64;  // High threshold

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
  config.maxChunkBytes = 100;  // Tiny chunk limit
  config.zeroCopyThresholdBytes = 16;

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
  EXPECT_GT(summary.chunksEmitted_, 1);
  EXPECT_EQ(chunks.size(), summary.chunksEmitted_);

  size_t totalReceivedBytes = 0;
  for (const auto& chk : chunks) {
    totalReceivedBytes += chk.totalBytes();
  }
  EXPECT_EQ(totalReceivedBytes, 5 * payload.size());
}

TEST(ScatterGatherArenaStreamerTest, WriteTripleFormats) {
  ScatterGatherConfig config;
  config.zeroCopyThresholdBytes = 10;

  std::string subj = "<http://subj>";
  std::string pred = "<http://pred>";
  std::string obj = "Detailed literal description exceeding 10 bytes.";

  // 1. Turtle format
  {
    ScatterGatherChunkStreamer streamer(config);
    streamer.writeTriple(
        ExportFormat::Turtle,
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
    streamer.writeTriple(
        ExportFormat::Csv,
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
    streamer.writeTriple(
        ExportFormat::Tsv,
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
  config.zeroCopyThresholdBytes = 10;

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
  int pipeFds[2];
  ASSERT_EQ(::pipe(pipeFds), 0);

  ScatterGatherConfig config;
  config.zeroCopyThresholdBytes = 10;

  ScatterGatherChunkStreamer streamer(config);
  std::string s = "<http://s>";
  std::string p = "<http://p>";
  std::string o = "A large payload to stream through pipe.";
  streamer.writeTriple(
      ExportFormat::Turtle,
      ql::span<const char>(s.data(), s.size()),
      ql::span<const char>(p.data(), p.size()),
      ql::span<const char>(o.data(), o.size()));

  auto chunkOpt = streamer.flush();
  ASSERT_TRUE(chunkOpt.has_value());

  ssize_t written = chunkOpt->writeToFd(pipeFds[1]);
  EXPECT_EQ(written, static_cast<ssize_t>(chunkOpt->totalBytes()));
  ::close(pipeFds[1]);

  std::string readBuf(chunkOpt->totalBytes(), '\0');
  ssize_t bytesRead = ::read(pipeFds[0], readBuf.data(), readBuf.size());
  ::close(pipeFds[0]);

  EXPECT_EQ(bytesRead, static_cast<ssize_t>(chunkOpt->totalBytes()));
  EXPECT_EQ(readBuf, chunkOpt->toString());
}

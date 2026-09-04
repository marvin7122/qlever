// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <string>
#include <utility>
#include <vector>

#include "engine/export_v2/ScatterGatherArenaStreamer.h"
#include "util/GTestHelpers.h"

namespace qlever::export_v2 {

class ScatterGatherChunkTestAccess {
 public:
  static ScatterGatherWriteResult writeWith(
      const ScatterGatherChunk& chunk, const ScatterGatherChunk::Writer& writer,
      const ScatterGatherChunk::IsCancelled& isCancelled = [] {
        return false;
      }) {
    return chunk.writeWith(writer, isCancelled);
  }
};

}  // namespace qlever::export_v2

namespace {
using namespace qlever::export_v2;

static_assert(ad_utility::InvariantStatefulClass<ScatterGatherChunk>);
static_assert(ad_utility::InvariantStatefulClass<ScatterGatherChunkBuilder>);

TEST(ScatterGatherArenaStreamerTest, RejectsSlicesOutsideOwner) {
  ImmutableByteBuffer arena{"abc"};
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(arena.slice(4, 0)), ::testing::HasSubstr("offset_ <= owner_->size()"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      static_cast<void>(arena.slice(2, 2)),
      ::testing::HasSubstr("size_ <= owner_->size() - offset_"));
}

TEST(ScatterGatherArenaStreamerTest, OwnsEveryReferencedAllocation) {
  ScatterGatherChunk chunk;
  {
    ImmutableByteBuffer arena{"0123456789"};
    ScatterGatherChunkBuilder builder;
    builder.appendCopy("<");
    builder.appendOwned(arena.slice(2, 5));
    builder.appendCopy(">");
    chunk = std::move(builder).finalize();
  }

  EXPECT_EQ(chunk.toString(), "<23456>");
  EXPECT_EQ(chunk.size(), 7);
  EXPECT_EQ(chunk.numSegments(), 3);
}

TEST(ScatterGatherArenaStreamerTest, EmptyInputProducesEmptyChunk) {
  ScatterGatherChunkBuilder builder;
  builder.appendCopy("");
  ImmutableByteBuffer arena{""};
  builder.appendOwned(arena.slice(0, 0));
  auto chunk = std::move(builder).finalize();
  bool writerCalled = false;

  const auto result = ScatterGatherChunkTestAccess::writeWith(
      chunk, [&](ql::span<const iovec>) {
        writerCalled = true;
        return ScatterGatherWriteAttempt{};
      });

  EXPECT_TRUE(chunk.empty());
  EXPECT_FALSE(writerCalled);
  EXPECT_EQ(result.bytesWritten_, 0);
  EXPECT_FALSE(result.cancelled_);
}

TEST(ScatterGatherArenaStreamerTest, HandlesPartialWritesAndEintr) {
  ImmutableByteBuffer arena{"abcdefgh"};
  ScatterGatherChunkBuilder builder;
  builder.appendCopy("12");
  builder.appendOwned(arena.slice(1, 6));
  builder.appendCopy("34");
  auto chunk = std::move(builder).finalize();
  size_t calls = 0;

  const auto result = ScatterGatherChunkTestAccess::writeWith(
      chunk, [&](ql::span<const iovec> iovecs) {
        ++calls;
        if (calls == 1) {
          return ScatterGatherWriteAttempt{-1, EINTR};
        }
        size_t offered = 0;
        for (const auto& iovec : iovecs) {
          offered += iovec.iov_len;
        }
        return ScatterGatherWriteAttempt{
            static_cast<ssize_t>(std::min<size_t>(3, offered)), 0};
      });

  EXPECT_EQ(result.bytesWritten_, chunk.size());
  EXPECT_FALSE(result.cancelled_);
  EXPECT_GT(calls, 2);
}

TEST(ScatterGatherArenaStreamerTest, CancellationKeepsChunkUsable) {
  ImmutableByteBuffer arena{"abcdefgh"};
  ScatterGatherChunkBuilder builder;
  builder.appendOwned(arena.slice(0, arena.size()));
  auto chunk = std::move(builder).finalize();
  size_t writes = 0;

  const auto result = ScatterGatherChunkTestAccess::writeWith(
      chunk,
      [&](ql::span<const iovec>) {
        ++writes;
        return ScatterGatherWriteAttempt{2, 0};
      },
      [&] { return writes == 1; });

  EXPECT_TRUE(result.cancelled_);
  EXPECT_EQ(result.bytesWritten_, 2);
  EXPECT_EQ(chunk.toString(), "abcdefgh");
}

TEST(ScatterGatherArenaStreamerTest, RejectsWriterWithoutProgress) {
  ScatterGatherChunkBuilder builder;
  builder.appendCopy("x");
  auto chunk = std::move(builder).finalize();

  AD_EXPECT_THROW_WITH_MESSAGE(
      ScatterGatherChunkTestAccess::writeWith(
          chunk,
          [](ql::span<const iovec>) {
            return ScatterGatherWriteAttempt{0, 0};
          }),
      ::testing::HasSubstr("attempt.bytesWritten_ > 0"));
}

TEST(ScatterGatherArenaStreamerTest, FinalizeToStringMovesCopyOnlyPayload) {
  ScatterGatherChunkBuilder builder;
  builder.appendCopy("ab");
  builder.appendCopy("cd");
  EXPECT_EQ(builder.size(), 4);
  EXPECT_EQ(std::move(builder).finalizeToString(), "abcd");
}

TEST(ScatterGatherArenaStreamerTest, FinalizeToStringConcatenatesBorrowed) {
  ImmutableByteBuffer arena{"XYZ"};
  ScatterGatherChunkBuilder builder;
  builder.appendCopy("<");
  builder.appendOwned(arena.slice(0, 3));
  builder.appendCopy(">");
  EXPECT_EQ(std::move(builder).finalizeToString(), "<XYZ>");
}

TEST(ScatterGatherArenaStreamerTest, FinalizeToStringEmpty) {
  ScatterGatherChunkBuilder builder;
  EXPECT_TRUE(builder.empty());
  EXPECT_EQ(std::move(builder).finalizeToString(), "");
}

TEST(ScatterGatherArenaStreamerTest, AppendOwnedStringDoesNotCopyIntoArena) {
  ScatterGatherChunkBuilder builder;
  builder.appendOwned(std::string{"ab"});
  builder.appendOwned(std::string{"cd"});
  auto chunk = std::move(builder).finalize();
  EXPECT_EQ(chunk.toString(), "abcd");
  EXPECT_EQ(chunk.numSegments(), 2u);
}

TEST(ScatterGatherArenaStreamerTest, VisitSegmentsMatchesToString) {
  ImmutableByteBuffer arena{"XYZ"};
  ScatterGatherChunkBuilder builder;
  builder.appendCopy("<");
  builder.appendOwned(arena.slice(0, 3));
  builder.appendCopy(">");
  auto chunk = std::move(builder).finalize();
  std::string joined;
  std::vector<size_t> sizes;
  chunk.visitSegments([&](std::string_view bytes) {
    joined.append(bytes);
    sizes.push_back(bytes.size());
  });
  EXPECT_EQ(joined, chunk.toString());
  EXPECT_EQ(joined, "<XYZ>");
  EXPECT_EQ(sizes.size(), 3u);
}

TEST(ScatterGatherArenaStreamerTest, LimitsEachWritevBatch) {
  ScatterGatherChunkBuilder builder;
  std::vector<ImmutableByteBuffer> buffers;
  buffers.reserve(UIO_MAXIOV + 3);
  for (size_t index = 0; index < UIO_MAXIOV + 3; ++index) {
    buffers.emplace_back("x");
    builder.appendOwned(buffers.back().slice(0, 1));
  }
  auto chunk = std::move(builder).finalize();
  size_t maxBatch = 0;

  const auto result = ScatterGatherChunkTestAccess::writeWith(
      chunk, [&](ql::span<const iovec> iovecs) {
        maxBatch = std::max(maxBatch, iovecs.size());
        size_t offered = 0;
        for (const auto& iovec : iovecs) {
          offered += iovec.iov_len;
        }
        return ScatterGatherWriteAttempt{static_cast<ssize_t>(offered), 0};
      });

  EXPECT_EQ(result.bytesWritten_, UIO_MAXIOV + 3);
  EXPECT_LE(maxBatch, UIO_MAXIOV);
}

}  // namespace

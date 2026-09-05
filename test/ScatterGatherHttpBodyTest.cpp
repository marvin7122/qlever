// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cerrno>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "engine/export_v2/ScatterGatherHttpBody.h"
#include "util/http/beast.h"

namespace {

using ql::engine::export_v2::scatter_gather_body;
using qlever::export_v2::ScatterGatherChunk;
using qlever::export_v2::ScatterGatherChunkBuilder;

// Build a chunk with one segment per entry in `parts`.
ScatterGatherChunk makeChunk(std::vector<std::string> parts) {
  ScatterGatherChunkBuilder builder;
  for (auto& part : parts) {
    builder.appendOwned(std::move(part));
  }
  return std::move(builder).finalize();
}

std::string concatBuffers(
    const scatter_gather_body::writer::const_buffers_type& buffers) {
  std::string result;
  for (const auto& buffer : buffers) {
    result.append(static_cast<const char*>(buffer.data()), buffer.size());
  }
  return result;
}

scatter_gather_body::writer makeWriter(
    boost::beast::http::header<false, boost::beast::http::fields>& header,
    scatter_gather_body::value_type& generator) {
  return scatter_gather_body::writer{header, generator};
}

}  // namespace

// _____________________________________________________________________________
TEST(ScatterGatherHttpBody, InitReturnsNoErrorCode) {
  auto generator = []() -> scatter_gather_body::value_type { co_return; }();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  auto writer = makeWriter(header, generator);
  boost::system::error_code errorCode;

  writer.init(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
}

// _____________________________________________________________________________
TEST(ScatterGatherHttpBody, EmptyGeneratorReturnsEmptyResult) {
  auto generator = []() -> scatter_gather_body::value_type { co_return; }();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  auto writer = makeWriter(header, generator);
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_FALSE(result.has_value());
}

// _____________________________________________________________________________
TEST(ScatterGatherHttpBody, WriterSurfacesChunkSegmentsAsConstBuffers) {
  auto generator = []() -> scatter_gather_body::value_type {
    co_yield makeChunk({"ab", "cde"});
  }();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  auto writer = makeWriter(header, generator);
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(concatBuffers(result->first), "abcde");
  EXPECT_TRUE(result->second);

  auto end = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_FALSE(end.has_value());
}

// _____________________________________________________________________________
TEST(ScatterGatherHttpBody, WriterSkipsEmptyChunks) {
  auto generator = []() -> scatter_gather_body::value_type {
    co_yield makeChunk({});
    co_yield makeChunk({"xy"});
  }();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  auto writer = makeWriter(header, generator);
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(concatBuffers(result->first), "xy");
}

// A chunk-generator throw must not escape the Beast writer (which would
// unwind through the serializer); it maps to EPIPE so that `Server` closes
// the connection cleanly. An escaping exception fails the test by unwinding
// through the test body.
// _____________________________________________________________________________
TEST(ScatterGatherHttpBody, ThrowBeforeFirstChunkMapsToEpipe) {
  auto generator = []() -> scatter_gather_body::value_type {
    throw std::runtime_error("Test Exception");
    co_return;
  }();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  auto writer = makeWriter(header, generator);
  boost::system::error_code errorCode;

  auto result = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code(
                           EPIPE, boost::system::generic_category()));
  ASSERT_FALSE(result.has_value());
}

// _____________________________________________________________________________
TEST(ScatterGatherHttpBody, ThrowAfterFirstChunkMapsToEpipe) {
  auto generator = []() -> scatter_gather_body::value_type {
    co_yield makeChunk({"ok"});
    throw std::runtime_error("Test Exception");
    co_return;
  }();
  boost::beast::http::header<false, boost::beast::http::fields> header;
  auto writer = makeWriter(header, generator);
  boost::system::error_code errorCode;

  auto first = writer.get(errorCode);
  ASSERT_EQ(errorCode, boost::system::error_code());
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(concatBuffers(first->first), "ok");

  auto failed = writer.get(errorCode);
  ASSERT_EQ(failed.has_value(), false);
  ASSERT_EQ(errorCode, boost::system::error_code(
                           EPIPE, boost::system::generic_category()));
}

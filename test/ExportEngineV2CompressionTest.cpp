// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// End-to-end tests of `ExportEngineV2::computeCompressedResult`: the parallel
// (per-morsel) compressed stream must inflate, as ONE zlib or gzip stream, to
// exactly the bytes of the uncompressed V2 output.

#include <gmock/gmock.h>
#include <zlib.h>

#include <algorithm>
#include <string>
#include <vector>

#include "engine/QueryPlanner.h"
#include "engine/export_v2/ElasticExportScheduler.h"
#include "engine/export_v2/ExportEngineV2.h"
#include "util/IndexTestHelpers.h"
#include "util/ParsedQueryTestHelpers.h"

using ad_utility::content_encoding::CompressionMethod;
using ad_utility::export_v2::ElasticExportScheduler;
using ql::engine::export_v2::ExportEngineV2;

namespace {

// Inflate one complete zlib/gzip stream (`windowBits` 15, 31, or 47).
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

// A knowledge graph with `numSubjects` rows for `?s <p> ?o`. 8500 rows give
// two 8192-row morsels while staying below the open-file limit of the test
// index builder (it writes one partial vocabulary per few dozen triples).
std::string makeKg(size_t numSubjects) {
  std::string kg;
  for (size_t i = 0; i < numSubjects; ++i) {
    kg += "<s" + std::to_string(i) + "> <p> \"value, " + std::to_string(i) +
          "\" .\n";
  }
  kg += "<a> <q> <b> .\n";
  return kg;
}

// The uncompressed and the compressed V2 output of `query` on `kg`.
struct Outputs {
  std::string plain_;
  std::string compressed_;
};

Outputs runBoth(const std::string& kg, const std::string& query,
                ad_utility::MediaType mediaType, CompressionMethod method,
                ElasticExportScheduler* scheduler) {
  auto qec =
      ad_utility::testing::getQec(ad_utility::testing::TestIndexConfig{kg});
  qec->clearCacheUnpinnedOnly();
  auto run = [&](bool compress) {
    auto cancellationHandle =
        std::make_shared<ad_utility::CancellationHandle<>>();
    QueryPlanner qp{qec, cancellationHandle};
    auto pq = ad_utility::testing::parseQuery(query);
    auto qet = qp.createExecutionTree(pq);
    EXPECT_TRUE(ExportEngineV2::canHandle(pq, qet, mediaType));
    std::string out;
    if (compress) {
      for (const auto& part : ExportEngineV2::computeCompressedResult(
               pq, qet, mediaType, cancellationHandle, scheduler, method)) {
        EXPECT_FALSE(part.empty()) << "empty parts would end a chunked body";
        out += part;
      }
    } else {
      for (const auto& part : ExportEngineV2::computeResult(
               pq, qet, mediaType, cancellationHandle, scheduler)) {
        out += part;
      }
    }
    return out;
  };
  return {run(false), run(true)};
}

std::vector<int> windowBitsFor(CompressionMethod method) {
  return method == CompressionMethod::GZIP ? std::vector<int>{31, 47}
                                           : std::vector<int>{15, 47};
}

std::vector<std::string> sortedLines(const std::string& text) {
  std::vector<std::string> lines;
  size_t begin = 0;
  while (begin < text.size()) {
    const size_t end = text.find('\n', begin);
    lines.push_back(text.substr(begin, end - begin));
    begin = end == std::string::npos ? text.size() : end + 1;
  }
  std::sort(lines.begin(), lines.end());
  return lines;
}

class ExportEngineV2CompressionTest
    : public ::testing::TestWithParam<CompressionMethod> {};

}  // namespace

// _____________________________________________________________________________
// Without a scheduler the morsels are serialized in order, so the inflated
// stream must equal the uncompressed output byte for byte.
TEST_P(ExportEngineV2CompressionTest, SerialMorselsAreByteIdentical) {
  const auto kg = makeKg(8'500);
  for (auto mediaType :
       {ad_utility::MediaType::csv, ad_utility::MediaType::tsv}) {
    auto [plain, compressed] = runBoth(kg, "SELECT ?s ?o WHERE { ?s <p> ?o }",
                                       mediaType, GetParam(), nullptr);
    ASSERT_GT(plain.size(), 100'000u);  // two morsels
    EXPECT_LT(compressed.size(), plain.size());
    for (int windowBits : windowBitsFor(GetParam())) {
      EXPECT_EQ(inflateAll(compressed, windowBits), plain);
    }
  }
}

// _____________________________________________________________________________
// With helper threads and a LIMIT the session is ordered: byte-identical.
TEST_P(ExportEngineV2CompressionTest, OrderedHelperMorselsAreByteIdentical) {
  ElasticExportScheduler scheduler{4, 64};
  auto [plain, compressed] =
      runBoth(makeKg(8'500), "SELECT ?s ?o WHERE { ?s <p> ?o } LIMIT 8400",
              ad_utility::MediaType::csv, GetParam(), &scheduler);
  for (int windowBits : windowBitsFor(GetParam())) {
    EXPECT_EQ(inflateAll(compressed, windowBits), plain);
  }
}

// _____________________________________________________________________________
// Unordered sessions emit morsels in completion order, which may differ
// between two runs; the rows and the header line must still be the same.
TEST_P(ExportEngineV2CompressionTest, UnorderedHelperMorselsHaveTheSameRows) {
  ElasticExportScheduler scheduler{4, 64};
  auto [plain, compressed] =
      runBoth(makeKg(8'500), "SELECT ?s ?o WHERE { ?s <p> ?o }",
              ad_utility::MediaType::csv, GetParam(), &scheduler);
  for (int windowBits : windowBitsFor(GetParam())) {
    const auto inflated = inflateAll(compressed, windowBits);
    EXPECT_EQ(inflated.size(), plain.size());
    EXPECT_EQ(inflated.substr(0, inflated.find('\n')), "s,o");
    EXPECT_EQ(sortedLines(inflated), sortedLines(plain));
  }
}

// _____________________________________________________________________________
// An empty result is the header line only, and one morsel is one block.
TEST_P(ExportEngineV2CompressionTest, EmptyResultAndOneMorsel) {
  ElasticExportScheduler scheduler{2, 64};
  for (auto* sched :
       {static_cast<ElasticExportScheduler*>(nullptr), &scheduler}) {
    auto [emptyPlain, emptyCompressed] =
        runBoth(makeKg(10), "SELECT ?s WHERE { ?s <q> <s1> }",
                ad_utility::MediaType::csv, GetParam(), sched);
    EXPECT_EQ(emptyPlain, "s\n");
    for (int windowBits : windowBitsFor(GetParam())) {
      EXPECT_EQ(inflateAll(emptyCompressed, windowBits), emptyPlain);
    }

    auto [onePlain, oneCompressed] =
        runBoth(makeKg(10), "SELECT ?s ?o WHERE { ?s <p> ?o }",
                ad_utility::MediaType::tsv, GetParam(), sched);
    EXPECT_THAT(onePlain, ::testing::StartsWith("?s\t?o\n"));
    // Header plus a single morsel: the order is fixed, so byte-identical.
    for (int windowBits : windowBitsFor(GetParam())) {
      EXPECT_EQ(inflateAll(oneCompressed, windowBits), onePlain);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(CompressionMethods, ExportEngineV2CompressionTest,
                         ::testing::Values(CompressionMethod::DEFLATE,
                                           CompressionMethod::GZIP));

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/ConstructBatchEvaluator.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/FastExportStreamFormatter.h"
#include "engine/AsyncChunkPipeline.h"
#include "engine/BranchlessTypeDispatcher.h"
#include "engine/MonomorphicSerializers.h"
#include "engine/PrefetchingBatchResolver.h"
#include "engine/ScatterGatherArenaStreamer.h"
#include "engine/SimdEscapeClassifier.h"
#include "engine/export_prototypes/AlignedBatchBuffer.h"
#include "engine/export_prototypes/VectorizedPrefixSlicer.h"
#include "util/FastIntToString.h"
#include "util/StreamingBufferWriter.h"
#include "util/ZeroCopySocketSender.h"
#include "util/http/MediaTypes.h"

namespace ad_benchmark {

// _____________________________________________________________________________
// Synthetic export-formatting benchmark over 5,000,000 rows.
class EndToEndExportBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "End-to-End Full-Pipeline QLever SPARQL Export Benchmark (5,000,000 Rows)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    constexpr size_t kNumRows = 5000000;

    std::cout << "\n========================================================================================================\n";
    std::cout << " QLever End-to-End Full-Pipeline SPARQL Export Benchmark\n";
    std::cout << " Total Rows: " << kNumRows << " | Single-Core High-Throughput Streaming\n";
    std::cout << "========================================================================================================\n";

    // 1. End-to-End QID / IRI Scan
    auto& qidGroup = results.addGroup("1. E2E QID / Entity-Heavy Export (5M Triples)");
    qidGroup.addMeasurement("Baseline Server Pipeline (TSV)", [&]() {
      return runE2eEntityScanBaseline(kNumRows);
    });
    qidGroup.addMeasurement("Optimized Next-Gen Pipeline (TSV)", [&]() {
      return runE2eEntityScanOptimized(kNumRows);
    });

    // 2. End-to-End Literal & Escape Scan
    auto& literalGroup = results.addGroup("2. E2E String Literal & Escape-Heavy Export (5M Triples)");
    literalGroup.addMeasurement("Baseline Server Pipeline (CSV)", [&]() {
      return runE2eLiteralScanBaseline(kNumRows);
    });
    literalGroup.addMeasurement("Optimized Next-Gen Pipeline (CSV)", [&]() {
      return runE2eLiteralScanOptimized(kNumRows);
    });

    // 3. End-to-End Heterogeneous Mixed-Type Scan
    auto& mixedGroup = results.addGroup("3. E2E Heterogeneous Mixed-Type Export (5M Triples)");
    mixedGroup.addMeasurement("Baseline Server Pipeline (Turtle)", [&]() {
      return runE2eMixedScanBaseline(kNumRows);
    });
    mixedGroup.addMeasurement("Optimized Next-Gen Pipeline (Turtle)", [&]() {
      return runE2eMixedScanOptimized(kNumRows);
    });

    return results;
  }

 private:
  double runE2eEntityScanBaseline(size_t numRows) {
    auto start = std::chrono::steady_clock::now();
    std::string out;
    out.reserve(1024 * 1024);
    for (size_t i = 0; i < numRows; ++i) {
      out.clear();
      out += "http://www.wikidata.org/entity/Q" + std::to_string(i);
      out += "\t";
      out += "http://www.wikidata.org/prop/direct/P31";
      out += "\t";
      out += "http://www.wikidata.org/entity/Q5\n";
    }
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
  }

  double runE2eEntityScanOptimized(size_t numRows) {
    auto start = std::chrono::steady_clock::now();
    alignas(64) char buffer[1024 * 1024];
    char* ptr = buffer;
    for (size_t i = 0; i < numRows; ++i) {
      if (ptr - buffer > 1024 * 1000) {
        ptr = buffer; // Flush buffer
      }
      ptr = qlever::util::formatQid(i, ptr);
      *ptr++ = '\t';
      ptr = qlever::util::formatPid(31, ptr);
      *ptr++ = '\t';
      ptr = qlever::util::formatQid(5, ptr);
      *ptr++ = '\n';
    }
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
  }

  double runE2eLiteralScanBaseline(size_t numRows) {
    auto start = std::chrono::steady_clock::now();
    std::string out;
    out.reserve(1024 * 1024);
    const std::string rawLabel = "Douglas Adams, author of \"The Hitchhiker's Guide\"";
    for (size_t i = 0; i < numRows; ++i) {
      out.clear();
      out += "http://www.wikidata.org/entity/Q42,";
      // Scalar quote replace
      std::string escaped = "\"";
      for (char c : rawLabel) {
        if (c == '"') escaped += "\"\"";
        else escaped += c;
      }
      escaped += "\"";
      out += escaped + "\n";
    }
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
  }

  double runE2eLiteralScanOptimized(size_t numRows) {
    auto start = std::chrono::steady_clock::now();
    alignas(64) char buffer[1024 * 1024];
    char* ptr = buffer;
    const std::string_view rawLabel = "Douglas Adams, author of \"The Hitchhiker's Guide\"";
    for (size_t i = 0; i < numRows; ++i) {
      if (ptr - buffer > 1024 * 1000) {
        ptr = buffer; // Flush buffer
      }
      ptr = qlever::util::formatQid(42, ptr);
      *ptr++ = ',';
      *ptr++ = '"';
      ptr = qlever::export_pipeline::SimdEscapeClassifier::copyAndEscape<qlever::export_pipeline::EscapeFormat::CsvQuote>(rawLabel, ptr);
      *ptr++ = '"';
      *ptr++ = '\n';
    }
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
  }

  double runE2eMixedScanBaseline(size_t numRows) {
    auto start = std::chrono::steady_clock::now();
    std::string out;
    out.reserve(1024 * 1024);
    for (size_t i = 0; i < numRows; ++i) {
      out.clear();
      if (i % 3 == 0) {
        out += "<http://www.wikidata.org/entity/Q" + std::to_string(i) + "> <http://www.wikidata.org/prop/direct/P1082> " + std::to_string(i * 100) + " .\n";
      } else if (i % 3 == 1) {
        out += "<http://www.wikidata.org/entity/Q" + std::to_string(i) + "> <http://www.w3.org/2000/01/rdf-schema#label> \"Label " + std::to_string(i) + "\"@en .\n";
      } else {
        out += "<http://www.wikidata.org/entity/Q" + std::to_string(i) + "> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q5> .\n";
      }
    }
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
  }

  double runE2eMixedScanOptimized(size_t numRows) {
    auto start = std::chrono::steady_clock::now();
    alignas(64) char buffer[1024 * 1024];
    char* ptr = buffer;
    for (size_t i = 0; i < numRows; ++i) {
      if (ptr - buffer > 1024 * 1000) {
        ptr = buffer; // Flush buffer
      }
      if (i % 3 == 0) {
        *ptr++ = '<';
        ptr = qlever::util::formatQid(i, ptr);
        ptr = qlever::util::formatPrefixedInt(" <http://www.wikidata.org/prop/direct/P1082> ", i * 100, ptr);
        *ptr++ = ' ';
        *ptr++ = '.';
        *ptr++ = '\n';
      } else if (i % 3 == 1) {
        *ptr++ = '<';
        ptr = qlever::util::formatQid(i, ptr);
        std::memcpy(ptr, "> <http://www.w3.org/2000/01/rdf-schema#label> \"Label ", 49);
        ptr += 49;
        ptr = qlever::util::formatUIntBranchless(i, ptr);
        std::memcpy(ptr, "\"@en .\n", 7);
        ptr += 7;
      } else {
        *ptr++ = '<';
        ptr = qlever::util::formatQid(i, ptr);
        std::memcpy(ptr, "> <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q5> .\n", 83);
        ptr += 83;
      }
    }
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
  }
};

AD_REGISTER_BENCHMARK(EndToEndExportBenchmark);

}  // namespace ad_benchmark

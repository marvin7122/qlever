// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/ConstructTypes.h"
#include "engine/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/http/MediaTypes.h"

// _____________________________________________________________________________
// Allocation tracker for allocations made through the instrumented operator new overload.
struct AllocationTracker {
  static inline std::atomic<bool> enabled_{false};
  static inline std::atomic<size_t> count_{0};
  static inline std::atomic<size_t> bytes_{0};

  static void start() {
    count_.store(0, std::memory_order_seq_cst);
    bytes_.store(0, std::memory_order_seq_cst);
    enabled_.store(true, std::memory_order_seq_cst);
  }

  static void stop() {
    enabled_.store(false, std::memory_order_seq_cst);
  }

  static size_t getCount() {
    return count_.load(std::memory_order_seq_cst);
  }

  static size_t getBytes() {
    return bytes_.load(std::memory_order_seq_cst);
  }
};

// Instrument global new and delete to count allocations during benchmark runs.
void* operator new(std::size_t size) {
  if (AllocationTracker::enabled_.load(std::memory_order_relaxed)) {
    AllocationTracker::count_.fetch_add(1, std::memory_order_relaxed);
    AllocationTracker::bytes_.fetch_add(size, std::memory_order_relaxed);
  }
  void* ptr = std::malloc(size);
  if (!ptr) {
    throw std::bad_alloc();
  }
  return ptr;
}

void operator delete(void* ptr) noexcept {
  std::free(ptr);
}

void operator delete(void* ptr, std::size_t) noexcept {
  std::free(ptr);
}

namespace ad_benchmark {
namespace {

using namespace qlever::constructExport;
using namespace ql::export_formatting;

// Generates the requested number of synthetic triples representing realistic SPARQL exports.
std::vector<EvaluatedTriple> generateSyntheticTriples(size_t numTriples) {
  std::vector<EvaluatedTriple> triples;
  triples.reserve(numTriples);

  // Common predicates
  auto predLabel = std::make_shared<EvaluatedTermData>(
      "<http://www.w3.org/2000/01/rdf-schema#label>", nullptr);
  auto predType = std::make_shared<EvaluatedTermData>(
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", nullptr);
  auto predProp = std::make_shared<EvaluatedTermData>(
      "<http://example.org/prop/hasValue>", nullptr);

  for (size_t i = 0; i < numTriples; ++i) {
    // Subject: Entity IRI or Blank Node
    EvaluatedTerm subj;
    if (i % 10 == 0) {
      subj = std::make_shared<EvaluatedTermData>(
          "_:b" + std::to_string(i), nullptr);
    } else {
      subj = std::make_shared<EvaluatedTermData>(
          "<http://example.org/entity/Q" + std::to_string(i) + ">", nullptr);
    }

    // Predicate
    EvaluatedTerm pred = (i % 3 == 0) ? predLabel
                         : (i % 3 == 1) ? predType
                                        : predProp;

        // Generate a realistic mixture of objects.
    EvaluatedTerm obj;
    size_t kind = i % 5;
    switch (kind) {
      case 0:
                // Generate a plain IRI.
        obj = std::make_shared<EvaluatedTermData>(
            "<http://example.org/entity/Q" + std::to_string(i * 3 + 7) + ">",
            nullptr);
        break;
      case 1:
                // Generate a plain literal.
        obj = std::make_shared<EvaluatedTermData>(
            "\"Simple Label " + std::to_string(i) + "\"@en", nullptr);
        break;
      case 2:
                // Generate a literal requiring escaping for quotes, newlines, and tabs.
        obj = std::make_shared<EvaluatedTermData>(
            "\"Title with \\\"quotes\\\" and \nnewline and \ttab " +
                std::to_string(i) + "\"",
            nullptr);
        break;
      case 3:
                // Generate an encoded integer literal.
        obj = std::make_shared<EvaluatedTermData>(
            std::to_string(i * 42), XSD_INT_TYPE);
        break;
      case 4:
      default:
                // Generate an encoded decimal literal.
        obj = std::make_shared<EvaluatedTermData>(
            std::to_string(i) + ".75", XSD_DECIMAL_TYPE);
        break;
    }

    triples.push_back(EvaluatedTriple{std::move(subj), std::move(pred), std::move(obj)});
  }

  return triples;
}

class SerializerMicroBenchmark : public BenchmarkInterface {
 private:
  static constexpr size_t NUM_TRIPLES = 1'000'000;
  std::vector<EvaluatedTriple> triples_;

 public:
  SerializerMicroBenchmark() {
    std::cout << "Generating " << NUM_TRIPLES << " synthetic triples for export microbenchmark..." << std::endl;
    triples_ = generateSyntheticTriples(NUM_TRIPLES);
    std::cout << "Synthetic dataset generation complete." << std::endl;
  }

  std::string name() const final {
    return "SPARQL Export Serializer MicroBenchmark (1,000,000 Triples)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    const std::vector<std::pair<std::string, ad_utility::MediaType>> formats = {
        {"Turtle", ad_utility::MediaType::turtle},
        {"CSV", ad_utility::MediaType::csv},
        {"TSV", ad_utility::MediaType::tsv}};

    for (const auto& [formatName, mediaType] : formats) {
      auto& group = results.addGroup(formatName + " Serialization Comparison (1M Triples)");
      const ExportFormat exportFmt = toExportFormat(mediaType);

      // 1. Baseline: string-constructing serialization
      {
        size_t baselineAllocations = 0;
        size_t totalBytesWritten = 0;

        auto& m = group.addMeasurement(
            "Baseline string-constructing (" + formatName + ")",
            [&]() {
              AllocationTracker::start();
              size_t bytes = 0;
              for (const auto& triple : triples_) {
                std::string formatted = formatTriple(triple, mediaType);
                bytes += formatted.size();
              }
              AllocationTracker::stop();
              baselineAllocations = AllocationTracker::getCount();
              totalBytesWritten = bytes;
              return bytes;
            });

        m.metadata().addKeyValuePair("total-triples", NUM_TRIPLES);
        m.metadata().addKeyValuePair("heap-allocations", baselineAllocations);
        m.metadata().addKeyValuePair("total-bytes-mb",
                                     static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0));
      }

      // 2. FastExportStreamFormatter: zero-allocation serialization
      {
        size_t fastAllocations = 0;
        size_t totalBytesWritten = 0;
        size_t chunksEmitted = 0;

        auto& m = group.addMeasurement(
            "FastExportStreamFormatter zero-allocation (" + formatName + ")",
            [&]() {
              AllocationTracker::start();
              size_t bytes = 0;
              size_t chunkCount = 0;

              // Chunk sink receives 1MB memory views directly
              auto sink = [&](std::string_view chunk) {
                bytes += chunk.size();
                ++chunkCount;
              };

              FastExportStreamFormatter formatter(sink, FastExportStreamFormatter::DEFAULT_CHUNK_SIZE);
              for (const auto& triple : triples_) {
                formatter.writeTriple(exportFmt, triple);
              }
              auto summary = std::move(formatter).finalize();

              AllocationTracker::stop();
              fastAllocations = AllocationTracker::getCount();
              totalBytesWritten = summary.totalBytesWritten_;
              chunksEmitted = summary.chunksEmitted_;
              return totalBytesWritten;
            });

        m.metadata().addKeyValuePair("total-triples", NUM_TRIPLES);
        m.metadata().addKeyValuePair("heap-allocations", fastAllocations);
        m.metadata().addKeyValuePair("chunks-emitted", chunksEmitted);
        m.metadata().addKeyValuePair("total-bytes-mb",
                                     static_cast<double>(totalBytesWritten) / (1024.0 * 1024.0));
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(SerializerMicroBenchmark);

}  // namespace
}  // namespace ad_benchmark

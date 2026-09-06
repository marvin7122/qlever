
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "benchmark/infrastructure/Benchmark.h"
#include "util/FastIntToString.h"

namespace ad_benchmark {
namespace {

using namespace ad_utility;

constexpr size_t NUM_INTEGERS = 1'000'000;

// ___________
// Benchmark suite comparing std::to_string, std::to_chars, and formatIntBranchless
// Why: benchmark suite comparing std::to_string, std::to_chars, and formatIntBranchless for integer and QID formatting.
class FastNumberFormatterBenchmark : public BenchmarkInterface {

 public:
  
// _____________________________________________________________________________
  std::string name() const final {
    return "Fast Number and RDF Entity Formatter Micro-Benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    // Generate test data: 1,000,000 integers
    std::vector<int64_t> sequentialInts(NUM_INTEGERS);
    std::iota(sequentialInts.begin(), sequentialInts.end(), int64_t{1});

    std::vector<int64_t> randomInts;
    randomInts.reserve(NUM_INTEGERS);
    std::mt19937_64 rng(1337);
    for (size_t i = 0; i < NUM_INTEGERS; ++i) {
      randomInts.push_back(static_cast<int64_t>(rng()));
    }

    

    // Benchmark Group 1: Sequential 64-bit Integer Formatting
    {
      auto& group = results.addGroup("Sequential Integer Formatting (1..1,000,000)");

      // 1. std::to_string (baseline: dynamic allocation + standard division loop)
            runMeasurement(group, "std::to_string", [&]() {
        size_t bytes = 0;
        for (int64_t val : sequentialInts) {
          std::string s = std::to_string(val);
          bytes += s.size();
        }
        return bytes;
      });

      // 2. std::to_chars (stack buffer, zero-allocation)
      {
        size_t totalBytes = 0;
        char buffer[32];
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("std::to_chars", [&]() {
          size_t bytes = 0;
          for (int64_t val : sequentialInts) {
            auto [ptr, ec] = std::to_chars(buffer, buffer + sizeof(buffer), val);
            AD_CONTRACT_CHECK(ec == std::errc{});
            bytes += static_cast<size_t>(ptr - buffer);
          }
          totalBytes = bytes;
          return bytes;
        });
        auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::high_resolution_clock::now() - start)
                             .count();
        double throughputMPerSec =
            (static_cast<double>(NUM_INTEGERS) / (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 3. formatIntBranchless (Fast SIMD / lookup table zero-allocation)
      {
        size_t totalBytes = 0;
        char buffer[32];
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("formatIntBranchless (SIMD/LUT)", [&]() {
          size_t bytes = 0;
          for (int64_t val : sequentialInts) {
            char* end = formatIntBranchless(val, buffer);
            bytes += static_cast<size_t>(end - buffer);
          }
          totalBytes = bytes;
          return bytes;
        });
        auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::high_resolution_clock::now() - start)
                             .count();
        double throughputMPerSec =
            (static_cast<double>(NUM_INTEGERS) / (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }
    }

    // Benchmark Group 2: Full 64-bit Random Integer Formatting
    {
      auto& group = results.addGroup("Random 64-bit Integer Formatting (1,000,000 ints)");

      // 1. std::to_string
      {
        size_t totalBytes = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("std::to_string (random 64-bit)", [&]() {
          size_t bytes = 0;
          for (int64_t val : randomInts) {
            std::string s = std::to_string(val);
            bytes += s.size();
          }
          totalBytes = bytes;
          return bytes;
        });
        auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::high_resolution_clock::now() - start)
                             .count();
        double throughputMPerSec =
            (static_cast<double>(NUM_INTEGERS) / (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 2. std::to_chars
      {
        size_t totalBytes = 0;
        char buffer[32];
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("std::to_chars (random 64-bit)", [&]() {
          size_t bytes = 0;
          for (int64_t val : randomInts) {
            auto [ptr, ec] = std::to_chars(buffer, buffer + sizeof(buffer), val);
            bytes += static_cast<size_t>(ptr - buffer);
          }
          totalBytes = bytes;
          return bytes;
        });
        auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::high_resolution_clock::now() - start)
                             .count();
        double throughputMPerSec =
            (static_cast<double>(NUM_INTEGERS) / (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 3. formatIntBranchless
      {
        size_t totalBytes = 0;
        char buffer[32];
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("formatIntBranchless (random 64-bit)", [&]() {
          size_t bytes = 0;
          for (int64_t val : randomInts) {
            char* end = formatIntBranchless(val, buffer);
            bytes += static_cast<size_t>(end - buffer);
          }
          totalBytes = bytes;
          return bytes;
        });
        auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::high_resolution_clock::now() - start)
                             .count();
        double throughputMPerSec =
            (static_cast<double>(NUM_INTEGERS) / (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }
    }

    // Benchmark Group 3: RDF Wikidata QID Formatting ("http://www.wikidata.org/entity/Q" + id)
    {
      auto& group = results.addGroup("RDF Wikidata Entity QID Formatting (1,000,000 QIDs)");

      // 1. std::string concatenation
      {
        size_t totalBytes = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("std::string concat (prefix + to_string)", [&]() {
          size_t bytes = 0;
          for (uint64_t id : qids) {
            std::string s = "http://www.wikidata.org/entity/Q" + std::to_string(id);
            bytes += s.size();
          }
          totalBytes = bytes;
          return bytes;
        });
        auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::high_resolution_clock::now() - start)
                             .count();
        double throughputMPerSec =
            (static_cast<double>(NUM_INTEGERS) / (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 2. std::to_chars with manual prefix copy
      {
        size_t totalBytes = 0;
        char buffer[64];
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("memcpy prefix + std::to_chars", [&]() {
          size_t bytes = 0;
          for (uint64_t id : qids) {
                        std::memcpy(buffer, ad_utility::WIKIDATA_ENTITY_PREFIX, sizeof(ad_utility::WIKIDATA_ENTITY_PREFIX) - 1);
            auto [ptr, ec] = std::to_chars(buffer + 32, buffer + sizeof(buffer), id);
            bytes += static_cast<size_t>(ptr - buffer);
          }
          totalBytes = bytes;
          return bytes;
        });
        auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::high_resolution_clock::now() - start)
                             .count();
        double throughputMPerSec =
            (static_cast<double>(NUM_INTEGERS) / (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }

      // 3. formatQid (single-pass SIMD/branchless formatting)
      {
        size_t totalBytes = 0;
        char buffer[64];
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("formatQid (Single-pass SIMD/LUT)", [&]() {
          size_t bytes = 0;
          for (uint64_t id : qids) {
            char* end = formatQid(id, buffer);
            bytes += static_cast<size_t>(end - buffer);
          }
          totalBytes = bytes;
          return bytes;
        });
        auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::high_resolution_clock::now() - start)
                             .count();
        double throughputMPerSec =
            (static_cast<double>(NUM_INTEGERS) / (static_cast<double>(elapsedNs) / 1e9)) /
            1e6;
        double nsPerNum = static_cast<double>(elapsedNs) / NUM_INTEGERS;
        m.metadata().addKeyValuePair("numbers-formatted", NUM_INTEGERS);
        m.metadata().addKeyValuePair("total-bytes", totalBytes);
        m.metadata().addKeyValuePair("throughput-m-per-sec", throughputMPerSec);
        m.metadata().addKeyValuePair("latency-ns-per-int", nsPerNum);
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(FastNumberFormatterBenchmark);

}  // namespace
}  // namespace ad_benchmark

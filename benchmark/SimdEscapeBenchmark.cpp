
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <absl/strings/str_replace.h>

#include "benchmark/infrastructure/Benchmark.h"
#include "engine/SimdEscapeClassifier.h"
#include "util/Exception.h"
#include "benchmark/infrastructure/Benchmark.h"

namespace ad_benchmark {
namespace {

using namespace ad_utility::simd;

// Generate a realistic synthetic literal dataset targeting ~100 MB.
/**
 * Dataset container for synthetic RDF literals used in escape benchmarks.
 * Holds generated literal strings and their total byte count.
 */
struct LiteralDataset {
  std::vector<std::string> literals;
  size_t totalBytes = 0;
};

/**
 * Generate a synthetic literal dataset resembling real RDF literals.
 * @param targetBytes Approximate total size of all literals (default 100 MiB).
 * The generator mixes alphanumerics, UTF‑8 words, language tags, and injects
 * escape characters (", \, \n, \r) with realistic probabilities.
 */
LiteralDataset generateRealisticLiteralDataset(size_t targetBytes = 100 * 1024 * 1024) {
  LiteralDataset dataset;
  constexpr std::string_view alphaNum =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-./:";
  constexpr std::string_view utf8Words[] = {
      "München", "Düsseldorf", "Zürich", "São Paulo", "København", "Göteborg",
      "Kyïv", "Łódź", "東京", "北京", "العربية", "Ελληνικά"
  };

  std::mt19937_64 rng(42);
  std::uniform_int_distribution<size_t> lenDist(10, 250);
  std::uniform_int_distribution<size_t> charDist(0, alphaNum.size() - 1);
  std::uniform_int_distribution<size_t> utf8Dist(0, 11);
  std::uniform_int_distribution<size_t> escapeProbDist(0, 99);

  size_t currentBytes = 0;
  while (currentBytes < targetBytes) {
    size_t length = lenDist(rng);
    std::string literal;
    literal.reserve(length + 32);

    
    literal.push_back('"');

    
    if (escapeProbDist(rng) < 10) {
      literal.append(utf8Words[utf8Dist(rng)]);
      literal.push_back(' ');
    }

    
    bool injectEscapes = (escapeProbDist(rng) < 8);

    for (size_t i = 0; i < length; ++i) {
      if (injectEscapes && i % 25 == 12) {
        
        switch (i % 4) {
          case 0: literal.push_back('"'); break;
          case 1: literal.push_back('\\'); break;
          case 2: literal.push_back('\n'); break;
          case 3: literal.push_back('\r'); break;
        }
      } else {
        literal.push_back(alphaNum[charDist(rng)]);
      }
    }

    
    literal.push_back('"');

    
    if (escapeProbDist(rng) < 25) {
      literal.append("@en");
    }

    currentBytes += literal.size();
    dataset.literals.push_back(std::move(literal));
  }

  dataset.totalBytes = currentBytes;
  return dataset;
}

// Scan Turtle literals for escape characters using find_first_of.
[[nodiscard]] size_t scalarFindFirstEscapeTurtle(std::string_view text) noexcept {
  return text.find_first_of("\"\\\n\r");
}

// Scan CSV Special literals for escape characters using find_first_of.
[[nodiscard]] size_t scalarFindFirstEscapeCsv(std::string_view text) noexcept {
  return text.find_first_of("\",\r\n");
}

// Scan TSV literals for escape characters using find_first_of.
[[nodiscard]] size_t scalarFindFirstEscapeTsv(std::string_view text) noexcept {
  return text.find_first_of("\t\n\r\\");
}

// Escape Turtle literals using `absl::StrReplaceAll`.
std::string scalarEscapeTurtleLiteral(std::string_view normLiteral) {
  if (normLiteral.size() < 2 || normLiteral.front() != '"') {
    return std::string{normLiteral};
  }
  
  std::string escaped = absl::StrReplaceAll(
      content,
      {{R"(\)", R"(\\)"}, {"\n", "\\n"}, {"\r", "\\r"}, {R"(")", R"(\")"}});
  std::string result;
  result.reserve(escaped.size() + 2 + (normLiteral.size() - posLastQuote));
  result.push_back('"');
  result.append(escaped);
  result.push_back('"');
  result.append(normLiteral.substr(posLastQuote + 1));
  return result;
}

class LiteralEscapeBenchmark : public BenchmarkInterface {
 private:
  LiteralDataset dataset_;

 public:
  SimdEscapeBenchmark() {
    std::cout << "Generating ~100 MB of realistic RDF literal strings..." << std::endl;
    dataset_ = generateRealisticLiteralDataset(100 * 1024 * 1024);
    std::cout << "Generated " << dataset_.literals.size() << " literals ("
              << (static_cast<double>(dataset_.totalBytes) / (1024.0 * 1024.0))
              << " MB total)." << std::endl;
  }

  
    const size_t numLiterals = dataset_.literals.size();
    const double totalGB = static_cast<double>(dataset_.totalBytes) / 1e9;

    // Helper to run a measurement, time it, and populate common metadata.
    auto runMeasurement = [&](BenchmarkGroup& group,
                              std::string_view name,
                              auto&& benchmarkLambda,
                              double inputBytes = dataset_.totalBytes,
                              size_t* outCounter = nullptr) {
      auto start = std::chrono::high_resolution_clock::now();
      auto& m = group.addMeasurement(name, std::forward<decltype(benchmarkLambda)>(benchmarkLambda));
      auto end = std::chrono::high_resolution_clock::now();
      double seconds = std::chrono::duration<double>(end - start).count();
      double throughputGBs = (inputBytes / 1e9) / seconds;

      m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
      m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
      if (outCounter) {
        m.metadata().addKeyValuePair("escapes-found", *outCounter);
      }
      return m;
    };

        // _______________________________________________________________________
    // Group 1: Scanning & Classification Throughput
    // _______________________________________________________________________
    {
      auto& group = results.addGroup("1. Literal Escape Scanning Throughput (100 MB)");

      // Baseline: Scalar Turtle Scan
      {
        size_t totalEscapesFound = 0;
        runMeasurement(group, "Scalar character-by-character scan (Turtle)",
                       [&]() {
                         size_t count = 0;
                         for (const auto& lit : dataset_.literals) {
                           if (scalarFindFirstEscapeTurtle(lit) != std::string_view::npos) {
                             ++count;
                           }
                         }
                         totalEscapesFound = count;
                         return dataset_.totalBytes;
                       },
                       dataset_.totalBytes, &totalEscapesFound);
        runMeasurement(group, "SimdEscapeClassifier::findFirstEscape (Turtle AVX2)",
                       [&]() {
                         size_t count = 0;
                         for (const auto& lit : dataset_.literals) {
                           if (SimdEscapeClassifier::hasEscapes<EscapeFormat::Turtle>(lit)) {
                             ++count;
                           }
                         }
                         totalEscapesFound = count;
                         return dataset_.totalBytes;
                       },
                       dataset_.totalBytes, &totalEscapesFound);
        runMeasurement(group, "Scalar character-by-character scan (CSV)",
                       [&]() {
                         size_t count = 0;
                         for (const auto& lit : dataset_.literals) {
                           if (scalarFindFirstEscapeCsv(lit) != std::string_view::npos) {
                             ++count;
                           }
                         }
                         totalEscapesFound = count;
                         return dataset_.totalBytes;
                       },
                       dataset_.totalBytes, &totalEscapesFound);
        runMeasurement(group, "SimdEscapeClassifier::findFirstEscape (CSV AVX2)",
                       [&]() {
                         size_t count = 0;
                         for (const auto& lit : dataset_.literals) {
                           if (SimdEscapeClassifier::hasEscapes<EscapeFormat::CsvSpecial>(lit)) {
                             ++count;
                           }
                         }
                         totalEscapesFound = count;
                         return dataset_.totalBytes;
                       },
                       dataset_.totalBytes, &totalEscapesFound);
        runMeasurement(group, "Scalar character-by-character scan (TSV)",
                       [&]() {
                         size_t count = 0;
                         for (const auto& lit : dataset_.literals) {
                           if (scalarFindFirstEscapeTsv(lit) != std::string_view::npos) {
                             ++count;
                           }
                         }
                         totalEscapesFound = count;
                         return dataset_.totalBytes;
                       },
                       dataset_.totalBytes, &totalEscapesFound);
        runMeasurement(group, "SimdEscapeClassifier::findFirstEscape (TSV AVX2)",
                       [&]() {
                         size_t count = 0;
                         for (const auto& lit : dataset_.literals) {
                           if (SimdEscapeClassifier::hasEscapes<EscapeFormat::Tsv>(lit)) {
                             ++count;
                           }
                         }
                         totalEscapesFound = count;
                         return dataset_.totalBytes;
                       },
                       dataset_.totalBytes, &totalEscapesFound);
      }

      // SIMD Vector Scan (Turtle)
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
                auto& m = group.addMeasurement("SimdEscapeClassifier::hasEscapes (Turtle AVX2)", [&]() {
          size_t count = 0;
          for (const auto& lit : dataset_.literals) {
            if (SimdEscapeClassifier::hasEscapes<EscapeFormat::Turtle>(lit)) {
              ++count;
            }
          }
          totalEscapesFound = count;
          return dataset_.totalBytes;
        });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs = (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("total-literals", numLiterals);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }

      // Baseline: Scalar CSV Scan
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("Scalar character-by-character scan (CSV)", [&]() {
          size_t count = 0;
          for (const auto& lit : dataset_.literals) {
            if (scalarFindFirstEscapeCsv(lit) != std::string_view::npos) {
              ++count;
            }
          }
          totalEscapesFound = count;
          return dataset_.totalBytes;
        });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs = (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }

      // Benchmark SIMD vector scan for `CSV` format
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
                auto& m = group.addMeasurement("SimdEscapeClassifier::hasEscapes (CSV AVX2)", [&]() {
          size_t count = 0;
          for (const auto& lit : dataset_.literals) {
            if (SimdEscapeClassifier::hasEscapes<EscapeFormat::CsvSpecial>(lit)) {
              ++count;
            }
          }
          totalEscapesFound = count;
          return dataset_.totalBytes;
        });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs = (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }

      // Baseline: Scalar TSV Scan
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("Scalar character-by-character scan (TSV)", [&]() {
          size_t count = 0;
          for (const auto& lit : dataset_.literals) {
            if (scalarFindFirstEscapeTsv(lit) != std::string_view::npos) {
              ++count;
            }
          }
          totalEscapesFound = count;
          return dataset_.totalBytes;
        });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs = (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }

      // Benchmark SIMD vector scan for `TSV` format
      {
        size_t totalEscapesFound = 0;
        auto start = std::chrono::high_resolution_clock::now();
                auto& m = group.addMeasurement("SimdEscapeClassifier::hasEscapes (TSV AVX2)", [&]() {
          size_t count = 0;
          for (const auto& lit : dataset_.literals) {
            if (SimdEscapeClassifier::hasEscapes<EscapeFormat::Tsv>(lit)) {
              ++count;
            }
          }
          totalEscapesFound = count;
          return dataset_.totalBytes;
        });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs = (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-bytes-mb", totalMB);
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
        m.metadata().addKeyValuePair("escapes-found", totalEscapesFound);
      }
    }

        // _______________________________________________________________________
    // Group 2: End-to-End Literal Formatting & Escaping Throughput
    // _______________________________________________________________________
    {
      auto& group = results.addGroup("2. End-to-End Escaping & Formatting Throughput (100 MB)");

      // Baseline: Scalar StrReplaceAll Escaping (Turtle)
      {
        size_t totalOutputBytes = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("Baseline scalar StrReplaceAll (Turtle)", [&]() {
          size_t outBytes = 0;
          for (const auto& lit : dataset_.literals) {
            std::string formatted = scalarEscapeTurtleLiteral(lit);
            outBytes += formatted.size();
          }
          totalOutputBytes = outBytes;
          return totalOutputBytes;
        });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs = (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-input-mb", totalMB);
        m.metadata().addKeyValuePair("output-bytes-mb",
                                     static_cast<double>(totalOutputBytes) / (1024.0 * 1024.0));
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
      }

      // SIMD Fast-Path Branchless Copy & Escape (Turtle)
      {
        size_t totalOutputBytes = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("SimdEscapeClassifier::validRDFLiteralFromNormalized (Turtle)", [&]() {
          size_t outBytes = 0;
          for (const auto& lit : dataset_.literals) {
            std::string formatted = SimdEscapeClassifier::validRDFLiteralFromNormalized(lit);
            outBytes += formatted.size();
          }
          totalOutputBytes = outBytes;
          return totalOutputBytes;
        });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs = (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-input-mb", totalMB);
        m.metadata().addKeyValuePair("output-bytes-mb",
                                     static_cast<double>(totalOutputBytes) / (1024.0 * 1024.0));
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
      }

      // SIMD Fast-Path Direct Buffer Copying (Turtle zero temporary string allocations)
      {
        std::vector<char> outputBuffer(256 * 1024 * 1024);
        size_t totalOutputBytes = 0;
        auto start = std::chrono::high_resolution_clock::now();
        auto& m = group.addMeasurement("SimdEscapeClassifier::copyAndEscape (Direct Buffer Streaming)", [&]() {
          char* outPtr = outputBuffer.data();
          for (const auto& lit : dataset_.literals) {
            outPtr = SimdEscapeClassifier::copyAndEscape<EscapeFormat::Turtle>(lit, outPtr);
          }
          totalOutputBytes = static_cast<size_t>(outPtr - outputBuffer.data());
          return totalOutputBytes;
        });
        auto end = std::chrono::high_resolution_clock::now();
        double seconds = std::chrono::duration<double>(end - start).count();
        double throughputGBs = (static_cast<double>(dataset_.totalBytes) / 1e9) / seconds;

        m.metadata().addKeyValuePair("total-input-mb", totalMB);
        m.metadata().addKeyValuePair("output-bytes-mb",
                                     static_cast<double>(totalOutputBytes) / (1024.0 * 1024.0));
        m.metadata().addKeyValuePair("throughput-gb-s", throughputGBs);
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(LiteralEscapeBenchmark);

}  // namespace
}  // namespace ad_benchmark

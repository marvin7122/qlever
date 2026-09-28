// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Real decode path: `CompressedVocabulary<VocabularyInMemory>` with its default
// compression (`FsstSquaredCompressionWrapper`, two FSST stages), looked up
// word by word via `operator[]`, which is what the index does for every
// vocabulary word it materializes. Uses only APIs that exist before and after
// the FSST `decompressInto` change, so the same source measures both.
// Environment: VOCAB_BENCH_WORDS (default 200000), VOCAB_BENCH_REPETITIONS
// (passes over all words, default 1). One untimed warm-up pass runs first.

#include <cerrno>
#include <cstdlib>
#include <random>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "util/File.h"

namespace ad_benchmark {
namespace {

size_t parseEnvironmentSize(const char* varName, size_t defaultValue) {
  const char* value = std::getenv(varName);
  if (value == nullptr) {
    return defaultValue;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long parsed = std::strtoul(value, &end, 10);
  AD_CONTRACT_CHECK(end != value && *end == '\0' && errno != ERANGE,
                    "Invalid value `", value, "` for environment variable ",
                    varName);
  return static_cast<size_t>(parsed);
}

class CompressedVocabularyLookupBenchmark : public BenchmarkInterface {
  using Vocab = CompressedVocabulary<VocabularyInMemory>;
  Vocab vocab_;
  size_t numWords_ = 0;
  size_t totalBytes_ = 0;

 public:
  CompressedVocabularyLookupBenchmark() {
    numWords_ = parseEnvironmentSize("VOCAB_BENCH_WORDS", 200'000);
    AD_CONTRACT_CHECK(numWords_ > 0);
    // Wikidata-like mix: entity IRIs, property IRIs and language-tagged
    // labels, from a fixed seed.
    std::mt19937_64 random{42};
    std::uniform_int_distribution<uint64_t> entity{1, 130'000'000};
    std::uniform_int_distribution<size_t> labelLength{4, 40};
    const std::string letters = "abcdefghijklmnopqrstuvwxyz     ";
    std::uniform_int_distribution<size_t> letter{0, letters.size() - 1};
    std::vector<std::string> words;
    words.reserve(numWords_);
    for (size_t i = 0; i < numWords_; ++i) {
      switch (i % 3) {
        case 0:
          words.push_back("<http://www.wikidata.org/entity/Q" +
                          std::to_string(entity(random)) + ">");
          break;
        case 1:
          words.push_back("<http://www.wikidata.org/prop/direct/P" +
                          std::to_string(entity(random) % 12'000) + ">");
          break;
        default: {
          std::string label = "\"";
          const size_t length = labelLength(random);
          for (size_t c = 0; c < length; ++c) {
            label += letters[letter(random)];
          }
          label += (i % 2 == 0) ? "\"@en" : "\"@de";
          words.push_back(std::move(label));
        }
      }
      totalBytes_ += words.back().size();
    }
    const std::string filename = "compressedVocabularyLookupBenchmark.vocab";
    {
      auto writer = Vocab::makeDiskWriterPtr(filename);
      for (const auto& word : words) {
        (*writer)(word, false);
      }
      writer->finish();
    }
    vocab_.open(filename);
    ad_utility::deleteFile(filename + ".words");
    ad_utility::deleteFile(filename + ".codebooks");
    AD_CORRECTNESS_CHECK(vocab_.size() == numWords_);
    for (size_t i = 0; i < numWords_; ++i) {
      AD_CORRECTNESS_CHECK(vocab_[i] == words[i]);
    }
  }

  std::string name() const final {
    return "CompressedVocabulary operator[] lookups";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    const size_t repetitions =
        parseEnvironmentSize("VOCAB_BENCH_REPETITIONS", 1);
    AD_CONTRACT_CHECK(repetitions > 0);
    auto run = [&](size_t passes) {
      size_t bytes = 0;
      for (size_t pass = 0; pass < passes; ++pass) {
        for (size_t i = 0; i < numWords_; ++i) {
          bytes += vocab_[i].size();
        }
      }
      return bytes;
    };
    AD_CORRECTNESS_CHECK(run(1) == totalBytes_);
    auto& entry = results.addMeasurement(
        "operator[] over all words x VOCAB_BENCH_REPETITIONS",
        [&]() { return run(repetitions); });
    entry.metadata().addKeyValuePair("lookups", numWords_ * repetitions);
    entry.metadata().addKeyValuePair("decodedBytesPerPass", totalBytes_);
    return results;
  }
};

AD_REGISTER_BENCHMARK(CompressedVocabularyLookupBenchmark);

}  // namespace
}  // namespace ad_benchmark

// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Measurement-only benchmark (not part of the PR): resolve a 4,096-index batch
// against a 50,000-word `VocabularyInMemoryBinSearch`. The same source compiles
// against the old result type (`std::shared_ptr<ql::span<std::string_view>>`,
// part 4) and the new value type (part 5), so the two binaries can be compared.
// Measurements (each repeated `VOCAB_RESOLVE_AB_REPETITIONS` times):
// 1. per-word `operator[]` + owning `std::string` copy (the pre-batch path);
// 2. one `lookupBatch` call;
// 3. per-word `operator[]` into a `std::vector<std::string_view>` (no copy),
//    the lower bound for a batch lookup that returns views into the vocabulary.
// `VOCAB_RESOLVE_AB_ONLY=<1|2|3>` runs a single measurement (for perf stat).

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/File.h"

namespace ad_benchmark {
namespace {

size_t envSize(const char* name, size_t defaultValue) {
  const char* value = std::getenv(name);
  if (value == nullptr || value[0] == '\0') {
    return defaultValue;
  }
  return static_cast<size_t>(std::strtoull(value, nullptr, 10));
}

std::string makeSyntheticWord(size_t index) {
  if (index % 97 == 0) {
    return "";
  }
  static constexpr std::string_view alphabet{
      "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
  std::string suffix;
  const size_t suffixLength = 8 + (index * 13) % 64;
  for (size_t character = 0; character < suffixLength; ++character) {
    suffix += alphabet[(index * 17 + character * 31) % alphabet.size()];
  }
  if (index % 3 == 0) {
    return "http://www.wikidata.org/entity/Q" + suffix;
  } else if (index % 3 == 1) {
    return "\"literal value " + suffix + "\"@en";
  }
  return "<http://example.org/property/" + suffix + ">";
}

template <typename Range>
size_t checksum(const Range& words) {
  size_t hash = 0;
  for (std::string_view view : words) {
    hash += view.size();
    for (char c : view) {
      hash = hash * 1315423911u + static_cast<unsigned char>(c);
    }
  }
  return hash;
}

// Views of a batch result, for both the old pointer type and the new value
// type.
template <typename Result>
ql::span<const std::string_view> viewsOf(const Result& result) {
  if constexpr (requires { result->size(); }) {
    return {result->data(), result->size()};
  } else {
    return {result.data(), result.size()};
  }
}

class VocabBatchResolveAbBenchmark : public BenchmarkInterface {
 private:
  std::string filename_ = "VocabBatchResolveAbBenchmark.vocab.tmp";
  VocabularyInMemoryBinSearch vocabulary_;
  std::vector<size_t> batch_;

 public:
  VocabBatchResolveAbBenchmark() {
    constexpr size_t numWords = 50'000;
    constexpr size_t batchSize = 4'096;
    {
      VocabularyInMemoryBinSearch::WordWriter writer{filename_};
      for (size_t i = 0; i < numWords; ++i) {
        writer(makeSyntheticWord(i), i);
      }
      writer.finish();
    }
    vocabulary_.open(filename_);
    batch_.resize(batchSize);
    std::iota(batch_.begin(), batch_.end(), 0);
    std::mt19937_64 randomEngine{42};
    std::shuffle(batch_.begin(), batch_.end(), randomEngine);
    for (size_t& index : batch_) {
      index = (index * 7919) % numWords;
    }
    auto expected = vocabulary_.lookupBatch(
        ql::span<const size_t>{batch_.data(), batch_.size()});
    auto views = viewsOf(expected);
    AD_CONTRACT_CHECK(views.size() == batch_.size());
    for (size_t i = 0; i < batch_.size(); ++i) {
      AD_CONTRACT_CHECK(views[i] == vocabulary_[batch_[i]].value());
    }
  }

  ~VocabBatchResolveAbBenchmark() override {
    ad_utility::deleteFile(filename_, false);
    ad_utility::deleteFile(filename_ + ".ids", false);
  }

  std::string name() const final { return "VocabBatchResolveAb"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    auto& group = results.addGroup("Resolve 4,096 of 50,000 words");
    const size_t repetitions = envSize("VOCAB_RESOLVE_AB_REPETITIONS", 10);
    const size_t only = envSize("VOCAB_RESOLVE_AB_ONLY", 0);
    const ql::span<const size_t> batch{batch_.data(), batch_.size()};

    if (only == 0 || only == 1) {
      group.addMeasurement("1 per-word operator[] + string copy", [&] {
        size_t sum = 0;
        for (size_t r = 0; r < repetitions; ++r) {
          std::vector<std::string> copies;
          copies.reserve(batch.size());
          for (size_t index : batch) {
            copies.emplace_back(vocabulary_[index].value());
          }
          sum += checksum(copies);
        }
        return sum;
      });
    }
    if (only == 0 || only == 2) {
      group.addMeasurement("2 single lookupBatch", [&] {
        size_t sum = 0;
        for (size_t r = 0; r < repetitions; ++r) {
          auto result = vocabulary_.lookupBatch(batch);
          sum += checksum(viewsOf(result));
        }
        return sum;
      });
    }
    if (only == 0 || only == 3) {
      group.addMeasurement("3 per-word operator[] views, no copy", [&] {
        size_t sum = 0;
        for (size_t r = 0; r < repetitions; ++r) {
          std::vector<std::string_view> views;
          views.reserve(batch.size());
          for (size_t index : batch) {
            views.push_back(vocabulary_[index].value());
          }
          sum += checksum(views);
        }
        return sum;
      });
    }
    return results;
  }
};

AD_REGISTER_BENCHMARK(VocabBatchResolveAbBenchmark);

}  // namespace
}  // namespace ad_benchmark

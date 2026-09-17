// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

// Subsystem mock for the batched export materialization design (thesis run
// 133 plan). It compares three ways to turn a batch of vocabulary indices
// into escaped output bytes on a synthetic FSST-compressed vocabulary with
// holes (`CompressedVocabulary<VocabularyInMemoryBinSearch>`, the shape of
// the truthy RAM-map vocabulary that serves label exports):
//
//   sequential:  one `operator[]` per index into a fresh `std::string`, then
//                per-cell escaping. Mirrors the export row loop in
//                `ExportQueryExecutionTrees.cpp`.
//   batch:       one `lookupBatch` per batch (API shape only;
//                `sequentialLookupBatch` is a loop over `operator[]`).
//   reuse:       `lookupBatch` plus one reused scratch string and one
//                pre-sized output chunk (the Stage A candidate).
//
// All arms must produce byte-identical output (checked up front). The input
// is a Zipf-skewed index multiset with duplicates, mimicking repeated labels
// in an export response. The vocabulary is synthetic, so only the relative
// overhead (allocation, dispatch, escaping) transfers; absolute times need
// the full export A/B on Wikidata truthy with the byte-identity gate.

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/CompressionWrappers.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/Serializer/FileSerializer.h"

namespace ad_benchmark {
namespace {

using Wrapper = ad_utility::vocabulary::FsstCompressionWrapper;
using Vocab = CompressedVocabulary<VocabularyInMemoryBinSearch, Wrapper>;

// Number of synthetic words with contiguous indices (no holes in the mock,
// so no placeholders disturb the comparison).
constexpr size_t numWords = 200'000;
// Batch size in indices, matching the export stream chunk granularity.
constexpr size_t batchSize = 4096;
// Repetitions per arm.
constexpr size_t numReps = 5;
// Deterministic seed so the multiset is identical for every arm.
constexpr uint64_t seed = 132;

// Build `numWords` sorted words: alternating IRIs and English labels with
// varying lengths.
std::vector<std::string> makeWords() {
  std::vector<std::string> words;
  words.reserve(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    if (i % 2 == 0) {
      words.push_back("<http://example.org/entity/Q" + std::to_string(i) +
                      "_label_" + std::string(i % 37, 'x') + ">");
    } else {
      words.push_back("\"label number " + std::to_string(i) + " " +
                      std::string(i % 53, 'y') + "\"@en");
    }
  }
  std::sort(words.begin(), words.end());
  return words;
}

// Zipf-skewed index multiset (`skew` > 1 concentrates on low indices).
std::vector<size_t> makeMultiset(size_t vocabSize, double skew = 6.0) {
  std::mt19937_64 rng{seed + 1};
  std::uniform_real_distribution<double> uniform{0.0, 1.0};
  std::vector<size_t> indices;
  indices.reserve(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    indices.push_back(
        static_cast<size_t>(vocabSize * std::pow(uniform(rng), skew)));
  }
  return indices;
}

// Build a `CompressedVocabulary<VocabularyInMemoryBinSearch>` under
// `basename` (`basename.words`, `basename.words.ids`, `basename.codebooks`)
// by compressing all words with one FSST codebook and writing them with
// explicit contiguous indices.
Vocab buildVocab(const std::string& basename,
                 const std::vector<std::string>& words) {
  auto [buffer, compressed, decoder] = Wrapper::compressAll(words);
  {
    VocabularyInMemoryBinSearch::WordWriter writer{basename + ".words"};
    for (size_t i = 0; i < compressed.size(); ++i) {
      (void)writer(compressed[i], i);
    }
    writer.finish();
  }
  {
    ad_utility::serialization::FileWriteSerializer writer{basename +
                                                          ".codebooks"};
    std::vector<Wrapper::Decoder> decoders;
    decoders.push_back(std::move(decoder));
    writer | decoders;
    writer.close();
  }
  Vocab vocab;
  vocab.open(basename);
  return vocab;
}

// Delete the files that `buildVocab` created for `basename`.
void deleteMockVocabFiles(const std::string& basename) {
  std::error_code ec;
  for (const auto& prefix :
       {basename + ".words", basename + ".words.ids",
        basename + ".codebooks"}) {
    std::filesystem::remove(prefix, ec);
  }
}

}  // namespace

class VocabBatchExportMock : public BenchmarkInterface {
  Vocab vocab_;
  std::vector<size_t> multiset_;
  std::string chunk_;
  // Sink against dead-code elimination of the timed output.
  size_t sink_ = 0;

  [[nodiscard]] std::string name() const final {
    return "Vocabulary batch export materialization mock";
  }

  // Arm 1: sequential per-index lookup, one owning string per cell.
  void runSequential() {
    chunk_.clear();
    for (size_t idx : multiset_) {
      std::string word = vocab_[idx];
      chunk_ += RdfEscaping::escapeForCsv(std::move(word));
      chunk_ += '\n';
    }
    consume();
  }

  // Arm 2: batched lookup, fresh string per cell for escaping.
  void runBatch() {
    chunk_.clear();
    for (size_t begin = 0; begin < multiset_.size(); begin += batchSize) {
      size_t end = std::min(begin + batchSize, multiset_.size());
      auto result = vocab_.lookupBatch(
          ql::span<const size_t>{multiset_.data() + begin, end - begin});
      for (std::string_view view : *result) {
        chunk_ += RdfEscaping::escapeForCsv(std::string{view});
        chunk_ += '\n';
      }
    }
    consume();
  }

  // Arm 3 (Stage A candidate): batched lookup with a reused scratch string.
  void runReuse() {
    chunk_.clear();
    std::string scratch;
    for (size_t begin = 0; begin < multiset_.size(); begin += batchSize) {
      size_t end = std::min(begin + batchSize, multiset_.size());
      auto result = vocab_.lookupBatch(
          ql::span<const size_t>{multiset_.data() + begin, end - begin});
      for (std::string_view view : *result) {
        scratch.assign(view);
        chunk_ += RdfEscaping::escapeForCsv(std::move(scratch));
        chunk_ += '\n';
      }
    }
    consume();
  }

  void consume() {
    AD_CONTRACT_CHECK(!chunk_.empty());
    sink_ += chunk_.size() + static_cast<unsigned char>(chunk_.front());
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    // Correctness gate (untimed): all arms must agree byte for byte.
    runSequential();
    const std::string expected = chunk_;
    runBatch();
    AD_CONTRACT_CHECK(chunk_ == expected);
    runReuse();
    AD_CONTRACT_CHECK(chunk_ == expected);

    auto& group = results.addGroup("export-materialization");
    for (size_t i = 0; i < numReps; ++i) {
      group.addMeasurement("sequential/" + std::to_string(i),
                           [this]() { runSequential(); });
      group.addMeasurement("batch/" + std::to_string(i),
                           [this]() { runBatch(); });
      group.addMeasurement("reuse/" + std::to_string(i),
                           [this]() { runReuse(); });
    }
    return results;
  }

 public:
  VocabBatchExportMock() {
    const std::string basename = "vocabBatchExportMock";
    vocab_ = buildVocab(basename, makeWords());
    multiset_ = makeMultiset(vocab_.size());
    chunk_.reserve(1 << 20);
  }

  ~VocabBatchExportMock() override {
    deleteMockVocabFiles("vocabBatchExportMock");
  }
};
AD_REGISTER_BENCHMARK(VocabBatchExportMock);
}  // namespace ad_benchmark

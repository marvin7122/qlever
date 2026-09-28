// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// RESEARCH ONLY (not part of the PR): attribute the cost of
// `VocabularyOnDisk::lookupBatch` and `VocabularyInternalExternal::lookupBatch`
// on files that sit in the page cache. Every case is repeated until it has
// resolved at least `kMinWordsPerCase` words and reports ns per word, so the
// numbers are not single ~100 us samples.
//
// Cases per (vocabulary size, batch size):
//   single        : `operator[]` per word (two `pread`s + one `std::string`).
//   batch         : `lookupBatch` (the vocabulary's own io_uring managers).
//   manual-sync   : the same two-phase read as `lookupBatch` (offset pairs,
//                   then words into one `ContiguousVocabBatchBuilder`), with a
//                   `BatchManager<SyncIoPolicy>` (one `pread` per request).
//   manual-uring  : the same with a fresh `BatchManager<IoUringPolicy>`.
//   ie-probe-only : only the membership probes of
//                   `VocabularyInternalExternal::lookupBatch` (no I/O).
//   ie-single / ie-batch : `VocabularyInternalExternal` per word / batched.

#include <chrono>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/File.h"
#include "util/IoUringManager.h"

namespace ad_benchmark {
namespace {

constexpr size_t kMinWordsPerCase = 2'000'000;

std::vector<std::string> makeWords(size_t numWords) {
  std::vector<std::string> words;
  words.reserve(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    std::string word = "<http://example.org/entity/" + std::to_string(i) + ">";
    word.append(i % 53, 'x');
    words.push_back(std::move(word));
  }
  return words;
}

std::vector<size_t> makeQueryIds(size_t vocabSize, size_t numQueries,
                                 uint32_t seed) {
  std::vector<size_t> ids(vocabSize);
  std::iota(ids.begin(), ids.end(), size_t{0});
  std::shuffle(ids.begin(), ids.end(), std::mt19937{seed});
  ids.resize(numQueries);
  return ids;
}

struct Pair {
  uint64_t offset;
  uint64_t next;
};

// The two-phase read of `VocabularyOnDisk::lookupBatch`, with a given manager.
size_t manualTwoPhase(ad_utility::BatchManagerBase& manager, int offsetsFd,
                      int wordsFd, const std::vector<size_t>& ids) {
  const size_t n = ids.size();
  std::vector<Pair> pairs(n);
  std::vector<size_t> sizes(n, sizeof(Pair));
  std::vector<uint64_t> fileOffsets(n);
  std::vector<char*> targets(n);
  for (size_t i = 0; i < n; ++i) {
    fileOffsets[i] = ids[i] * sizeof(uint64_t);
    targets[i] = reinterpret_cast<char*>(&pairs[i]);
  }
  manager.wait(manager.addBatch(offsetsFd, sizes, fileOffsets, targets));
  for (size_t i = 0; i < n; ++i) {
    sizes[i] = pairs[i].next - pairs[i].offset;
    fileOffsets[i] = pairs[i].offset;
  }
  ContiguousVocabBatchBuilder builder(sizes);
  auto wordTargets = builder.targets();
  manager.wait(manager.addBatch(wordsFd, sizes, fileOffsets,
                                ql::span<char*>{wordTargets}));
  auto result = std::move(builder).finalize();
  size_t total = 0;
  for (std::string_view w : result) {
    total += w.size();
  }
  return total;
}

template <typename F>
void timeCase(BenchmarkResults& results, const std::string& group,
              const std::string& name, size_t wordsPerCall, F f,
              size_t& checksum) {
  const size_t calls = std::max<size_t>(1, kMinWordsPerCase / wordsPerCall);
  // One untimed warm-up call.
  checksum += f();
  auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < calls; ++i) {
    checksum += f();
  }
  auto ns = std::chrono::duration<double, std::nano>(
                std::chrono::steady_clock::now() - start)
                .count();
  double perWord = ns / static_cast<double>(calls * wordsPerCall);
  std::cout << "ATTR\t" << group << "\t" << name << "\t" << perWord
            << "\tns/word\t(" << calls << " calls)" << std::endl;
  results.addGroup(group + " / " + name)
      .addMeasurement("ns per word x1000", [perWord]() {
        volatile double sink = perWord;
        (void)sink;
      });
}
}  // namespace

class BMVocabBatchLookupAttribution : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "Vocabulary lookupBatch cost attribution (research)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    const char* envDir = std::getenv("ATTR_DIR");
    const auto dir = (envDir ? std::filesystem::path{envDir}
                             : std::filesystem::temp_directory_path()) /
                     "qleverVocabBatchAttr";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    size_t checksum = 0;

    for (size_t numWords : {4'096u, 200'000u}) {
      const auto words = makeWords(numWords);
      const std::string odName =
          (dir / ("od" + std::to_string(numWords))).string();
      const std::string ieName =
          (dir / ("ie" + std::to_string(numWords))).string();
      {
        VocabularyOnDisk::WordWriter w(odName);
        for (const auto& word : words) {
          w(word, false);
        }
        w.finish();
      }
      {
        auto w = VocabularyInternalExternal::makeDiskWriterPtr(ieName);
        for (size_t i = 0; i < words.size(); ++i) {
          (*w)(words[i], i % 2 == 0);
        }
        w->finish();
      }
      VocabularyOnDisk od;
      od.open(odName);
      VocabularyInternalExternal ie;
      ie.open(ieName);
      ad_utility::File wordsFile(odName, "r");
      ad_utility::File offsetsFile(odName + ".offsets", "r");
      ad_utility::BatchManager<ad_utility::SyncIoPolicy> syncManager;
#ifdef QLEVER_HAS_IO_URING
      ad_utility::BatchManager<ad_utility::IoUringPolicy> uringManager{256};
#endif

      for (size_t batch : {1u, 4u, 16u, 128u, 2'048u, 50'000u}) {
        if (batch > numWords) {
          continue;
        }
        const auto ids = makeQueryIds(numWords, batch, 42);
        const std::string g = "words=" + std::to_string(numWords) +
                              " batch=" + std::to_string(batch);
        timeCase(
            results, g, "od-single", batch,
            [&]() {
              size_t t = 0;
              for (size_t i : ids) t += od[i].size();
              return t;
            },
            checksum);
        timeCase(
            results, g, "od-batch", batch,
            [&]() {
              size_t t = 0;
              for (std::string_view w : od.lookupBatch(ids)) t += w.size();
              return t;
            },
            checksum);
        timeCase(
            results, g, "od-manual-sync", batch,
            [&]() {
              return manualTwoPhase(syncManager, offsetsFile.fd(),
                                    wordsFile.fd(), ids);
            },
            checksum);
#ifdef QLEVER_HAS_IO_URING
        timeCase(
            results, g, "od-manual-uring", batch,
            [&]() {
              return manualTwoPhase(uringManager, offsetsFile.fd(),
                                    wordsFile.fd(), ids);
            },
            checksum);
#endif
        timeCase(
            results, g, "ie-probe-only", batch,
            [&]() {
              size_t t = 0;
              const auto& in = ie.internalVocab();
              const uint64_t end = in.endIndex();
              for (size_t i : ids) {
                auto w = i < end ? in[i] : std::optional<std::string_view>{};
                t += w.has_value() ? w->size() : 1;
              }
              return t;
            },
            checksum);
        timeCase(
            results, g, "ie-single", batch,
            [&]() {
              size_t t = 0;
              for (size_t i : ids) t += ie[i].size();
              return t;
            },
            checksum);
        timeCase(
            results, g, "ie-batch", batch,
            [&]() {
              size_t t = 0;
              for (std::string_view w : ie.lookupBatch(ids)) t += w.size();
              return t;
            },
            checksum);
      }
    }
    std::cout << "attribution checksum: " << checksum << '\n';
    std::filesystem::remove_all(dir);
    return results;
  }
};

AD_REGISTER_BENCHMARK(BMVocabBatchLookupAttribution);
}  // namespace ad_benchmark

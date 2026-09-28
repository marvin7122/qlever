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
// timed in long trials (see `runGroup`) and reports ns per word, so the
// numbers are not single ~100 us samples. Each timed trial runs >= 1 s,
// with >= 10 interleaved trials after an untimed warm-up (median, min..max).
// Run pinned to one core (`taskset -c <n>`).
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

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
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

// A registered case of the current group; all cases of a group are run as
// interleaved trials by `runGroup`.
struct Case {
  std::string group;
  std::string name;
  size_t wordsPerCall;
  std::function<size_t()> f;
  size_t calls = 1;
  std::vector<double> nsPerWord;
};
std::vector<Case> pendingCases;

// Environment knobs: `ATTR_TRIALS` (default 11) and `ATTR_MIN_SECONDS`
// (default 1.0, the minimum duration of one timed trial).
size_t envSize(const char* name, size_t dflt) {
  const char* v = std::getenv(name);
  return v ? std::stoul(v) : dflt;
}
double envDouble(const char* name, double dflt) {
  const char* v = std::getenv(name);
  return v ? std::stod(v) : dflt;
}

template <typename F>
void timeCase(BenchmarkResults&, const std::string& group,
              const std::string& name, size_t wordsPerCall, F f, size_t&) {
  pendingCases.push_back(Case{group, name, wordsPerCall, std::move(f)});
}

// Run all pending cases: one untimed warm-up and calibration per case (so
// that one timed trial takes at least `ATTR_MIN_SECONDS`), then `ATTR_TRIALS`
// interleaved trials (case 1, case 2, ..., case 1, case 2, ...). Report the
// median and min..max of ns per word.
void runGroup(BenchmarkResults& results, size_t& checksum) {
  using Clock = std::chrono::steady_clock;
  const size_t trials = envSize("ATTR_TRIALS", 11);
  const double minSeconds = envDouble("ATTR_MIN_SECONDS", 1.0);
  for (auto& c : pendingCases) {
    // Warm-up and calibration: double the number of calls until one round
    // takes at least 1/8 of the target, then scale.
    size_t calls = 1;
    while (true) {
      auto start = Clock::now();
      for (size_t i = 0; i < calls; ++i) checksum += c.f();
      double sec = std::chrono::duration<double>(Clock::now() - start).count();
      if (sec >= minSeconds / 8) {
        c.calls = std::max<size_t>(
            1, static_cast<size_t>(calls * (minSeconds * 1.05) / sec) + 1);
        break;
      }
      calls *= 2;
    }
  }
  for (size_t t = 0; t < trials; ++t) {
    for (auto& c : pendingCases) {
      auto start = Clock::now();
      for (size_t i = 0; i < c.calls; ++i) checksum += c.f();
      double ns = std::chrono::duration<double, std::nano>(Clock::now() - start)
                      .count();
      c.nsPerWord.push_back(ns / static_cast<double>(c.calls * c.wordsPerCall));
    }
  }
  for (auto& c : pendingCases) {
    auto v = c.nsPerWord;
    std::sort(v.begin(), v.end());
    double median = v[v.size() / 2];
    std::cout << "ATTR\t" << c.group << "\t" << c.name << "\tmedian\t" << median
              << "\tmin\t" << v.front() << "\tmax\t" << v.back()
              << "\tns/word\ttrials\t" << v.size() << "\tcalls/trial\t"
              << c.calls << "\twords/trial\t" << c.calls * c.wordsPerCall
              << std::endl;
    results.addGroup(c.group + " / " + c.name)
        .addMeasurement("median ns per word (value in stdout)", [median]() {
          volatile double sink = median;
          (void)sink;
        });
  }
  pendingCases.clear();
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

      for (size_t batch : {1u, 16u, 128u, 2'048u, 50'000u}) {
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
        runGroup(results, checksum);
      }
    }
    std::cout << "attribution checksum: " << checksum << '\n';
    std::filesystem::remove_all(dir);
    return results;
  }
};

AD_REGISTER_BENCHMARK(BMVocabBatchLookupAttribution);
}  // namespace ad_benchmark

// Micro-benchmark for `SplitVocabulary::lookupBatch` (not for merging).
//
// Builds the vocabulary type of `on-disk-compressed-geo-split`
// (`SplitGeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>`) on
// disk and compares, in one process, two arms on the same batches:
//   per-word: `sequentialLookupBatch` (one `operator[]` per index, the code
//             path of `SplitVocabulary::lookupBatch` on master),
//   batched:  `SplitVocabulary::lookupBatch` of this branch.
// Index patterns, batches of 1024 sorted indices (the CONSTRUCT batch size):
//   sequential: a dense window, every index taken with probability 1/4,
//   scattered:  a uniform sample over all marker-0 words,
//   mixed:      a uniform sample over all words of both markers.
// Each trial runs all batches of one pattern once per arm; the arms alternate
// within a trial, their order alternates between trials. "cold" evicts the
// files of the vocabulary from the page cache (`POSIX_FADV_DONTNEED`) before
// each arm, "warm" does not. Output: ns per looked-up word per trial and arm,
// plus a checksum of the looked-up bytes (must be equal between the arms).
//
// Usage: SplitVocabularyLookupBatchMicroBenchmark <dir> [numWords] [numTrials]

#include <absl/strings/str_cat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

#include "global/Constants.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/SplitVocabularyImpl.h"
#include "index/vocabulary/VocabularyInternalExternal.h"

namespace {

using SGV =
    SplitGeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>;
constexpr size_t BATCH_SIZE = 1024;
constexpr size_t NUM_BATCHES = 400;

// Evict all files in `dir` from the page cache.
void evict(const std::string& dir) {
  ::sync();
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    int fd = ::open(entry.path().c_str(), O_RDONLY);
    if (fd >= 0) {
      ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
      ::close(fd);
    }
  }
}

// A label-like literal, sorted by `i` (fixed width).
std::string label(size_t i) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "\"Mensch Nummer %09zu\"@de", i);
  return buf;
}

std::string wkt(size_t i) {
  return absl::StrCat("\"POINT(", i % 360, " ", i % 180, ".", i, ")\"",
                      GEO_LITERAL_SUFFIX);
}

using Batches = std::vector<std::vector<size_t>>;

Batches makeBatches(std::string_view pattern, size_t numPlain, size_t numGeo,
                    std::mt19937_64& rng) {
  Batches batches;
  for (size_t b = 0; b < NUM_BATCHES; ++b) {
    std::vector<size_t> batch;
    if (pattern == "sequential") {
      std::uniform_int_distribution<size_t> start(0, numPlain - 4 * BATCH_SIZE);
      std::bernoulli_distribution take(0.25);
      for (size_t i = start(rng); batch.size() < BATCH_SIZE; ++i) {
        if (take(rng)) batch.push_back(i);
      }
    } else if (pattern == "scattered") {
      std::uniform_int_distribution<size_t> any(0, numPlain - 1);
      while (batch.size() < BATCH_SIZE) batch.push_back(any(rng));
    } else {
      std::uniform_int_distribution<size_t> any(0, numPlain + numGeo - 1);
      while (batch.size() < BATCH_SIZE) {
        size_t i = any(rng);
        batch.push_back(i < numPlain ? i : SGV::addMarker(i - numPlain, 1));
      }
    }
    std::sort(batch.begin(), batch.end());
    batches.push_back(std::move(batch));
  }
  return batches;
}

// Run all `batches` with `lookup`, return {ns per word, checksum}.
template <typename Lookup>
std::pair<double, uint64_t> run(const Batches& batches, Lookup lookup) {
  uint64_t checksum = 0;
  size_t numWords = 0;
  auto start = std::chrono::steady_clock::now();
  for (const auto& batch : batches) {
    auto result = lookup(batch);
    for (std::string_view word : *result) {
      checksum = checksum * 1099511628211ULL + word.size();
      for (char c : word) checksum = checksum * 31 + static_cast<uint8_t>(c);
    }
    numWords += batch.size();
  }
  auto ns = std::chrono::duration<double, std::nano>(
                std::chrono::steady_clock::now() - start)
                .count();
  return {ns / static_cast<double>(numWords), checksum};
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s <dir> [numWords] [numTrials]\n", argv[0]);
    return 2;
  }
  const std::string dir = argv[1];
  const size_t numWords = argc > 2 ? std::stoul(argv[2]) : 4'000'000;
  const size_t numTrials = argc > 3 ? std::stoul(argv[3]) : 5;
  const size_t numGeo = numWords / 100;
  const size_t numPlain = numWords - numGeo;
  std::filesystem::create_directories(dir);
  const std::string filename = dir + "/vocab";

  {
    SGV writeVocab;
    auto ww = writeVocab.makeDiskWriterPtr(filename);
    ww->readableName() = "bench";
    for (size_t i = 0; i < numPlain; ++i) (*ww)(label(i), true);
    for (size_t i = 0; i < numGeo; ++i) (*ww)(wkt(i), true);
    ww->finish();
  }
  SGV vocab;
  vocab.open(filename);
  std::printf("words=%zu plain=%zu geo=%zu batch=%zu batches=%zu trials=%zu\n",
              numWords, numPlain, numGeo, BATCH_SIZE, NUM_BATCHES, numTrials);

  auto perWord = [&vocab](const std::vector<size_t>& batch) {
    return ad_utility::vocabulary::sequentialLookupBatch(vocab, batch);
  };
  auto batched = [&vocab](const std::vector<size_t>& batch) {
    return vocab.lookupBatch(batch);
  };

  std::mt19937_64 rng{42};
  int rc = 0;
  for (std::string_view pattern : {"sequential", "scattered", "mixed"}) {
    const Batches batches = makeBatches(pattern, numPlain, numGeo, rng);
    for (bool cold : {true, false}) {
      if (!cold) run(batches, batched);  // Warm up the page cache.
      for (size_t trial = 0; trial < numTrials; ++trial) {
        for (size_t k = 0; k < 2; ++k) {
          const bool batchedArm = (k + trial) % 2 == 0;
          if (cold) evict(dir);
          auto [nsPerWord, checksum] =
              batchedArm ? run(batches, batched) : run(batches, perWord);
          std::printf(
              "RESULT pattern=%.*s cache=%s trial=%zu arm=%s "
              "ns_per_word=%.2f checksum=%016llx\n",
              static_cast<int>(pattern.size()), pattern.data(),
              cold ? "cold" : "warm", trial,
              batchedArm ? "batched" : "per-word", nsPerWord,
              static_cast<unsigned long long>(checksum));
          std::fflush(stdout);
        }
      }
    }
  }
  return rc;
}

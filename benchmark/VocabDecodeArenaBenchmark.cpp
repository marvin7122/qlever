// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Isolate the FSST decode-into-arena path from CONSTRUCT export so its effect
// is measurable on its own. No Turtle output and no HTTP traffic are involved.
// Layer 0 decodes RAM-resident compressed words with two arms:
// `decode-then-copy` (decompress into a temporary, then copy into the arena)
// and `into` (decode directly into arena memory). The speedup of the arena
// path is the `copy` arm time divided by the `into` arm time for the same
// layer. Layer 1 times `CompressedVocabulary::lookupBatch` end to end after a
// warmup pass, so it includes the lookup cost and is not comparable to
// layer 0. The `--arm` flag is ignored for layer 1.

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/memory_resource.h"
#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Algorithm.h"
#include "util/Exception.h"

using Vocab = CompressedVocabulary<VocabularyInternalExternal>;

namespace {

//____________________________________________________________________________
struct Options {
  std::string vocabPrefix;
  std::string arm{"copy"};
  int layer = 0;
  size_t batchSize = 1024;
  size_t warmupBatches = 100;
  size_t timedBatches = 10000;
  size_t startIndex = 0;
};

//____________________________________________________________________________
void usage(const char* argv0) {
  std::cerr
      << "Usage: " << argv0
      << " --vocab <prefix> --arm copy|into --layer 0|1"
         " [--batch-size 1024] [--warmup-batches 100] [--timed-batches 10000]"
         " [--start-index 0]\n"
         "  prefix is the path passed to CompressedVocabulary::open\n"
         "  (Wikidata: .../wikidata.vocabulary).\n";
}

//____________________________________________________________________________
// Parse `text` as the integer CLI value for `name`. Set `failed` and return 0
// when the text is not a valid integer.
int parseCliInt(const std::string& text, const char* name, bool& failed) {
  try {
    return std::stoi(text);
  } catch (const std::exception&) {
    std::cerr << "invalid value for " << name << ": '" << text << "'\n";
    failed = true;
    return 0;
  }
}

//____________________________________________________________________________
// Parse `text` as the non-negative integer CLI value for `name`. Set `failed`
// and return 0 when the text is not a valid integer.
size_t parseCliSize(const std::string& text, const char* name, bool& failed) {
  try {
    return static_cast<size_t>(std::stoull(text));
  } catch (const std::exception&) {
    std::cerr << "invalid value for " << name << ": '" << text << "'\n";
    failed = true;
    return 0;
  }
}

//____________________________________________________________________________
// Parse the command line into `options`. Return true when parsing succeeded
// and the program should continue; return false when help was printed or the
// arguments are invalid (usage was already printed in that case).
bool parseArgs(int argc, char** argv, Options& options) {
  AD_CONTRACT_CHECK(argc >= 1);
  AD_CONTRACT_CHECK(argv != nullptr);
  bool failed = false;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    // Read the value following a `--flag` argument. Set `failed` and return
    // an empty string when no value is present; usage is reported once after
    // the loop so the error is not printed twice.
    auto need = [&](const char* name) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << name << "\n";
        failed = true;
        return {};
      }
      return argv[++i];
    };
    if (arg == "--vocab") {
      options.vocabPrefix = need("--vocab");
    } else if (arg == "--arm") {
      options.arm = need("--arm");
    } else if (arg == "--layer") {
      options.layer = parseCliInt(need("--layer"), "--layer", failed);
    } else if (arg == "--batch-size") {
      options.batchSize =
          parseCliSize(need("--batch-size"), "--batch-size", failed);
    } else if (arg == "--warmup-batches") {
      options.warmupBatches =
          parseCliSize(need("--warmup-batches"), "--warmup-batches", failed);
    } else if (arg == "--timed-batches") {
      options.timedBatches =
          parseCliSize(need("--timed-batches"), "--timed-batches", failed);
    } else if (arg == "--start-index") {
      options.startIndex =
          parseCliSize(need("--start-index"), "--start-index", failed);
    } else if (arg == "-h" || arg == "--help") {
      usage(argv[0]);
      return false;
    } else {
      std::cerr << "unknown argument: " << arg << "\n";
      usage(argv[0]);
      return false;
    }
  }
  if (failed) {
    usage(argv[0]);
    return false;
  }
  // Sanity caps: larger values only exhaust memory when building the index
  // batches and the layer 0 fixture, so reject them with a usage error.
  static constexpr size_t kMaxBatchSize = 1'000'000;
  static constexpr size_t kMaxBatches = 1'000'000;
  static constexpr size_t kMaxTotalWords = 100'000'000;
  const size_t totalBatches = options.warmupBatches + options.timedBatches;
  if (options.vocabPrefix.empty() ||
      (options.arm != "copy" && options.arm != "into") ||
      (options.layer != 0 && options.layer != 1) || options.batchSize == 0 ||
      options.batchSize > kMaxBatchSize ||
      options.warmupBatches > kMaxBatches || options.timedBatches == 0 ||
      options.timedBatches > kMaxBatches ||
      totalBatches * options.batchSize > kMaxTotalWords) {
    usage(argv[0]);
    return false;
  }
  return true;
}

//____________________________________________________________________________
struct FixtureWord {
  std::string compressed;
  size_t decoderIndex;
};

//____________________________________________________________________________
// Return the checksum updated by mixing in the size and the first and last
// byte of the decoded word.
uint64_t mixView(uint64_t checksum, std::string_view view) {
  checksum = checksum * 1315423911ull + view.size();
  if (!view.empty()) {
    checksum += static_cast<unsigned char>(view.front());
    checksum += static_cast<unsigned char>(view.back()) << 8;
  }
  return checksum;
}

//____________________________________________________________________________
struct RusageSnap {
  rusage ru;
};

//____________________________________________________________________________
// Return the current resource usage snapshot of the calling process.
RusageSnap snapUsage() {
  RusageSnap snapshot{};
  AD_CORRECTNESS_CHECK(getrusage(RUSAGE_SELF, &snapshot.ru) == 0);
  return snapshot;
}

//____________________________________________________________________________
// Return the timeval converted to seconds as a double.
double sec(const timeval& t) {
  return static_cast<double>(t.tv_sec) +
         static_cast<double>(t.tv_usec) / 1'000'000.0;
}

//____________________________________________________________________________
// Advise the kernel that the vocabulary words file will be needed soon.
void adviseWillNeed(const std::string& path) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return;
  }
  struct stat st {};
  if (fstat(fd, &st) == 0 && st.st_size > 0) {
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_WILLNEED);
  }
  ::close(fd);
}

//____________________________________________________________________________
struct DecodeStats {
  uint64_t checksum = 0;
  uint64_t bytesUsed = 0;
  uint64_t bytesBound = 0;
};

//____________________________________________________________________________
// Decode all words of one batch into arena-backed views. The shared driver
// owns the checksum mixing and the used-vs-bound accounting; each arm only
// supplies the per-word decode step. A missing return value marks a zero-size
// word, which contributes an empty view and no checksum input.
template <typename DecodeOp>
DecodeStats runDecodeBatch(ql::span<const FixtureWord> words,
                           ql::pmr::monotonic_buffer_resource& arena,
                           DecodeOp decodeOne) {
  AD_CONTRACT_CHECK(!words.empty());
  DecodeStats stats;
  ql::pmr::polymorphic_allocator<char> allocator{&arena};
  for (const auto& word : words) {
    if (std::optional<std::string_view> view =
            decodeOne(allocator, stats, word);
        view.has_value()) {
      stats.checksum = mixView(stats.checksum, *view);
    }
  }
  AD_CORRECTNESS_CHECK(stats.bytesUsed <= stats.bytesBound);
  volatile uint64_t keep = stats.checksum;
  (void)keep;
  return stats;
}

//____________________________________________________________________________
// Decode one batch by decompressing each word into a temporary and copying
// the result into the arena. Return the aggregate checksum and byte counts.
DecodeStats runDecodeCopyBatch(const Vocab& vocab,
                               ql::span<const FixtureWord> words,
                               ql::pmr::monotonic_buffer_resource& arena) {
  const auto& wrapper = vocab.compressionWrapper();
  return runDecodeBatch(
      words, arena,
      [&](ql::pmr::polymorphic_allocator<char>& allocator, DecodeStats& stats,
          const FixtureWord& word) -> std::optional<std::string_view> {
        std::string decompressed =
            wrapper.decompress(word.compressed, word.decoderIndex);
        const size_t size = decompressed.size();
        stats.bytesUsed += size;
        stats.bytesBound +=
            wrapper.maxDecompressedSize(word.compressed, word.decoderIndex);
        if (size == 0) {
          return std::nullopt;
        }
        char* mem = allocator.allocate(size);
        ql::ranges::copy(decompressed, mem);
        return std::string_view{mem, size};
      });
}

//____________________________________________________________________________
// Decode one batch directly into arena memory at the size bound. Return the
// aggregate checksum and byte counts.
DecodeStats runDecodeIntoBatch(const Vocab& vocab,
                               ql::span<const FixtureWord> words,
                               ql::pmr::monotonic_buffer_resource& arena) {
  const auto& wrapper = vocab.compressionWrapper();
  // Workspace for multi-stage FSST decoders; single-stage decoders ignore it.
  // Reuse across words is safe: decoders only use it as temporary workspace
  // and grow it as needed (same pattern as `CompressedVocabulary::lookupBatch`
  // on this branch).
  std::string scratch;
  return runDecodeBatch(
      words, arena,
      [&](ql::pmr::polymorphic_allocator<char>& allocator, DecodeStats& stats,
          const FixtureWord& word) -> std::optional<std::string_view> {
        const size_t bound =
            wrapper.maxDecompressedSize(word.compressed, word.decoderIndex);
        stats.bytesBound += bound;
        if (bound == 0) {
          return std::nullopt;
        }
        char* mem = allocator.allocate(bound);
        const size_t size =
            wrapper.decompressInto(word.compressed, word.decoderIndex,
                                   ql::span<char>{mem, bound}, scratch);
        AD_CORRECTNESS_CHECK(size <= bound);
        stats.bytesUsed += size;
        return std::string_view{mem, size};
      });
}

//____________________________________________________________________________
// Look up one batch of word indices end to end. Return the aggregate checksum
// and byte counts.
DecodeStats runLookupBatch(const Vocab& vocab, ql::span<const size_t> indices) {
  AD_CONTRACT_CHECK(!indices.empty());
  DecodeStats stats;
  auto result = vocab.lookupBatch(indices);
  AD_CORRECTNESS_CHECK(result != nullptr);
  for (const auto& view : *result) {
    stats.bytesUsed += view.size();
    stats.checksum = mixView(stats.checksum, view);
  }
  volatile uint64_t keep = stats.checksum;
  (void)keep;
  return stats;
}

//____________________________________________________________________________
// Return `batch` word indices starting at `start`, wrapping at `vocabSize`.
std::vector<size_t> makeIndices(size_t start, size_t batch, size_t vocabSize) {
  std::vector<size_t> indices;
  indices.reserve(batch);
  for (size_t i = 0; i < batch; ++i) {
    indices.push_back((start + i) % vocabSize);
  }
  return indices;
}

//____________________________________________________________________________
// Build the per-batch word index lists for the warmup and timed batches.
std::vector<std::vector<size_t>> buildIndexBatches(const Options& options,
                                                   size_t vocabSize) {
  const size_t totalBatches = options.warmupBatches + options.timedBatches;
  std::vector<std::vector<size_t>> indexBatches;
  indexBatches.reserve(totalBatches);
  // Reduce the start index first so an arbitrarily large `--start-index`
  // cannot overflow the per-batch offset arithmetic below.
  const size_t startOffset = options.startIndex % vocabSize;
  for (size_t b = 0; b < totalBatches; ++b) {
    const size_t start = (startOffset + b * options.batchSize) % vocabSize;
    indexBatches.push_back(makeIndices(start, options.batchSize, vocabSize));
  }
  return indexBatches;
}

//____________________________________________________________________________
// Build the RAM-resident fixture of compressed words and decoder indices for
// the layer 0 arms. The lookup of compressed words happens once outside the
// timed section so layer 0 isolates the decompression and arena operations.
std::vector<std::vector<FixtureWord>> buildLayerZeroFixture(
    const Vocab& vocab, const std::vector<std::vector<size_t>>& indexBatches,
    size_t batchSize) {
  std::vector<std::vector<FixtureWord>> fixtures(indexBatches.size());
  for (size_t b = 0; b < indexBatches.size(); ++b) {
    auto compressed = vocab.lookupCompressedBatch(indexBatches[b]);
    AD_CORRECTNESS_CHECK(compressed != nullptr);
    AD_CORRECTNESS_CHECK(compressed->size() == batchSize);
    fixtures[b].reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
      FixtureWord word;
      word.compressed = std::string{(*compressed)[i]};
      word.decoderIndex = vocab.decoderIndex(indexBatches[b][i]);
      fixtures[b].push_back(std::move(word));
    }
  }
  return fixtures;
}

}  // namespace

//____________________________________________________________________________
int main(int argc, char** argv) {
  Options options;
  if (!parseArgs(argc, argv, options)) {
    return 2;
  }

  Vocab vocab;
  // `open` returns void and throws on failure, so a failed open never reaches
  // the size check below.
  vocab.open(options.vocabPrefix);
  const size_t vocabSize = vocab.size();
  AD_CONTRACT_CHECK(vocabSize >= options.batchSize);

  adviseWillNeed(options.vocabPrefix + ".words");

  std::vector<std::vector<size_t>> indexBatches =
      buildIndexBatches(options, vocabSize);

  // The arena is reused across all timed batches and released between them,
  // so the measurement reflects decode plus arena fill rather than arena
  // lifetime.
  ql::pmr::monotonic_buffer_resource arena;

  if (options.layer == 0) {
    std::vector<std::vector<FixtureWord>> fixtures =
        buildLayerZeroFixture(vocab, indexBatches, options.batchSize);
    for (size_t b = 0; b < options.warmupBatches; ++b) {
      if (options.arm == "copy") {
        (void)runDecodeCopyBatch(vocab, fixtures[b], arena);
      } else {
        (void)runDecodeIntoBatch(vocab, fixtures[b], arena);
      }
      arena.release();
    }
    const auto ru0 = snapUsage();
    const auto t0 = std::chrono::steady_clock::now();
    DecodeStats agg;
    for (size_t b = 0; b < options.timedBatches; ++b) {
      const size_t i = options.warmupBatches + b;
      DecodeStats one = (options.arm == "copy")
                            ? runDecodeCopyBatch(vocab, fixtures[i], arena)
                            : runDecodeIntoBatch(vocab, fixtures[i], arena);
      arena.release();
      agg.checksum ^= one.checksum;
      agg.bytesUsed += one.bytesUsed;
      agg.bytesBound += one.bytesBound;
    }
    const auto t1 = std::chrono::steady_clock::now();
    const auto ru1 = snapUsage();

    const double wall = std::chrono::duration<double>(t1 - t0).count();
    const double utime = sec(ru1.ru.ru_utime) - sec(ru0.ru.ru_utime);
    const double stime = sec(ru1.ru.ru_stime) - sec(ru0.ru.ru_stime);
    const long minflt = ru1.ru.ru_minflt - ru0.ru.ru_minflt;
    const long majflt = ru1.ru.ru_majflt - ru0.ru.ru_majflt;
    const uint64_t nWords = options.timedBatches * options.batchSize;
    const double nsPerWord = wall * 1e9 / static_cast<double>(nWords);
    const double usedBound = agg.bytesBound == 0
                                 ? 0.0
                                 : static_cast<double>(agg.bytesUsed) /
                                       static_cast<double>(agg.bytesBound);

    std::cout << "arm\tlayer\twall_s\tutime_s\tstime_s\tminflt\tmajflt\t"
                 "ns_per_word\tbytes_used\tbytes_bound\tused_over_bound\t"
                 "checksum\twords\n";
    std::cout << options.arm << '\t' << options.layer << '\t' << wall << '\t'
              << utime << '\t' << stime << '\t' << minflt << '\t' << majflt
              << '\t' << nsPerWord << '\t' << agg.bytesUsed << '\t'
              << agg.bytesBound << '\t' << usedBound << '\t' << agg.checksum
              << '\t' << nWords << '\n';
    return 0;
  }

  for (size_t b = 0; b < options.warmupBatches; ++b) {
    (void)runLookupBatch(vocab, indexBatches[b]);
  }
  const auto ru0 = snapUsage();
  const auto t0 = std::chrono::steady_clock::now();
  DecodeStats agg;
  for (size_t b = 0; b < options.timedBatches; ++b) {
    const size_t i = options.warmupBatches + b;
    DecodeStats one = runLookupBatch(vocab, indexBatches[i]);
    agg.checksum ^= one.checksum;
    agg.bytesUsed += one.bytesUsed;
  }
  const auto t1 = std::chrono::steady_clock::now();
  const auto ru1 = snapUsage();

  const double wall = std::chrono::duration<double>(t1 - t0).count();
  const double utime = sec(ru1.ru.ru_utime) - sec(ru0.ru.ru_utime);
  const double stime = sec(ru1.ru.ru_stime) - sec(ru0.ru.ru_stime);
  const long minflt = ru1.ru.ru_minflt - ru0.ru.ru_minflt;
  const long majflt = ru1.ru.ru_majflt - ru0.ru.ru_majflt;
  const uint64_t nWords = options.timedBatches * options.batchSize;
  const double nsPerWord = wall * 1e9 / static_cast<double>(nWords);

  std::cout << "arm\tlayer\twall_s\tutime_s\tstime_s\tminflt\tmajflt\t"
               "ns_per_word\tbytes_used\tbytes_bound\tused_over_bound\t"
               "checksum\twords\n";
  std::cout << "lookupBatch\t" << options.layer << '\t' << wall << '\t' << utime
            << '\t' << stime << '\t' << minflt << '\t' << majflt << '\t'
            << nsPerWord << '\t' << agg.bytesUsed << '\t' << agg.bytesBound
            << '\t' << 0.0 << '\t' << agg.checksum << '\t' << nWords << '\n';
  return 0;
}

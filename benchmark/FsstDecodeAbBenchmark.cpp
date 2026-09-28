// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Bench-only A/B source for fork #73 / ad-freiburg#3522 (FSST
// `decompressInto`). The same file is compiled against the base (part 2,
// without `decompressInto`) and against part 3. Workload: 40,000
// Wikidata-like words with 180-character suffixes, compressed through
// FSST_AB_STAGES (1 or 3) FSST stages, decoded FSST_AB_REPETITIONS times
// each after one untimed warm-up pass. FSST_AB_MODE selects the decode call:
//   owning: `decompress`, one owning `std::string` per word (and per stage),
//           the only API in the base;
//   into:   `decompressInto` into one reused output buffer (and, for three
//           stages, one reused scratch string); aborts in a binary without
//           that API.
// jemalloc counters (if jemalloc is linked) are read outside the timed
// region.

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "util/FsstCompressor.h"

namespace ad_benchmark {
namespace {

// _____________________________________________________________________________
// Allocation statistics via jemalloc's `mallctl`, declared weak so that the
// benchmark also links without jemalloc (then nothing is recorded).
extern "C" int mallctl(const char* name, void* oldp, size_t* oldlenp,
                       void* newp, size_t newlen) __attribute__((weak));

struct JemallocThreadStats {
  static bool available() { return mallctl != nullptr; }
  static uint64_t readUint64(const char* name) {
    uint64_t value = 0;
    size_t length = sizeof(value);
    return mallctl(name, &value, &length, nullptr, 0) == 0 ? value : 0;
  }
  // Number of allocation requests (all arenas) and bytes allocated by this
  // thread.
  static std::pair<uint64_t, uint64_t> read() {
    if (!available()) {
      return {0, 0};
    }
    mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    uint64_t epoch = 1;
    size_t length = sizeof(epoch);
    mallctl("epoch", &epoch, &length, &epoch, length);
    return {readUint64("stats.arenas.4096.small.nrequests") +
                readUint64("stats.arenas.4096.large.nrequests"),
            readUint64("thread.allocated")};
  }
};

// _____________________________________________________________________________
template <typename D>
constexpr bool hasDecompressInto =
    requires(const D& d, std::string_view s, ql::span<char> o) {
      d.decompressInto(s, o);
    };

// _____________________________________________________________________________
// The `into` decode function. A template, so that the branch without
// `decompressInto` is discarded when it does not exist.
template <typename Single, typename Repeated>
std::function<size_t(std::string_view)> makeIntoDecoder(
    const Single& single, const Repeated& repeated, size_t stages,
    const std::vector<std::string_view>& compressed, std::string& output,
    std::string& scratch) {
  if constexpr (hasDecompressInto<Single>) {
    size_t bound = 0;
    for (auto c : compressed) {
      bound = std::max(bound, stages == 1 ? Single::maxDecompressedSize(c)
                                          : Repeated::maxDecompressedSize(c));
    }
    output.assign(bound, '\0');
    const ql::span<char> out{output.data(), output.size()};
    return [&single, &repeated, &scratch, stages,
            out](std::string_view c) -> size_t {
      return stages == 1 ? single.decompressInto(c, out)
                         : repeated.decompressInto(c, out, scratch);
    };
  } else {
    AD_FAIL();
  }
}

// _____________________________________________________________________________
size_t parseEnvironmentSize(const char* varName, size_t defaultValue) {
  const char* value = std::getenv(varName);
  if (value == nullptr) {
    return defaultValue;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long parsed = std::strtoul(value, &end, 10);
  AD_CONTRACT_CHECK(end != value && *end == '\0' && errno != ERANGE,
                    "Invalid value `", value, "` for ", varName);
  return static_cast<size_t>(parsed);
}

class FsstDecodeAbBenchmark : public BenchmarkInterface {
  static constexpr size_t numberOfWords = 40'000;
  std::vector<std::string> words_;
  std::vector<std::shared_ptr<std::string>> storage_;
  std::vector<std::string_view> compressed_;
  std::array<FsstDecoder, 3> decoders_;
  size_t stages_ = 3;
  size_t totalBytes_ = 0;

 public:
  FsstDecodeAbBenchmark() {
    stages_ = parseEnvironmentSize("FSST_AB_STAGES", 3);
    AD_CONTRACT_CHECK(stages_ == 1 || stages_ == 3);
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    words_.reserve(numberOfWords);
    for (size_t i = 0; i < numberOfWords; ++i) {
      std::string word = "http://www.wikidata.org/entity/Q";
      for (size_t c = 0; c < 180; ++c) {
        word += alphabet[(i * 17 + c * 31 + (i / 7) * c) % alphabet.size()];
      }
      totalBytes_ += word.size();
      words_.push_back(std::move(word));
    }
    compressed_.assign(words_.begin(), words_.end());
    for (size_t stage = 0; stage < stages_; ++stage) {
      auto [storage, compressed, decoder] =
          FsstEncoder::compressAll(compressed_);
      compressed_ = std::move(compressed);
      decoders_[stage] = std::move(decoder);
      storage_.push_back(std::move(storage));
    }
  }

  std::string name() const final { return "FSST decode A/B"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    const char* modeValue = std::getenv("FSST_AB_MODE");
    const std::string mode = modeValue == nullptr ? "owning" : modeValue;
    AD_CONTRACT_CHECK(mode == "owning" || mode == "into");
    const size_t repetitions = parseEnvironmentSize("FSST_AB_REPETITIONS", 1);
    AD_CONTRACT_CHECK(repetitions > 0);

    FsstRepeatedDecoder<3> repeated{decoders_};
    const FsstDecoder& single = decoders_[0];
    std::string output;
    std::string scratch;

    auto decodeOwning = [&](std::string_view c) -> size_t {
      return stages_ == 1 ? single.decompress(c).size()
                          : repeated.decompress(c).size();
    };
    std::function<size_t(std::string_view)> decode = decodeOwning;
    if (mode == "into") {
      decode = makeIntoDecoder(single, repeated, stages_, compressed_, output,
                               scratch);
    }
    auto run = [&](size_t passes) {
      size_t bytes = 0;
      for (size_t pass = 0; pass < passes; ++pass) {
        for (auto c : compressed_) {
          bytes += decode(c);
        }
      }
      return bytes;
    };
    // Untimed warm-up pass, also the correctness check (total decoded bytes).
    AD_CORRECTNESS_CHECK(run(1) == totalBytes_);
    const auto before = JemallocThreadStats::read();
    auto& entry =
        results.addMeasurement(absl::StrCat(mode, ", ", stages_, " stage(s)"),
                               [&]() { return run(repetitions); });
    const auto after = JemallocThreadStats::read();
    entry.metadata().addKeyValuePair("decodes", numberOfWords * repetitions);
    entry.metadata().addKeyValuePair("stages", stages_);
    entry.metadata().addKeyValuePair("mode", mode);
    entry.metadata().addKeyValuePair("hasDecompressInto",
                                     hasDecompressInto<FsstDecoder>);
    if (JemallocThreadStats::available()) {
      entry.metadata().addKeyValuePair("allocationRequests",
                                       after.first - before.first);
      entry.metadata().addKeyValuePair("allocatedBytes",
                                       after.second - before.second);
    }
    return results;
  }
};

AD_REGISTER_BENCHMARK(FsstDecodeAbBenchmark);

}  // namespace
}  // namespace ad_benchmark

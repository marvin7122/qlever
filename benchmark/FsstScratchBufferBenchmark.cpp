// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <array>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cerrno>

#include "benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "util/FsstCompressor.h"

namespace ad_benchmark {
namespace {

template <size_t N>
size_t decodeRepeated(const std::array<FsstDecoder, N>& decoders,
                      std::string_view compressed, ql::span<char> output,
                      ql::span<char> scratch) {
  AD_CONTRACT_CHECK(N > 0);
  AD_CONTRACT_CHECK(!output.empty());
  AD_CONTRACT_CHECK(!scratch.empty());
  size_t destination = (N % 2 == 0) ? 1 : 0;
  std::array<ql::span<char>, 2> buffers{output, scratch};
  std::string_view input = compressed;
  size_t bytesWritten = 0;
  for (size_t stage = 0; stage < N; ++stage) {
    bytesWritten =
        decoders[N - 1 - stage].decompressInto(input, buffers[destination]);
    AD_CONTRACT_CHECK(bytesWritten <= buffers[destination].size());
    input = {buffers[destination].data(), bytesWritten};
    destination ^= 1;
  }
  AD_CONTRACT_CHECK(bytesWritten <= output.size());
  return bytesWritten;
}

class FsstScratchBufferBenchmark : public BenchmarkInterface {
 private:
  // Model a fixed three-stage repeated-FSST decode pipeline.
  static constexpr size_t numberOfStages = 3;
  // Keep views into the compressed strings retained by decoderStorage_.
  std::vector<std::string_view> compressed_;
  // Retain one decoder for each stage of the repeated pipeline.
  std::array<FsstDecoder, numberOfStages> decoders_;
  std::vector<std::shared_ptr<std::string>> decoderStorage_;
  size_t outputCapacity_ = 0;
  size_t intermediateCapacity_ = 0;

 public:
  FsstScratchBufferBenchmark() {
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    std::vector<std::string> words;
    words.reserve(5'000);
    for (size_t i = 0; i < 5'000; ++i) {
      std::string suffix;
      suffix.reserve(45);
      for (size_t character = 0; character < 45; ++character) {
        suffix += alphabet[(i * 17 + character * 31) % alphabet.size()];
      }
      words.push_back("http://www.wikidata.org/entity/Q" + suffix);
    }

    compressed_.assign(words.begin(), words.end());
    decoderStorage_.reserve(numberOfStages);
    for (size_t stage = 0; stage < numberOfStages; ++stage) {
      auto [storage, compressed, decoder] =
          FsstEncoder::compressAll(compressed_);
      compressed_ = std::move(compressed);
      decoders_[stage] = std::move(decoder);
      decoderStorage_.push_back(std::move(storage));
    }

    for (const std::string_view& compressed : compressed_) {
      outputCapacity_ = std::max(
          outputCapacity_,
          FsstRepeatedDecoder<numberOfStages>::maxDecompressedSize(compressed));
    }
    intermediateCapacity_ = outputCapacity_ / FsstDecoder::maxExpansionFactor;
  }

  std::string name() const final { return "FSST scratch buffer strategies"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    auto& group = results.addGroup(
        "Three-stage FSST scratch-buffer strategies (5,000 words)");
    const auto parseEnvironmentSize = [](const char* value,
                                          size_t defaultValue) {
      if (value == nullptr) {
        return defaultValue;
      }
      errno = 0;
      char* end = nullptr;
      const unsigned long parsed = std::strtoul(value, &end, 10);
      AD_CONTRACT_CHECK(end != value && *end == '\0' && errno != ERANGE);
      return static_cast<size_t>(parsed);
    };
    const size_t selectedStrategy =
        parseEnvironmentSize(std::getenv("FSST_SCRATCH_ONLY"), 3);
    // Bound the benchmark workload even when configured through the environment.
    constexpr size_t maxRepetitions = 1'000'000;
    const size_t repetitions = parseEnvironmentSize(
        std::getenv("FSST_SCRATCH_INNER_REPETITIONS"), 1);
    AD_CONTRACT_CHECK(selectedStrategy <= 3);
    AD_CONTRACT_CHECK(repetitions > 0);
    AD_CONTRACT_CHECK(repetitions <= maxRepetitions);

    auto runDecodeMeasurement = [&](ql::span<char> output,
                                    ql::span<char> scratch) {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (const auto& compressed : compressed_) {
          totalBytes += decodeRepeated(decoders_, compressed, output, scratch);
        }
      }
      return totalBytes;
    };
    auto addFullSizeStringScratch = [&] {
      group.addMeasurement("full-size std::string scratch", [&] {
        std::string output(outputCapacity_, '\0');
        std::string scratch(outputCapacity_, '\0');
        return runDecodeMeasurement({output.data(), output.size()},
                                    {scratch.data(), scratch.size()});
      });
    };
    auto addFullSizeUninitializedScratch = [&] {
      group.addMeasurement("full-size uninitialized scratch", [&] {
        auto output = std::make_unique<char[]>(outputCapacity_);
        auto scratch = std::make_unique<char[]>(outputCapacity_);
        return runDecodeMeasurement({output.get(), outputCapacity_},
                                    {scratch.get(), outputCapacity_});
      });
    };
    auto addStageAwareUninitializedScratch = [&] {
      group.addMeasurement("stage-aware uninitialized scratch", [&] {
        auto output = std::make_unique<char[]>(outputCapacity_);
        auto scratch = std::make_unique<char[]>(intermediateCapacity_);
        return runDecodeMeasurement({output.get(), outputCapacity_},
                                    {scratch.get(), intermediateCapacity_});
      });
    };

    constexpr std::array<std::array<size_t, 3>, 6> orders{{
        {0, 1, 2},
        {0, 2, 1},
        {1, 0, 2},
        {1, 2, 0},
        {2, 0, 1},
        {2, 1, 0},
    }};
    const char* orderEnv = std::getenv("FSST_SCRATCH_ORDER");
    const size_t orderIndex =
        orderEnv == nullptr
            ? 0
            : std::strtoul(orderEnv, nullptr, 10) % orders.size();
    for (size_t strategy : orders[orderIndex]) {
      if (strategy != selectedStrategy && selectedStrategy != 3) {
        continue;
      }
      if (strategy == 0) {
        addFullSizeStringScratch();
      } else if (strategy == 1) {
        addFullSizeUninitializedScratch();
      } else {
        AD_CORRECTNESS_CHECK(strategy == 2);
        addStageAwareUninitializedScratch();
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(FsstScratchBufferBenchmark);

}  // namespace
}  // namespace ad_benchmark

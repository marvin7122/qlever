// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/memory.h"
#include "backports/span.h"
#include "util/FsstCompressor.h"
#include "util/Random.h"

namespace ad_benchmark {
namespace {

template <size_t N>
size_t decodeRepeated(const std::array<FsstDecoder, N>& decoders,
                      std::string_view compressed, ql::span<char> output,
                      ql::span<char> scratch) {
  size_t destination = (N % 2 == 0) ? 1 : 0;
  std::array<ql::span<char>, 2> buffers{output, scratch};
  std::string_view input = compressed;
  size_t bytesWritten = 0;
  for (size_t stage = 0; stage < N; ++stage) {
    bytesWritten =
        decoders[N - 1 - stage].decompressInto(input, buffers[destination]);
    input = {buffers[destination].data(), bytesWritten};
    destination ^= 1;
  }
  return bytesWritten;
}

class FsstScratchBufferBenchmark : public BenchmarkInterface {
 private:
  static constexpr size_t numberOfStages = 3;
  std::vector<std::string_view> compressed_;
  std::array<FsstDecoder, numberOfStages> decoders_;
  std::vector<std::shared_ptr<std::string>> decoderStorage_;
  size_t outputCapacity_ = 0;
  size_t intermediateCapacity_ = 0;

 public:
  FsstScratchBufferBenchmark() {
    ad_utility::RandomCustomStringGenerator generator{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    std::vector<std::string> words;
    words.reserve(5'000);
    for (size_t i = 0; i < 5'000; ++i) {
      words.push_back("http://www.wikidata.org/entity/Q" + generator(45));
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

    for (std::string_view compressed : compressed_) {
      outputCapacity_ = std::max(
          outputCapacity_,
          FsstRepeatedDecoder<numberOfStages>::maxDecompressedSize(compressed));
    }
    intermediateCapacity_ = outputCapacity_ / FsstDecoder::maxExpansionFactor;
  }

  BenchmarkResults run() const override {
    BenchmarkResults results;
    auto& group = results.addResultGroup(
        "Three-stage FSST scratch-buffer strategies (5,000 words)");

    group.addMeasurement("full-size std::string scratch", [&] {
      std::string output(outputCapacity_, '\0');
      std::string scratch(outputCapacity_, '\0');
      size_t totalBytes = 0;
      for (std::string_view compressed : compressed_) {
        totalBytes += decodeRepeated(decoders_, compressed, output, scratch);
      }
      return totalBytes;
    });

    group.addMeasurement("full-size uninitialized scratch", [&] {
      auto output = ql::make_unique_for_overwrite<char[]>(outputCapacity_);
      auto scratch = ql::make_unique_for_overwrite<char[]>(outputCapacity_);
      size_t totalBytes = 0;
      for (std::string_view compressed : compressed_) {
        totalBytes += decodeRepeated(decoders_, compressed,
                                     {output.get(), outputCapacity_},
                                     {scratch.get(), outputCapacity_});
      }
      return totalBytes;
    });

    group.addMeasurement("stage-aware uninitialized scratch", [&] {
      auto output = ql::make_unique_for_overwrite<char[]>(outputCapacity_);
      auto scratch =
          ql::make_unique_for_overwrite<char[]>(intermediateCapacity_);
      size_t totalBytes = 0;
      for (std::string_view compressed : compressed_) {
        totalBytes += decodeRepeated(decoders_, compressed,
                                     {output.get(), outputCapacity_},
                                     {scratch.get(), intermediateCapacity_});
      }
      return totalBytes;
    });

    return results;
  }
};

AD_REGISTER_BENCHMARK(FsstScratchBufferBenchmark);

}  // namespace
}  // namespace ad_benchmark

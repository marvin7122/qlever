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
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>


namespace ad_benchmark {
namespace {




// _____________________________________________________________________________
class FsstScratchBufferBenchmark : public BenchmarkInterface {
 private:
  // Model a fixed three-stage repeated-FSST decode pipeline.
  // TODO<marvin> Consider making stage count configurable if multi-stage benchmarks are needed.
  static constexpr size_t numberOfStages = 3;

  struct CompressedData {
    std::vector<std::string> storage;
    std::vector<std::string_view> views;

    // Views are into strings owned by storage; the destruction order ensures
    // views are destroyed before storage.
  } compressed_;

  std::array<FsstDecoder, numberOfStages> decoders_;
  // Maximum fully decompressed size across the benchmark inputs, sizing the
  // final output buffer.
  size_t outputCapacity_ = 0;
  // Bound required for intermediate repeated-FSST stages, avoiding a full-size
  // scratch allocation.
  size_t intermediateCapacity_ = 0;
  bool initialized_ = false;

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

    compressed_.views.assign(words.begin(), words.end());
    compressed_.storage.reserve(numberOfStages);
    for (size_t stage = 0; stage < numberOfStages; ++stage) {
      auto [storage, compressed, decoder] =
          FsstEncoder::compressAll(compressed_.views);
      compressed_.views = std::move(compressed);
      // Compute bound for intermediate stage capacity (exclude final stage)
      if (stage < numberOfStages - 1) {
        for (const std::string_view& comp : compressed_.views) {
          intermediateCapacity_ = std::max(intermediateCapacity_,
              decoder.maxDecompressedSize(comp));
        }
      }
      decoders_[stage] = std::move(decoder);
      compressed_.storage.push_back(std::move(storage));
    }

    for (const std::string_view& compressed : compressed_) {
      outputCapacity_ = std::max(
          outputCapacity_,
          FsstRepeatedDecoder<numberOfStages>::maxDecompressedSize(compressed));
    }
    
    auto& group = results.addGroup(
        
      
    constexpr size_t maxRepetitions = 1'000'000;
    const size_t repetitions =
        parseEnvironmentSize(std::getenv("FSST_SCRATCH_INNER_REPETITIONS"), 1);
    AD_CONTRACT_CHECK(selectedStrategy <= 3);
    AD_CONTRACT_CHECK(repetitions > 0);
    AD_CONTRACT_CHECK(repetitions <= maxRepetitions);

    auto runDecodeMeasurement = [&](ql::span<char> output,
                                    ql::span<char> scratch) {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (const auto& compressed : compressed_.views) {
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
        AD_CONTRACT_CHECK(outputCapacity_ > 0);
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
    const size_t orderIndex =
        parseEnvironmentSize(std::getenv("FSST_SCRATCH_ORDER"), 0);
    AD_CONTRACT_CHECK(orderIndex < orders.size());
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

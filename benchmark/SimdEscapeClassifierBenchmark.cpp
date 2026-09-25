// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "engine/export_v2/SimdEscapeClassifier.h"

namespace {

using ql::engine::export_v2::EscapeFormat;
using ql::engine::export_v2::SimdEscapeClassifier;
using namespace ad_benchmark;

constexpr size_t targetBytesPerMeasurement = 64 * 1024 * 1024;

// Prevent dead-code elimination of benchmark computations.
template <typename T>
inline void escapeSink(T&& val) {
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" : "+r,m"(val) : : "memory");
#endif
}

// Scan `inputs` repeatedly until about `targetBytesPerMeasurement` bytes
// were processed; the framework records the wall time of this call.
template <EscapeFormat Format, bool UseSimd>
void scanRepeatedly(const std::vector<std::string>& inputs, size_t length) {
  const size_t repetitions =
      std::max<size_t>(1, targetBytesPerMeasurement / (inputs.size() * length));
  size_t checksum = 0;
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    for (const std::string& input : inputs) {
      if constexpr (UseSimd) {
        checksum += SimdEscapeClassifier::findFirstEscapeSimd<Format>(input);
      } else {
        checksum += SimdEscapeClassifier::findFirstEscapeScalar<Format>(input);
      }
    }
  }
  escapeSink(checksum);
}

// Register one scalar and one SIMD entry per input length. Each entry is the
// time to scan about 64 MiB; the speedup is the ratio of the two entries.
template <EscapeFormat Format>
void runLengths(BenchmarkResults& results, std::string_view name, char escape,
                const std::array<size_t, 14>& lengths) {
  for (const size_t length : lengths) {
    std::vector<std::string> inputs(4096, std::string(length, 'a'));
    for (size_t index = 0; index < inputs.size(); index += 12) {
      inputs[index][length / 2] = escape;
    }
    std::string measurementName =
        std::string{name} + "," + std::to_string(length);
    results.addMeasurement(
        measurementName + "_scalar_64MiB",
        [&inputs, length]() { scanRepeatedly<Format, false>(inputs, length); });
    results.addMeasurement(
        measurementName + "_simd_64MiB",
        [&inputs, length]() { scanRepeatedly<Format, true>(inputs, length); });
  }
}

class BMSimdEscapeClassifier : public BenchmarkInterface {
 public:
  std::string name() const final { return "SimdEscapeClassifier"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    constexpr std::array<size_t, 14> lengths{10, 16,  24,  31,  32,  48,  64,
                                             96, 128, 192, 250, 256, 512, 1024};

    runLengths<EscapeFormat::Turtle>(results, "turtle", '"', lengths);
    runLengths<EscapeFormat::Csv>(results, "csv", ',', lengths);
    runLengths<EscapeFormat::Tsv>(results, "tsv", '\t', lengths);

    return results;
  }
};

AD_REGISTER_BENCHMARK(BMSimdEscapeClassifier);

}  // namespace

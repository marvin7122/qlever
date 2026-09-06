// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <iomanip>
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

template <EscapeFormat Format, bool UseSimd>
double measure(const std::vector<std::string>& inputs, size_t length) {
  const size_t repetitions =
      std::max<size_t>(1, targetBytesPerMeasurement / (inputs.size() * length));
  size_t checksum = 0;
  const auto start = std::chrono::steady_clock::now();
  for (size_t repetition = 0; repetition < repetitions; ++repetition) {
    for (const std::string& input : inputs) {
      if constexpr (UseSimd) {
        checksum += SimdEscapeClassifier::findFirstEscapeSimd<Format>(input);
      } else {
        checksum += SimdEscapeClassifier::findFirstEscapeScalar<Format>(input);
      }
    }
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;
  const double bytes =
      static_cast<double>(repetitions * inputs.size() * length);
  const double nanoseconds =
      std::chrono::duration<double, std::nano>(elapsed).count();
  return nanoseconds / bytes;
}

class BMSimdEscapeClassifier : public BenchmarkInterface {
 public:
  std::string name() const final { return "SimdEscapeClassifier"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    constexpr std::array<size_t, 14> lengths{10, 16,  24,  31,  32,  48,  64,
                                             96, 128, 192, 250, 256, 512, 1024};

    struct FormatInfo {
      EscapeFormat format;
      std::string_view name;
      char escape;
    };

    const std::array<FormatInfo, 3> formats{
        FormatInfo{EscapeFormat::Turtle, "turtle", '"'},
        FormatInfo{EscapeFormat::Csv, "csv", ','},
        FormatInfo{EscapeFormat::Tsv, "tsv", '\t'}};

    for (const auto& fmt : formats) {
      for (const size_t length : lengths) {
        std::vector<std::string> inputs(4096, std::string(length, 'a'));
        for (size_t index = 0; index < inputs.size(); index += 12) {
          inputs[index][length / 2] = fmt.escape;
        }

        const double scalar = measure<fmt.format, false>(inputs, length);
        const double simd = measure<fmt.format, true>(inputs, length);

        std::string measurementName =
            fmt.name.data() + std::string(",") + std::to_string(length);

        results.addMeasurement(measurementName + "_scalar_ns_per_byte",
                               [scalar]() { (void)scalar; });
        results.addMeasurement(measurementName + "_simd_ns_per_byte",
                               [simd]() { (void)simd; });
        results.addMeasurement(measurementName + "_simd_speedup",
                               [scalar, simd]() { (void)(scalar / simd); });
      }
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(BMSimdEscapeClassifier);

}  // namespace

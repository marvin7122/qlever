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
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "engine/export_v2/SimdEscapeClassifier.h"

namespace {

using ql::engine::export_v2::EscapeFormat;
using ql::engine::export_v2::SimdEscapeClassifier;

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
  static volatile size_t observedChecksum;
  observedChecksum = checksum;
  return nanoseconds / bytes;
}

template <EscapeFormat Format>
void measureFormat(std::string_view name, char escape) {
  constexpr std::array lengths{size_t{10}, 16,  24,  31,  32,  48,  64,
                               96,         128, 192, 250, 256, 512, 1024};
  for (const size_t length : lengths) {
    std::vector<std::string> inputs(4096, std::string(length, 'a'));
    for (size_t index = 0; index < inputs.size(); index += 12) {
      inputs[index][length / 2] = escape;
    }
    const double scalar = measure<Format, false>(inputs, length);
    const double simd = measure<Format, true>(inputs, length);
    std::cout << name << ',' << length << ',' << std::fixed
              << std::setprecision(4) << scalar << ',' << simd << ','
              << scalar / simd << '\n';
  }
}

}  // namespace

int main() {
  std::cout
      << "format,length,scalar_ns_per_byte,simd_ns_per_byte,simd_speedup\n";
  measureFormat<EscapeFormat::Turtle>("turtle", '"');
  measureFormat<EscapeFormat::Csv>("csv", ',');
  measureFormat<EscapeFormat::Tsv>("tsv", '\t');
}

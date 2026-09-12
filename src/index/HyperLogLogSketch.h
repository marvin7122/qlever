// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <absl/numeric/bits.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"

namespace ql::index::stats {

// _____________________________________________________________________________
// HyperLogLog++ Metadata Cardinality Sketch:
// 1 KB compact sketch (1024 8-bit registers) embedded inside relation metadata
// to provide instantaneous O(1) distinct cardinality estimates and set union
// cardinalities during query planning.
template <size_t Precision = 10>  // 2^10 = 1024 registers
class HyperLogLogSketch {
 public:
  static constexpr size_t NUM_REGISTERS = 1 << Precision;
  static constexpr uint64_t REGISTER_MASK = NUM_REGISTERS - 1;

  static_assert(Precision >= 4 && Precision <= 16,
                "Precision must be in [4, 16] per the HyperLogLog++ "
                "recommendation; register values rely on this bound");

  // NOTE: This class is NOT thread-safe. Concurrent insert() or merge()
  // calls on the same instance require external synchronization. Sharing
  // const references across threads (e.g. in the query planner) is safe.

 private:
  std::vector<uint8_t> registers_;

  // Fast 64-bit splitmix hash function.
  // Note: hashes the full bit representation of the Id, including the
  // datatype bits. The same logical value with different datatypes (e.g. Int
  // 42 vs. Double 42.0) therefore counts as distinct keys.
  [[nodiscard]] static constexpr uint64_t hashValue(Id id) noexcept {
    uint64_t z = id.getBits() + 0x9e3779b97f4a7c15ULL;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }

 public:
  HyperLogLogSketch() : registers_(NUM_REGISTERS, 0) {}

  void insert(Id id) noexcept {
    uint64_t hash = hashValue(id);
    size_t regIdx = static_cast<size_t>(hash >> (64 - Precision));
    uint64_t remaining = (hash << Precision) | 1ULL;
    uint8_t leadingZeros =
        static_cast<uint8_t>(absl::countl_zero(remaining)) + 1;

    if (leadingZeros > registers_[regIdx]) {
      registers_[regIdx] = leadingZeros;
    }
  }

  // Merge another sketch into this one in O(M) time.
  void merge(const HyperLogLogSketch& other) noexcept {
    for (size_t i = 0; i < NUM_REGISTERS; ++i) {
      registers_[i] = std::max(registers_[i], other.registers_[i]);
    }
  }

  // Estimate distinct cardinality with small-range linear counting correction.
  [[nodiscard]] uint64_t estimateCardinality() const noexcept {
    double sum = 0.0;
    size_t zeroRegisters = 0;

    for (size_t i = 0; i < NUM_REGISTERS; ++i) {
      // ldexp instead of 1.0 / (1ULL << r): shifting by 64 or more is
      // undefined behavior, which a saturated register could trigger.
      sum += std::ldexp(1.0, -static_cast<int>(registers_[i]));
      if (registers_[i] == 0) {
        zeroRegisters++;
      }
    }

    // Alpha correction factor for m = 1024
    constexpr double alpha =
        0.7213 / (1.0 + 1.079 / static_cast<double>(NUM_REGISTERS));
    double rawEstimate =
        alpha * static_cast<double>(NUM_REGISTERS * NUM_REGISTERS) / sum;

    if (rawEstimate <= 2.5 * static_cast<double>(NUM_REGISTERS) &&
        zeroRegisters > 0) {
      // Linear counting for small cardinalities
      return static_cast<uint64_t>(
          static_cast<double>(NUM_REGISTERS) *
          std::log(static_cast<double>(NUM_REGISTERS) /
                   static_cast<double>(zeroRegisters)));
    }

    return static_cast<uint64_t>(rawEstimate);
  }
};

}  // namespace ql::index::stats

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BUFFERPOOLBUDGET_H
#define QLEVER_SRC_UTIL_BUFFERPOOLBUDGET_H

#include <cstddef>
#include <vector>

namespace ad_utility {

// Memory signals sampled by the caller. `pressure` is the fraction of time
// tasks were stalled on memory (for example PSI `some avg10 / 100`), in
// [0, 1].
struct MemorySignals {
  size_t freeBytes;
  double pressure;
};

// Pure policy that decides the target size of the vocabulary buffer pool from
// injected memory signals. It does no I/O and owns no thread; the caller
// drives it by calling `tick()` periodically.
//
// Per tick:
// - Shrink if free memory is below the reserve or the pressure exceeds
//   `kPressureThreshold`. The cut is the deficit below the reserve, but at
//   least `kMinShrinkFraction` of the current target.
// - Otherwise grow by `kStep` if free memory exceeds the reserve and there is
//   no pressure at all.
// - Otherwise hold.
// The result is always clamped to `[floor(), ceiling()]`.
class BufferPoolBudget {
 public:
  static constexpr size_t kFloor = size_t{256} << 20;
  static constexpr size_t kStep = size_t{1} << 30;
  static constexpr double kMinShrinkFraction = 0.10;
  static constexpr double kPressureThreshold = 0.10;

 private:
  size_t reserve_;
  size_t ceiling_;
  size_t target_;

 public:
  // `fileSizes` are the sizes of the vocabulary files served by the pool and
  // must not be empty. The ceiling is the smallest of them, but never less
  // than `kFloor`. `initialTarget` is clamped to `[floor(), ceiling()]`.
  BufferPoolBudget(size_t reserve, const std::vector<size_t>& fileSizes,
                   size_t initialTarget);

  // Apply the policy to `signals`, store the new target and return it. Throws
  // if `signals.pressure` is not in [0, 1].
  size_t tick(MemorySignals signals);

  size_t target() const noexcept { return target_; }
  static constexpr size_t floor() noexcept { return kFloor; }
  size_t ceiling() const noexcept { return ceiling_; }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BUFFERPOOLBUDGET_H

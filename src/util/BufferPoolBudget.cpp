// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/BufferPoolBudget.h"

#include <algorithm>

#include "util/Exception.h"

namespace ad_utility {

// _____________________________________________________________________________
BufferPoolBudget::BufferPoolBudget(size_t reserve,
                                   const std::vector<size_t>& fileSizes,
                                   size_t initialTarget)
    : reserve_{reserve} {
  AD_CONTRACT_CHECK(!fileSizes.empty());
  ceiling_ = std::max(std::ranges::min(fileSizes), kFloor);
  target_ = std::clamp(initialTarget, kFloor, ceiling_);
}

// _____________________________________________________________________________
size_t BufferPoolBudget::tick(MemorySignals signals) {
  AD_CONTRACT_CHECK(signals.pressure >= 0.0 && signals.pressure <= 1.0,
                    "Memory pressure must be in [0, 1]");
  size_t target = target_;
  if (signals.freeBytes < reserve_ || signals.pressure > kPressureThreshold) {
    size_t deficit =
        signals.freeBytes < reserve_ ? reserve_ - signals.freeBytes : 0;
    auto minCut = static_cast<size_t>(static_cast<double>(target) *
                                      kMinShrinkFraction);
    size_t cut = std::max(deficit, minCut);
    target = cut >= target ? kFloor : target - cut;
  } else if (signals.freeBytes > reserve_ && signals.pressure == 0.0) {
    target += std::min(kStep, ceiling_ - target);
  }
  target_ = std::clamp(target, kFloor, ceiling_);
  return target_;
}

}  // namespace ad_utility

// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_VIEWS_ASYNCTRANSFORMSTOPFLAG_H
#define QLEVER_SRC_UTIL_VIEWS_ASYNCTRANSFORMSTOPFLAG_H

#include <atomic>

namespace ad_utility {

// The flag that an `AsyncTransformView` passes to a transformation that takes
// it as its second argument. It is requested when the view is destroyed, so
// the results of the pending elements are no longer needed (see NOTE 5 in
// `AsyncTransformView.h`). It is a type of its own (and not convertible to
// `bool`), so that a transformation only receives it if it asks for it.
// It refers to the flag of the view (it is a non-owning reference), and it is
// only valid during the call of the transformation that received it, because
// the view waits for all its tasks before the flag is destroyed. A
// transformation must not store it beyond that call.
class AsyncTransformStopFlag {
  // Non-owning; refers to the flag in the `AsyncTransformView` that created
  // this flag.
  const std::atomic<bool>& flag_;

 public:
  explicit AsyncTransformStopFlag(const std::atomic<bool>& flag)
      : flag_{flag} {}
  ~AsyncTransformStopFlag() noexcept = default;

  // Return true iff the view no longer needs the result. The relaxed load is
  // enough: the flag is only a hint to skip work whose result is discarded, no
  // data is published through it, and a late observation only costs extra rows.
  bool isRequested() const { return flag_.load(std::memory_order_relaxed); }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_ASYNCTRANSFORMSTOPFLAG_H

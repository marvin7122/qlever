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
class AsyncTransformStopFlag {
  const std::atomic<bool>* flag_;

 public:
  explicit AsyncTransformStopFlag(const std::atomic<bool>& flag)
      : flag_{&flag} {}

  // Return true iff the view no longer needs the result.
  bool isRequested() const { return flag_->load(std::memory_order_relaxed); }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_ASYNCTRANSFORMSTOPFLAG_H

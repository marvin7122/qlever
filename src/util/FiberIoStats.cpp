// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/FiberIoStats.h"

#include <absl/strings/str_cat.h>

#include <functional>

#include "backports/algorithm.h"
#include "util/Log.h"

namespace ad_utility::fiberIoStats {

namespace {
constexpr std::array<std::string_view, kNumCounters> kNames{
    "fiberRuns",
    "fiberBodies",
    "fiberWaits",
    "waitingFibersAtWaitSum",
    "yields",
    "parks",
    "waitingFibersAtParkSum",
    "blockingWaits",
    "peekReaps",
    "drainsWithoutKernelWait",
    "kernelWaits",
    "kernelWaitNs",
    "resolvedIds"};
}  // namespace

// _____________________________________________________________________________
Snapshot snapshot() {
  Snapshot values{};
#ifdef QLEVER_FIBER_IO_STATS
  ql::ranges::transform(detail::counters(), values.begin(),
                        [](const std::atomic<uint64_t>& counter) {
                          return counter.load(std::memory_order_relaxed);
                        });
#endif
  return values;
}

// _____________________________________________________________________________
Snapshot difference(const Snapshot& after, const Snapshot& before) {
  Snapshot values{};
  ql::ranges::transform(after, before, values.begin(), std::minus<>{});
  return values;
}

// _____________________________________________________________________________
std::string format(const Snapshot& values) {
  std::string result;
  for (const auto& [name, value] : ::ranges::views::zip(kNames, values)) {
    absl::StrAppend(&result, result.empty() ? "" : " ", name, "=", value);
  }
  return result;
}

// _____________________________________________________________________________
ExportScope::ExportScope(std::string_view label)
    : label_{label}, start_{snapshot()} {}

// _____________________________________________________________________________
ExportScope::~ExportScope() {
  AD_LOG_INFO << "Fiber I/O stats of " << label_ << ": "
              << format(difference(snapshot(), start_)) << std::endl;
}

// _____________________________________________________________________________
std::shared_ptr<ExportScope> makeExportScope(std::string_view label) {
  if constexpr (!kEnabled) {
    (void)label;
    return nullptr;
  } else {
    return std::make_shared<ExportScope>(label);
  }
}

}  // namespace ad_utility::fiberIoStats

// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "util/Exception.h"

namespace ad_utility::export_v2 {

// -----------------------------------------------------------------------------
// Job State Interface & Owned Morsel
// -----------------------------------------------------------------------------
// Type-erased job side of the scheduler contract. The scheduler only ever
// observes jobs through this interface, so cancellation, demand changes, and
// lease accounting stay behind one narrow boundary. `ExportJobStateBase`
// carries no state and no scheduler dependency; the typed
// `ExportJobState<ResultType>` implementation lives with the scheduler until
// it is extracted as its own module.

class ExportJobStateBase {
 public:
  virtual ~ExportJobStateBase() = default;
  [[nodiscard]] virtual uint64_t jobId() const noexcept = 0;
  virtual void onDemandChanged(size_t activeForegroundQueries,
                               uint64_t newEpoch) = 0;
  virtual void onHelperLeaseAcquired(uint64_t leaseEpoch) = 0;
  virtual void onHelperLeaseReleased(uint64_t leaseEpoch) = 0;
  virtual void executeHelperTask(size_t morselIndex, uint64_t leaseEpoch) = 0;
  [[nodiscard]] virtual bool isCancelled() const noexcept = 0;
};

struct OwnedMorsel {
  OwnedMorsel(std::shared_ptr<ExportJobStateBase> jobState,
              uint64_t submissionEpoch, size_t morselIndex)
      : submissionEpoch_{submissionEpoch},
        morselIndex_{morselIndex},
        jobState_{std::move(jobState)},
        jobId_{checkedJobId(jobState_)} {}

  // The job id is derived from the state, never passed alongside it: the
  // state owns its identity, so a mismatched id is unrepresentable. The
  // state handle is private so no later reassignment can desynchronize the
  // cached id; move it out with `extractJobState`.
  [[nodiscard]] const std::shared_ptr<ExportJobStateBase>& jobState()
      const noexcept {
    return jobState_;
  }
  [[nodiscard]] std::shared_ptr<ExportJobStateBase> extractJobState() && {
    return std::move(jobState_);
  }
  [[nodiscard]] uint64_t jobId() const noexcept { return jobId_; }

  uint64_t submissionEpoch_{0};
  size_t morselIndex_{0};

 private:
  // Assert the state handle before deriving the cached id, so the identity
  // invariant holds from construction on without two-phase initialization.
  static uint64_t checkedJobId(
      const std::shared_ptr<ExportJobStateBase>& jobState) {
    AD_CONTRACT_CHECK(jobState != nullptr);
    return jobState->jobId();
  }

  std::shared_ptr<ExportJobStateBase> jobState_;
  uint64_t jobId_;
};

}  // namespace ad_utility::export_v2

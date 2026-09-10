// Copyright 2026, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "util/Exception.h"

namespace ad_utility::export_v2 {

// -----------------------------------------------------------------------------
// Production Job State Interface & Owned Morsel
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
  std::shared_ptr<ExportJobStateBase> jobState_;
  uint64_t jobId_{0};
  uint64_t submissionEpoch_{0};
  size_t morselIndex_{0};

  // The job id is derived from the state, never passed alongside it: the
  // state owns its identity, so a mismatched id is unrepresentable.
  OwnedMorsel(std::shared_ptr<ExportJobStateBase> jobState,
              uint64_t submissionEpoch, size_t morselIndex)
      : jobState_{std::move(jobState)},
        submissionEpoch_{submissionEpoch},
        morselIndex_{morselIndex} {
    AD_CONTRACT_CHECK(jobState_ != nullptr);
    jobId_ = jobState_->jobId();
  }
};

}  // namespace ad_utility::export_v2

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_LOOKUPPIPELINE_H
#define QLEVER_SRC_INDEX_VOCABULARY_LOOKUPPIPELINE_H

#include <cstddef>
#include <memory>

#include "util/IoUringManager.h"

namespace ad_utility::vocabulary {

// While a `LookupPipelineScope` is alive on a thread, the split-phase lookups
// of `VocabularyOnDisk` that this thread starts (`beginLookup`) are pipelined
// lookups:
// - They read through one I/O manager per thread (`manager()`) instead of
//   taking a manager from the vocabulary's pool, so that a thread can have
//   several lookups in flight without exhausting the pool (8 managers) and
//   without waiting for another thread's manager.
// - With the buffer pools, `beginLookup` already starts loading the missing
//   offset pages, and `VocabLookupHandleBase::advance` waits for them and
//   starts loading the missing word pages, so that a caller can do CPU work
//   while both phases are in flight.
// All handles started in a scope must be finished or destroyed before the
// scope ends, on the same thread.
class LookupPipelineScope {
 public:
  // The ring size of the per-thread manager: enough for the reads of two
  // windows of 1024 rows without blocking in `addBatch`.
  static constexpr unsigned ringSize = 4096;

  LookupPipelineScope() { ++depth(); }
  ~LookupPipelineScope() { --depth(); }
  LookupPipelineScope(const LookupPipelineScope&) = delete;
  LookupPipelineScope& operator=(const LookupPipelineScope&) = delete;

  // Whether a scope is alive on the calling thread.
  static bool active() { return depth() > 0; }

  // The I/O manager of the calling thread (created on first use, destroyed
  // when the thread exits). Only to be used by the calling thread.
  static BatchManagerBase& manager() {
    thread_local std::unique_ptr<BatchManagerBase> threadManager;
    thread_local bool preferIoUring = true;
    if (threadManager == nullptr) {
      threadManager = makeBatchManager(preferIoUring, ringSize);
    }
    return *threadManager;
  }

 private:
  static size_t& depth() {
    thread_local size_t threadDepth = 0;
    return threadDepth;
  }
};

}  // namespace ad_utility::vocabulary

#endif  // QLEVER_SRC_INDEX_VOCABULARY_LOOKUPPIPELINE_H

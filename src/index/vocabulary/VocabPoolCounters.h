// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// Diagnostic counters for the `lookupBatch` manager pool of
// `VocabularyOnDisk`. Compiled in only with `QLEVER_VOCAB_POOL_COUNTERS`;
// otherwise every hook below is an empty inline function.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABPOOLCOUNTERS_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABPOOLCOUNTERS_H

#include <atomic>
#include <chrono>
#include <cstdint>

#include "util/Log.h"

namespace ad_utility::vocabPoolCounters {
#ifdef QLEVER_VOCAB_POOL_COUNTERS
struct Counters {
  std::atomic<uint64_t> checkouts_{0};
  std::atomic<uint64_t> waitNs_{0};
  std::atomic<uint64_t> maxWaitNs_{0};
  std::atomic<uint64_t> slowCheckouts_{0};
  std::atomic<uint64_t> ownedCalls_{0};
  std::atomic<uint64_t> holdNs_{0};
};
inline Counters& counters() {
  static Counters c;
  return c;
}
inline void report() {
  auto& c = counters();
  AD_LOG_INFO << "VOCAB_POOL_COUNTERS checkouts=" << c.checkouts_.load()
              << " wait_ns=" << c.waitNs_.load()
              << " max_wait_ns=" << c.maxWaitNs_.load()
              << " slow_checkouts=" << c.slowCheckouts_.load()
              << " owned_calls=" << c.ownedCalls_.load()
              << " hold_ns=" << c.holdNs_.load() << std::endl;
}
using Clock = std::chrono::steady_clock;
inline Clock::time_point now() { return Clock::now(); }
// Record one pool checkout whose `pop` took from `start` to `end`.
inline void recordCheckout(Clock::time_point start, Clock::time_point end) {
  auto& c = counters();
  auto ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
          .count());
  c.waitNs_ += ns;
  uint64_t prev = c.maxWaitNs_.load();
  while (ns > prev && !c.maxWaitNs_.compare_exchange_weak(prev, ns)) {
  }
  // A checkout slower than 20 us waited for the queue mutex or for a
  // manager; an uncontended pop takes well under one microsecond.
  if (ns > 20'000) {
    ++c.slowCheckouts_;
  }
  if ((++c.checkouts_ & 255) == 0) {
    report();
  }
}
// Record how long a checked-out manager was held (both read phases).
inline void recordHold(Clock::time_point start, Clock::time_point end) {
  counters().holdNs_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
          .count());
}
inline void recordOwnedCall() {
  if ((++counters().ownedCalls_ & 1023) == 0) {
    report();
  }
}
#else
struct TimePoint {};
inline TimePoint now() { return {}; }
inline void recordCheckout(TimePoint, TimePoint) {}
inline void recordHold(TimePoint, TimePoint) {}
inline void recordOwnedCall() {}
#endif
}  // namespace ad_utility::vocabPoolCounters

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABPOOLCOUNTERS_H

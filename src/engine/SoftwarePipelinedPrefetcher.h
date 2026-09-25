// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SOFTWAREPIPELINEDPREFETCHER_H
#define QLEVER_SRC_ENGINE_SOFTWAREPIPELINEDPREFETCHER_H

#include <algorithm>
#include <cstddef>
#include <type_traits>

#include "backports/algorithm.h"
#include "backports/concepts.h"

namespace qlever {

// Default for `SoftwarePipelinedPrefetcher::PREFETCH_DISTANCE`. With a DRAM
// latency of about 100 ns and a consumer call of a few nanoseconds, 16
// iterations are enough for the prefetched cache line to arrive before it is
// consumed. Callers with more expensive consumers can use a smaller distance.
inline constexpr size_t DEFAULT_PREFETCH_DISTANCE = 16;

namespace detail {
// `R` is a random-access range with known size whose elements are (possibly
// const) pointers, and `F` can be invoked repeatedly with its elements.
template <typename R, typename F>
CPP_concept PrefetchInput =
    ql::ranges::random_access_range<const R> &&
    ql::ranges::sized_range<const R> &&
    std::is_pointer_v<ql::ranges::range_value_t<const R>> &&
    ql::concepts::invocable<F&, ql::ranges::range_reference_t<const R>>;
}  // namespace detail

// Call a consumer for each pointer of a range and prefetch the target of the
// pointer `PREFETCH_DISTANCE` positions ahead. This hides the memory latency
// of random accesses (e.g. hash table probes or vocabulary lookups) behind the
// work of the consumer for the preceding pointers.
template <size_t PREFETCH_DISTANCE = DEFAULT_PREFETCH_DISTANCE>
class SoftwarePipelinedPrefetcher {
  static_assert(PREFETCH_DISTANCE > 0);

 public:
  // Call `consume(pointer)` for each `pointer` in `pointers`, in order and
  // exactly once. Before the call for position `i`, prefetch (for reading,
  // with high temporal locality) the target of position
  // `i + PREFETCH_DISTANCE`, if that position exists. The last
  // `PREFETCH_DISTANCE` pointers are consumed in a second loop without a
  // prefetch, so the main loop has no bounds branch. `consume` is invoked
  // repeatedly and is therefore called as an lvalue, not forwarded.
  CPP_template(typename PointerRange, typename Consumer)(
      requires detail::PrefetchInput<
          PointerRange,
          Consumer>) static void processWithPrefetch(const PointerRange&
                                                         pointers,
                                                     Consumer&& consume) {
    auto current = ql::ranges::begin(pointers);
    const auto end = ql::ranges::end(pointers);
    const auto distance = static_cast<ptrdiff_t>(
        std::min<size_t>(ql::ranges::size(pointers), PREFETCH_DISTANCE));
    for (auto ahead = current + distance; ahead != end; ++ahead, ++current) {
      __builtin_prefetch(*ahead, 0, 3);
      consume(*current);
    }
    for (; current != end; ++current) {
      consume(*current);
    }
  }
};

}  // namespace qlever

#endif  // QLEVER_SRC_ENGINE_SOFTWAREPIPELINEDPREFETCHER_H

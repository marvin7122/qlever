// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_ASYNCCHUNKPIPELINE_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_ASYNCCHUNKPIPELINE_H

#include <array>
#include <cstddef>
#include <exception>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "util/Exception.h"

namespace qlever::export_v2 {

// Whether the export V2 code path is compiled in (CMake option
// `QLEVER_ENABLE_EXPORT_V2`). A pipeline starts `Running` only if this
// compile-time switch AND the runtime switch
// `AsyncChunkPipelineConfig::runtimeEnabled_` were both on at construction:
// the compile-time switch keeps default builds free of the experimental path,
// the runtime switch lets a build that has it still use the existing export
// without a rebuild.
#if defined(QLEVER_ENABLE_EXPORT_V2) && QLEVER_ENABLE_EXPORT_V2
inline constexpr bool exportV2CompiledIn = true;
#else
inline constexpr bool exportV2CompiledIn = false;
#endif

// The number of slots of an `AsyncChunkPipeline`: one chunk can be
// transmitted while the next one is produced (double buffering).
inline constexpr size_t numRingSlots = 2;
// The ring arithmetic below (modulo indexing, `Full` after two pushes) only
// holds for exactly two slots; changing the count requires updating it.
static_assert(numRingSlots == 2);

// Runtime configuration of an `AsyncChunkPipeline`, copied at construction.
struct AsyncChunkPipelineConfig {
  // The runtime kill switch, see `exportV2CompiledIn`. Off by default so that
  // a default-constructed pipeline never runs.
  bool runtimeEnabled_ = false;
};

// The result of `AsyncChunkPipeline::push`.
enum class PushResult {
  // The `chunk` was stored and the caller's `chunk` was moved from.
  Accepted,
  // Transient backpressure: both slots hold undrained chunks. The caller's
  // `chunk` is left untouched; retry after the next successful `pop`.
  Full,
  // Permanent: the pipeline is disabled, finished, failed, or cancelled. The
  // caller's `chunk` is left untouched; do not retry.
  Closed
};

// Counters of an `AsyncChunkPipeline`, returned by value as a snapshot. Sizes
// are taken from `ChunkType::size()`, or 0 for chunk types without `size()`.
struct AsyncChunkPipelineStats {
  // Chunks accepted by `push`.
  size_t chunksProduced_ = 0;
  size_t bytesProduced_ = 0;
  // Chunks returned by `pop`.
  size_t chunksConsumed_ = 0;
  size_t bytesConsumed_ = 0;
  // Chunks dropped by `cancel` (including the implicit `cancel` of the
  // destructor while `Running`).
  size_t chunksDiscarded_ = 0;
  size_t bytesDiscarded_ = 0;
};

namespace detail {
// Whether a const `T&` has a callable `size()` member. A `size()` that is
// only non-const does not satisfy this trait.
template <typename T, typename = void>
struct HasSize : std::false_type {};
template <typename T>
struct HasSize<T, std::void_t<decltype(std::declval<const T&>().size())>>
    : std::true_type {};

// Whether calling `size()` on a const `T&` is `noexcept`. True for types
// without a const `size()` (the `if constexpr` discards the lookup, so this
// stays well-formed for sizeless chunk types like `unique_ptr`).
template <typename T>
constexpr bool hasNothrowSize() {
  if constexpr (HasSize<T>::value) {
    return noexcept(std::declval<const T&>().size());
  } else {
    return true;
  }
}
}  // namespace detail

// A two-slot ring that hands serialized chunks from the producer
// (serialization) to the consumer (socket transmission) of one export, so
// that at most one chunk is produced ahead of the transmitted one and memory
// stays bounded. Each `pop` frees its slot, so the ring rotates without
// allocating.
//
// The ring performs no synchronization: all member functions must be called
// from the thread (or the event loop) that drives the export. Nothing blocks:
// `push` returns `PushResult::Closed` when not `Running`, else
// `PushResult::Full` when both slots are occupied; `pop` returns the oldest
// queued chunk, rethrows the producer's exception when `Failed` and empty,
// and returns `std::nullopt` otherwise. The driver then suspends the
// respective side, using `isRunning` to tell an empty running ring from a
// closed one.
//
// `ChunkType` must own its data (a `chunk` outlives the call to `push`) and be
// nothrow move constructible, which keeps the ring consistent when a `chunk`
// is moved in or out. A const `size()` member, if present, must be `noexcept`:
// `chunkSize()` calls it from `noexcept` contexts (`cancel` and therefore the
// destructor), where a throw would terminate.
template <typename ChunkType = std::string>
class AsyncChunkPipeline {
  static_assert(std::is_nothrow_move_constructible_v<ChunkType>,
                "The ring relies on non-throwing moves of `ChunkType`");
  static_assert(std::is_nothrow_destructible_v<ChunkType>,
                "Chunks are destroyed in the `noexcept` `cancel` and in the "
                "destructor, so their destruction must not throw");
  static_assert(!std::is_pointer_v<ChunkType>, "`ChunkType` must own its data");
  static_assert(!std::is_reference_v<ChunkType>,
                "`ChunkType` must own its data (no references)");
  static_assert(detail::hasNothrowSize<ChunkType>(),
                "`ChunkType::size()` runs in `noexcept` contexts and must "
                "not throw");

 private:
  // The lifecycle. `Disabled` is set at construction when either kill switch
  // is off, and no transition ever leaves it. `Running` accepts chunks.
  // `Finished` and `Failed` still hand out the queued chunks, after which
  // `pop` reports the end (`Finished`) or rethrows the producer's exception
  // (`Failed`). `Cancelled` has dropped all queued chunks.
  enum class State { Disabled, Running, Finished, Cancelled, Failed };

  // An engaged slot holds an undrained `chunk`, a disengaged slot is free.
  // When `numFilledSlots_ > 0` the occupied slots are the `numFilledSlots_`
  // slots starting at `slots_[consumeIndex_]` (modulo `numRingSlots`); when
  // `numFilledSlots_ == 0` no slot is occupied.
  std::array<std::optional<ChunkType>, numRingSlots> slots_;
  size_t consumeIndex_ = 0;
  size_t numFilledSlots_ = 0;
  State state_;
  // Non-null if and only if `state_ == State::Failed`.
  std::exception_ptr producerFailure_;
  AsyncChunkPipelineStats stats_;

  // Return `chunk.size()` for the byte counters, or 0 for chunk types
  // without a const `size()`. Stays `noexcept` so it can run in `cancel()`
  // and therefore the destructor.
  [[nodiscard]] static size_t chunkSize(
      [[maybe_unused]] const ChunkType& chunk) noexcept {
    if constexpr (detail::HasSize<ChunkType>::value) {
      return chunk.size();
    } else {
      return 0;
    }
  }

  // Assert the ring shape: index bounds, the failure-pointer bicon
  // (`Failed` if and only if a failure is stored), and the engagement window
  // (exactly the `numFilledSlots_` slots from `consumeIndex_` are engaged).
  // Internal invariant (checked on entry and exit of the mutators below),
  // unlike the `AD_CONTRACT_CHECK` in `fail`, which guards caller input.
  void checkRingInvariants() const {
    AD_CORRECTNESS_CHECK(consumeIndex_ < numRingSlots);
    AD_CORRECTNESS_CHECK(numFilledSlots_ <= numRingSlots);
    AD_CORRECTNESS_CHECK((state_ == State::Failed) ==
                         (producerFailure_ != nullptr));
    size_t engagedSlots = 0;
    for (const auto& slot : slots_) {
      engagedSlots += slot.has_value() ? 1 : 0;
    }
    AD_CORRECTNESS_CHECK(engagedSlots == numFilledSlots_);
    for (size_t i = 0; i < numRingSlots; ++i) {
      const size_t offset = (i + numRingSlots - consumeIndex_) % numRingSlots;
      AD_CORRECTNESS_CHECK(slots_[i].has_value() == (offset < numFilledSlots_));
    }
  }

 public:
  // Create a pipeline that is initially `Running` if both kill switches were
  // on at construction (see `exportV2CompiledIn`) and initially `Disabled`
  // otherwise. Later `finish`/`fail`/`cancel` move `Running` to
  // `Finished`/`Failed`/`Cancelled` but never leave `Disabled`.
  explicit AsyncChunkPipeline(AsyncChunkPipelineConfig config = {}) noexcept
      : state_{exportV2CompiledIn && config.runtimeEnabled_ ? State::Running
                                                            : State::Disabled} {
  }

  // A pipeline belongs to exactly one export, whose producer and consumer
  // refer to it; forbid copies and moves so it cannot be detached from them.
  AsyncChunkPipeline(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline& operator=(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline(AsyncChunkPipeline&&) = delete;
  AsyncChunkPipeline& operator=(AsyncChunkPipeline&&) = delete;

  ~AsyncChunkPipeline() { cancel(); }

  // Return the construction-time value: `true` if both kill switches were on
  // (stays `true` after `finish`, `fail`, or `cancel`), `false` if the
  // pipeline is `Disabled` (stays `false`). Use `isRunning` to find out
  // whether `push` can still accept chunks.
  [[nodiscard]] bool isEnabled() const noexcept {
    return state_ != State::Disabled;
  }

  // Return whether the pipeline is `Running`. When `true`, `push` returns
  // `Accepted` if a slot is free else `Full`; when `false`, `push` returns
  // `Closed` (use `isEnabled` for the construction-time value).
  [[nodiscard]] bool isRunning() const noexcept {
    return state_ == State::Running;
  }

  // If the pipeline is `Running` and a slot is free, move `chunk` into it and
  // return `Accepted`. Otherwise leave `chunk` untouched and return `Closed`
  // (not `Running`, checked first) or `Full` (both slots occupied). Do not
  // block.
  [[nodiscard]] PushResult push(ChunkType&& chunk) {
    if (!isRunning()) {
      return PushResult::Closed;
    }
    checkRingInvariants();
    if (numFilledSlots_ == numRingSlots) {
      return PushResult::Full;
    }
    auto& slot = slots_[(consumeIndex_ + numFilledSlots_) % numRingSlots];
    AD_CORRECTNESS_CHECK(!slot.has_value());
    const size_t size = chunkSize(chunk);
    slot.emplace(std::move(chunk));
    ++numFilledSlots_;
    ++stats_.chunksProduced_;
    stats_.bytesProduced_ += size;
    checkRingInvariants();
    return PushResult::Accepted;
  }

  // If a `chunk` is queued, return the oldest one and free its slot, in every
  // state. Otherwise rethrow the producer's exception if the pipeline is
  // `Failed` (on every call), and return `std::nullopt` in all other states.
  // An empty `Running` pipeline thus also returns `std::nullopt`; use
  // `isRunning` to tell "not yet" from "never again". Do not block.
  [[nodiscard]] std::optional<ChunkType> pop() {
    checkRingInvariants();
    if (numFilledSlots_ > 0) {
      auto& slot = slots_[consumeIndex_];
      AD_CORRECTNESS_CHECK(slot.has_value());
      // Measure before mutating the ring, so the counters only reflect
      // chunks that are actually stored.
      const size_t bytes = chunkSize(*slot);
      std::optional<ChunkType> oldestChunk{std::move(slot)};
      slot.reset();
      consumeIndex_ = (consumeIndex_ + 1) % numRingSlots;
      --numFilledSlots_;
      ++stats_.chunksConsumed_;
      stats_.bytesConsumed_ += bytes;
      checkRingInvariants();
      return oldestChunk;
    }
    if (state_ == State::Failed) {
      AD_CORRECTNESS_CHECK(producerFailure_ != nullptr);
      std::rethrow_exception(producerFailure_);
    }
    return std::nullopt;
  }

  // Report that the producer has pushed its last `chunk`. The queued chunks
  // can still be popped. Do nothing if the pipeline is not `Running`.
  void finish() noexcept {
    if (isRunning()) {
      state_ = State::Finished;
    }
    checkRingInvariants();
  }

  // Report a producer failure: `pop` rethrows `failure` on every call once
  // the queued chunks are drained. Do nothing if the pipeline is not
  // `Running`: it is disabled, or the export already ended in another way and
  // its outcome must not change afterwards (the first failure wins).
  // Otherwise `failure` must not be null.
  void fail(std::exception_ptr failure) {
    if (!isRunning()) {
      return;
    }
    AD_CONTRACT_CHECK(failure != nullptr);
    producerFailure_ = std::move(failure);
    state_ = State::Failed;
    checkRingInvariants();
  }

  // Drop all queued chunks, count them as discarded, and close the pipeline.
  // Do nothing if the pipeline is not `Running` (`Disabled`, `Finished`,
  // `Failed`, `Cancelled`): when `Finished` or `Failed` the queued chunks
  // remain for the consumer and, if undrained, are released without being
  // counted when the pipeline is destroyed. `Disabled` never holds chunks;
  // `Cancelled` was already discarded.
  void cancel() noexcept {
    if (!isRunning()) {
      return;
    }
    state_ = State::Cancelled;
    for (auto& slot : slots_) {
      if (slot.has_value()) {
        ++stats_.chunksDiscarded_;
        stats_.bytesDiscarded_ += chunkSize(*slot);
        slot.reset();
      }
    }
    numFilledSlots_ = 0;
    checkRingInvariants();
  }

  // Return a snapshot of the counters.
  [[nodiscard]] AsyncChunkPipelineStats stats() const noexcept {
    return stats_;
  }
};

}  // namespace qlever::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_ASYNCCHUNKPIPELINE_H

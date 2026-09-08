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
#include <utility>

#include "util/Exception.h"
#include "util/Invariants.h"

namespace qlever::export_v2 {

#if defined(QLEVER_ENABLE_EXPORT_V2) && QLEVER_ENABLE_EXPORT_V2
inline constexpr bool kExportV2CompiledIn = true;
#else
inline constexpr bool kExportV2CompiledIn = false;
#endif

// A double-buffered asynchronous chunk ring, adapted from the 2-slot design
// of PR #82. Two fixed slots alternate between the producer filling the next
// chunk and the consumer draining the transmitted one; each completed `pop`
// frees its slot for reuse, so the ring rotates without allocation.
//
// The ring performs no synchronization of its own: all methods must be called
// from the single query worker thread (or its async event loop), which makes
// the handoff lock-free by construction. There is no blocking either: when
// both slots hold undrained chunks, `push` reports `PushResult::Full` and the
// async driver suspends chunk generation until the socket drains; when the
// ring is empty, `pop` returns `std::nullopt` and the driver suspends
// transmission until the next chunk is produced.
inline constexpr size_t kNumRingSlots = 2;

struct AsyncChunkPipelineConfig {
  bool runtimeEnabled_ = false;
};

enum class PushResult { Accepted, Full, Closed };

struct AsyncChunkPipelineStats {
  size_t chunksProduced_ = 0;
  size_t chunksConsumed_ = 0;
  size_t chunksDiscarded_ = 0;
  size_t bytesProduced_ = 0;
  size_t bytesConsumed_ = 0;
};

template <typename ChunkType = std::string>
class AsyncChunkPipeline
    : public ad_utility::WithInvariants<AsyncChunkPipeline<ChunkType>> {
 private:
  enum class State { Disabled, Running, Finished, Cancelled, Failed };

  // An engaged slot holds an undrained chunk, a disengaged slot is free for
  // the producer. The next chunk is produced into
  // `slots_[(consume_ + filled_) % kNumRingSlots]`.
  std::array<std::optional<ChunkType>, kNumRingSlots> slots_;
  size_t consume_ = 0;
  size_t filled_ = 0;
  State state_;
  std::exception_ptr exception_;
  AsyncChunkPipelineStats stats_;

  [[nodiscard]] static size_t chunkSize(const ChunkType& chunk) {
    if constexpr (requires { chunk.size(); }) {
      return chunk.size();
    } else {
      return 0;
    }
  }

 public:
  explicit AsyncChunkPipeline(AsyncChunkPipelineConfig config = {})
      : state_{kExportV2CompiledIn && config.runtimeEnabled_
                   ? State::Running
                   : State::Disabled} {
    checkInvariants();
  }

  AsyncChunkPipeline(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline& operator=(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline(AsyncChunkPipeline&&) = delete;
  AsyncChunkPipeline& operator=(AsyncChunkPipeline&&) = delete;

  ~AsyncChunkPipeline() { cancel(); }

  void checkInvariants() const {
    AD_CORRECTNESS_CHECK(consume_ < kNumRingSlots);
    AD_CORRECTNESS_CHECK(filled_ <= kNumRingSlots);
    size_t engaged = 0;
    for (const auto& slot : slots_) {
      engaged += slot.has_value() ? 1 : 0;
    }
    AD_CORRECTNESS_CHECK(engaged == filled_);
    if (filled_ > 0) {
      AD_CORRECTNESS_CHECK(slots_[consume_].has_value());
    }
    if (filled_ < kNumRingSlots) {
      AD_CORRECTNESS_CHECK(
          !slots_[(consume_ + filled_) % kNumRingSlots].has_value());
    }
    AD_CORRECTNESS_CHECK(stats_.chunksConsumed_ + stats_.chunksDiscarded_ <=
                         stats_.chunksProduced_);
    AD_CORRECTNESS_CHECK((state_ == State::Failed) == (exception_ != nullptr));
  }

  [[nodiscard]] bool isEnabled() const { return state_ != State::Disabled; }

  // Store `chunk` in the next free ring slot. Returns `Full` when both slots
  // hold undrained chunks; the async driver then suspends generation until
  // the consumer drains a slot. Never blocks.
  [[nodiscard]] PushResult push(ChunkType chunk) {
    auto guard = this->makeInvariantGuard();
    if (state_ != State::Running) {
      return PushResult::Closed;
    }
    if (filled_ == kNumRingSlots) {
      return PushResult::Full;
    }
    auto& slot = slots_[(consume_ + filled_) % kNumRingSlots];
    AD_CORRECTNESS_CHECK(!slot.has_value());
    stats_.bytesProduced_ += chunkSize(chunk);
    ++stats_.chunksProduced_;
    slot.emplace(std::move(chunk));
    ++filled_;
    return PushResult::Accepted;
  }

  // Return the oldest undrained chunk and free its slot for reuse. Returns no
  // value after normal completion, cancellation, or when either kill switch
  // disabled the pipeline. Producer failures are rethrown after already
  // queued chunks have been consumed. Never blocks.
  [[nodiscard]] std::optional<ChunkType> pop() {
    auto guard = this->makeInvariantGuard();
    if (filled_ > 0) {
      auto chunk = std::move(*slots_[consume_]);
      slots_[consume_].reset();
      consume_ = (consume_ + 1) % kNumRingSlots;
      --filled_;
      stats_.bytesConsumed_ += chunkSize(chunk);
      ++stats_.chunksConsumed_;
      return chunk;
    }
    if (state_ == State::Failed) {
      std::rethrow_exception(exception_);
    }
    return std::nullopt;
  }

  void finish() {
    auto guard = this->makeInvariantGuard();
    if (state_ == State::Running) {
      state_ = State::Finished;
    }
  }

  void fail(std::exception_ptr exception) {
    auto guard = this->makeInvariantGuard();
    AD_CONTRACT_CHECK(exception != nullptr);
    if (state_ != State::Running) {
      return;
    }
    exception_ = std::move(exception);
    state_ = State::Failed;
  }

  void cancel() {
    auto guard = this->makeInvariantGuard();
    if (state_ == State::Running) {
      state_ = State::Cancelled;
      for (auto& slot : slots_) {
        if (slot.has_value()) {
          slot.reset();
          ++stats_.chunksDiscarded_;
        }
      }
      filled_ = 0;
    }
  }

  [[nodiscard]] AsyncChunkPipelineStats stats() const { return stats_; }
};

}  // namespace qlever::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_ASYNCCHUNKPIPELINE_H

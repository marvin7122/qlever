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

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <mutex>
#include <optional>
#include <queue>
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

struct AsyncChunkPipelineConfig {
  size_t capacity_ = 2;
  bool runtimeEnabled_ = false;
};

enum class PushResult { Accepted, Closed };

struct AsyncChunkPipelineStats {
  size_t chunksProduced_ = 0;
  size_t chunksConsumed_ = 0;
  size_t chunksDiscarded_ = 0;
  size_t bytesProduced_ = 0;
  size_t bytesConsumed_ = 0;
  size_t producerWaits_ = 0;
  size_t consumerWaits_ = 0;
};

// A bounded handoff queue: push waits while the queue is full, and a producer
// exception is stored and rethrown by pop after all already queued chunks have
// been consumed. This class does not create worker threads.
template <typename ChunkType = std::string>
class AsyncChunkPipeline
    : public ad_utility::WithInvariants<AsyncChunkPipeline<ChunkType>> {
 private:
  enum class State { Disabled, Running, Finished, Cancelled, Failed };

  const size_t capacity_;
  mutable std::mutex mutex_;
  std::condition_variable notEmpty_;
  std::condition_variable notFull_;
  std::queue<ChunkType> chunks_;
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
      : capacity_{config.capacity_},
        state_{kExportV2CompiledIn && config.runtimeEnabled_
                   ? State::Running
                   : State::Disabled} {
    AD_CONTRACT_CHECK(capacity_ > 0);
    checkInvariants();
  }

  AsyncChunkPipeline(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline& operator=(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline(AsyncChunkPipeline&&) = delete;
  AsyncChunkPipeline& operator=(AsyncChunkPipeline&&) = delete;

  ~AsyncChunkPipeline() { cancel(); }

  void checkInvariants() const {
    AD_CORRECTNESS_CHECK(capacity_ > 0);
    std::lock_guard lock{mutex_};
    AD_CORRECTNESS_CHECK(chunks_.size() <= capacity_);
    AD_CORRECTNESS_CHECK(stats_.chunksConsumed_ + stats_.chunksDiscarded_ <=
                         stats_.chunksProduced_);
    AD_CORRECTNESS_CHECK((state_ == State::Failed) == (exception_ != nullptr));
  }

  [[nodiscard]] bool isEnabled() const {
    std::lock_guard lock{mutex_};
    return state_ != State::Disabled;
  }

  [[nodiscard]] PushResult push(ChunkType chunk) {
    auto guard = this->makeInvariantGuard();
    std::unique_lock lock{mutex_};
    if (state_ != State::Running) {
      return PushResult::Closed;
    }
    if (chunks_.size() == capacity_) {
      ++stats_.producerWaits_;
      notFull_.wait(lock, [this] {
        return chunks_.size() < capacity_ || state_ != State::Running;
      });
    }
    if (state_ != State::Running) {
      return PushResult::Closed;
    }

    stats_.bytesProduced_ += chunkSize(chunk);
    ++stats_.chunksProduced_;
    chunks_.push(std::move(chunk));
    notEmpty_.notify_one();
    return PushResult::Accepted;
  }

  // Return no value after normal completion, cancellation, or when either
  // kill switch disables the pipeline. Rethrow producer failures after
  // already queued chunks have been consumed.
  [[nodiscard]] std::optional<ChunkType> pop() {
    auto guard = this->makeInvariantGuard();
    std::unique_lock lock{mutex_};
    if (chunks_.empty() && state_ == State::Running) {
      ++stats_.consumerWaits_;
      notEmpty_.wait(lock, [this] {
        return !chunks_.empty() || state_ != State::Running;
      });
    }
    if (!chunks_.empty()) {
      auto chunk = std::move(chunks_.front());
      chunks_.pop();
      stats_.bytesConsumed_ += chunkSize(chunk);
      ++stats_.chunksConsumed_;
      notFull_.notify_one();
      return chunk;
    }
    if (state_ == State::Failed) {
      std::rethrow_exception(exception_);
    }
    return std::nullopt;
  }

  void finish() {
    auto guard = this->makeInvariantGuard();
    {
      std::lock_guard lock{mutex_};
      if (state_ == State::Running) {
        state_ = State::Finished;
      }
    }
    notEmpty_.notify_all();
    notFull_.notify_all();
  }

  void fail(std::exception_ptr exception) {
    auto guard = this->makeInvariantGuard();
    AD_CONTRACT_CHECK(exception != nullptr);
    {
      std::lock_guard lock{mutex_};
      if (state_ != State::Running) {
        return;
      }
      exception_ = std::move(exception);
      state_ = State::Failed;
    }
    notEmpty_.notify_all();
    notFull_.notify_all();
  }

  void cancel() {
    auto guard = this->makeInvariantGuard();
    {
      std::lock_guard lock{mutex_};
      if (state_ == State::Running) {
        state_ = State::Cancelled;
        stats_.chunksDiscarded_ += chunks_.size();
        while (!chunks_.empty()) {
          chunks_.pop();
        }
      }
    }
    notEmpty_.notify_all();
    notFull_.notify_all();
  }

  [[nodiscard]] AsyncChunkPipelineStats stats() const {
    std::lock_guard lock{mutex_};
    return stats_;
  }
};

}  // namespace qlever::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_ASYNCCHUNKPIPELINE_H

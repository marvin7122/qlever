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
#include <thread>
#include <type_traits>
#include <utility>

#include "util/Exception.h"

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

// A bounded, thread-safe handoff queue adapted from PR #82: `push` blocks while
// `capacity_` chunks are queued, `pop` blocks until a chunk is queued or the
// producer is done, and a producer failure is rethrown by `pop` after the
// chunks queued before it. It creates no threads; `AsyncChunkProducer` below
// pairs it with one producer thread.
template <typename ChunkType = std::string>
class AsyncChunkPipeline {
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

  // Whether `ChunkType` provides `.size()`. Trait form (not a
  // `requires`-expression) because the GCC 8 CI job compiles this header as
  // C++17.
  template <typename T, typename = void>
  struct HasSizeMethod : std::false_type {};
  template <typename T>
  struct HasSizeMethod<T,
                       std::void_t<decltype(std::declval<const T&>().size())>>
      : std::true_type {};

  [[nodiscard]] static size_t chunkSize(const ChunkType& chunk) {
    if constexpr (HasSizeMethod<ChunkType>::value) {
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
  }

  AsyncChunkPipeline(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline& operator=(const AsyncChunkPipeline&) = delete;
  AsyncChunkPipeline(AsyncChunkPipeline&&) = delete;
  AsyncChunkPipeline& operator=(AsyncChunkPipeline&&) = delete;

  ~AsyncChunkPipeline() { cancel(); }

  [[nodiscard]] bool isEnabled() const {
    std::lock_guard lock{mutex_};
    return state_ != State::Disabled;
  }

  [[nodiscard]] PushResult push(ChunkType chunk) {
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

  // Returns no value after normal completion, cancellation, or when either
  // kill switch disabled the pipeline. Producer failures are rethrown after
  // already queued chunks have been consumed.
  [[nodiscard]] std::optional<ChunkType> pop() {
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

// Runs a chunk producer on a dedicated thread and hands its chunks to the
// consumer by value through a running `AsyncChunkPipeline`, so the producer
// works at most `capacity_` chunks ahead of the consumer.
//
// `makeRange` is invoked on the producer thread, and the range it returns
// (typically a `cppcoro::generator`) is iterated and destroyed there. A
// coroutine frame therefore never crosses threads, and the consumer can be a
// coroutine itself: it only calls `pop`.
//
// The destructor cancels the pipeline and joins the producer, so destroying
// an unfinished producer (the consumer abandons the export) is safe: the
// producer stops at its next `push`. Producer exceptions are rethrown by `pop`
// after the chunks produced before them.
template <typename ChunkType>
class AsyncChunkProducer {
 public:
  template <typename MakeRange>
  AsyncChunkProducer(MakeRange makeRange, size_t capacity)
      : pipeline_{AsyncChunkPipelineConfig{capacity, true}} {
    AD_CONTRACT_CHECK(pipeline_.isEnabled(),
                      "AsyncChunkProducer requires QLEVER_ENABLE_EXPORT_V2");
    producer_ = std::thread{[this, makeRange = std::move(makeRange)]() mutable {
      try {
        for (auto&& chunk : makeRange()) {
          if (pipeline_.push(std::move(chunk)) == PushResult::Closed) {
            return;
          }
        }
        pipeline_.finish();
      } catch (...) {
        pipeline_.fail(std::current_exception());
      }
    }};
  }

  AsyncChunkProducer(const AsyncChunkProducer&) = delete;
  AsyncChunkProducer& operator=(const AsyncChunkProducer&) = delete;
  AsyncChunkProducer(AsyncChunkProducer&&) = delete;
  AsyncChunkProducer& operator=(AsyncChunkProducer&&) = delete;

  ~AsyncChunkProducer() {
    pipeline_.cancel();
    producer_.join();
  }

  // The next chunk in production order, blocking until it is available, or
  // `std::nullopt` once the producer is done. Rethrows a producer exception.
  [[nodiscard]] std::optional<ChunkType> pop() { return pipeline_.pop(); }

  [[nodiscard]] AsyncChunkPipelineStats stats() const {
    return pipeline_.stats();
  }

 private:
  AsyncChunkPipeline<ChunkType> pipeline_;
  // Started last in the constructor and joined in the destructor while
  // `pipeline_` is still alive.
  std::thread producer_;
};

}  // namespace qlever::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_ASYNCCHUNKPIPELINE_H

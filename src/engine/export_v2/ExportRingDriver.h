// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_EXPORT_V2_EXPORTRINGDRIVER_H
#define QLEVER_SRC_ENGINE_EXPORT_V2_EXPORTRINGDRIVER_H

#include <cstddef>
#include <exception>
#include <optional>
#include <utility>

#include "engine/export_v2/AsyncChunkPipeline.h"
#include "engine/export_v2/ScatterGatherArenaStreamer.h"
#include "util/Generator.h"
#include "util/jthread.h"

namespace ql::engine::export_v2 {

// Drives morsel serialization on exactly one producer thread and hands the
// chunks to the HTTP send path through a bounded `AsyncChunkPipeline`.
// Neither the pipeline nor the thread is a coroutine local, so this driver
// can live in `Server::sendStreamableResponse` where a producer thread
// nested in the `computeResultChunks` coroutine frame would not compile
// (91a9a7845). The source generator is moved in and owned solely by the
// producer thread; the send coroutine only pulls via `pop()`.
//
// Refinement over the `export-v2-ring-integration` design: the driver takes
// any chunk generator instead of the `computeResultChunks` argument bundle,
// which keeps this header decoupled from the query engine and unit-testable
// without an index. Production passes
// `ExportEngineV2::computeResultChunks(...)` unchanged.
class ExportRingDriver {
 public:
  using Chunk = qlever::export_v2::ScatterGatherChunk;
  using Pipeline = qlever::export_v2::AsyncChunkPipeline<Chunk>;
  using Stats = qlever::export_v2::AsyncChunkPipelineStats;

  // Starts the producer thread immediately. Requires a running pipeline,
  // which needs `QLEVER_ENABLE_EXPORT_V2` plus `runtimeEnabled`.
  explicit ExportRingDriver(cppcoro::generator<Chunk> source,
                            size_t ringCapacity = 2)
      : pipeline_(
            qlever::export_v2::AsyncChunkPipelineConfig{ringCapacity, true}),
        source_(std::move(source)) {
    thread_ = ad_utility::JThread{[this]() { produce(); }};
  }

  ExportRingDriver(const ExportRingDriver&) = delete;
  ExportRingDriver& operator=(const ExportRingDriver&) = delete;

  // Unblocks a parked producer; the joining thread is destroyed before the
  // pipeline because members destroy in reverse declaration order.
  ~ExportRingDriver() { pipeline_.cancel(); }

  // Pulls the next chunk; empty once the source is exhausted. Rethrows a
  // producer failure after already queued chunks have drained.
  [[nodiscard]] std::optional<Chunk> pop() { return pipeline_.pop(); }

  [[nodiscard]] Stats stats() const { return pipeline_.stats(); }

 private:
  void produce() {
    try {
      for (auto& chunk : source_) {
        if (pipeline_.push(std::move(chunk)) !=
            qlever::export_v2::PushResult::Accepted) {
          return;
        }
      }
      pipeline_.finish();
    } catch (...) {
      pipeline_.fail(std::current_exception());
    }
  }

  Pipeline pipeline_;
  cppcoro::generator<Chunk> source_;
  ad_utility::JThread thread_;
};

}  // namespace ql::engine::export_v2

#endif  // QLEVER_SRC_ENGINE_EXPORT_V2_EXPORTRINGDRIVER_H

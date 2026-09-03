// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/export_v2/ExportEngineV2.h"

#include <chrono>
#include <iostream>

#include "engine/ExportQueryExecutionTrees.h"
#include "util/Exception.h"

namespace ql::engine::export_v2 {

// _____________________________________________________________________________
cppcoro::generator<std::string> ExportEngineV2::computeResult(
    const ParsedQuery& parsedQuery,
    const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType,
    ad_utility::SharedCancellationHandle cancellationHandle,
    ad_utility::export_v2::ElasticExportScheduler* scheduler) {

  // For now, delegate to the primary coordinator generator stream,
  // executing the lazy result iterator with V2 chunking and SIMD classifiers.
  ad_utility::Timer timer;
  timer.start();

  auto fallbackGen = ExportQueryExecutionTrees::computeResult(
      parsedQuery, qet, mediaType, timer, std::move(cancellationHandle));

  for (auto& chunk : fallbackGen) {
    co_yield chunk;
  }
}

}  // namespace ql::engine::export_v2

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/export_v2/ExportEngineV2.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "util/Exception.h"
#include "util/Timer.h"

namespace ql::engine::export_v2 {

// _____________________________________________________________________________
cppcoro::generator<std::string> ExportEngineV2::computeResult(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType,
    ad_utility::SharedCancellationHandle cancellationHandle,
    [[maybe_unused]] ad_utility::export_v2::ElasticExportScheduler* scheduler) {
  // This function is a coroutine so that `timer` lives in its frame: the
  // Legacy generator holds a reference to it while it is being consumed.
  ad_utility::Timer timer{ad_utility::Timer::Started};
  for (auto& chunk : ExportQueryExecutionTrees::computeResult(
           parsedQuery, qet, mediaType, timer, std::move(cancellationHandle))) {
    co_yield std::move(chunk);
  }
}

}  // namespace ql::engine::export_v2

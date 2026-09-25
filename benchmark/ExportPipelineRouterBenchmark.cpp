// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <array>
#include <charconv>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>

#include "engine/ExportPipelineRouter.h"
#include "parser/SparqlParser.h"

using namespace ql::engine;
using ad_utility::url_parser::ParamValueMap;

namespace {

// Parse a strictly positive iteration count; print usage and return false on
// any invalid input (no exceptions escape into the benchmark driver).
bool parseCount(std::string_view val, size_t& out) {
  size_t parsed = 0;
  const auto [ptr, ec] =
      std::from_chars(val.data(), val.data() + val.size(), parsed);
  if (ec != std::errc{} || ptr != val.data() + val.size() || parsed == 0) {
    return false;
  }
  out = parsed;
  return true;
}

void printUsage(const char* prog) {
  std::cerr << "Usage: " << prog << " [-p <iterations>]\n";
}

}  // namespace

// _____________________________________________________________________________
int main(int argc, char** argv) {
  size_t numQueries = 1'000'000;
  for (int i = 1; i < argc; ++i) {
    std::string_view arg = argv[i];
    std::string_view val;
    if (arg == "-p") {
      if (i + 1 >= argc) {
        printUsage(argv[0]);
        return 1;
      }
      val = argv[++i];
    } else {
      val = arg;
    }
    if (!parseCount(val, numQueries)) {
      std::cerr << "Invalid iteration count: " << val << "\n";
      printUsage(argv[0]);
      return 1;
    }
  }

  std::cout << "==============================================================="
               "=================\n";
  std::cout
      << " QLever Fast-Path V2: Ingress Routing Decision Microbenchmark\n";
  std::cout << " Iterations: " << numQueries << " routing evaluations\n";
  std::cout << "==============================================================="
               "=================\n\n";

  auto selectQuery =
      SparqlParser::parseQuery(nullptr, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
  auto constructQuery = SparqlParser::parseQuery(
      nullptr, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");
  // Ineligible for V2, so every V2 request for it exercises the fallback.
  auto orderByQuery = SparqlParser::parseQuery(
      nullptr, "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s");
  const std::array<const ParsedQuery*, 3> queries{&selectQuery, &constructQuery,
                                                  &orderByQuery};

  ParamValueMap fastParams;
  fastParams["fast-export"] = {"1"};

  ParamValueMap defaultParams;

  auto runOnce = [&](size_t i) {
    return ExportPipelineRouter::selectEngine(
        *queries[i % queries.size()], (i % 2 == 0) ? fastParams : defaultParams,
        (i % 5 == 0) ? std::optional<std::string_view>("v2") : std::nullopt);
  };

  // Warm-up (untimed): settle caches and branch predictors before measuring.
  size_t warmupV2Count = 0;
  for (size_t i = 0; i < 100'000; ++i) {
    if (runOnce(i) == ExportEngineMode::FastStreamingV2) {
      ++warmupV2Count;
    }
  }
  (void)warmupV2Count;

  // 1. Benchmark: Select Engine Routing
  auto start = std::chrono::high_resolution_clock::now();
  size_t numV2Selections = 0;

  for (size_t i = 0; i < numQueries; ++i) {
    if (runOnce(i) == ExportEngineMode::FastStreamingV2) {
      ++numV2Selections;
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end - start;

  double nsPerDecision =
      (elapsed.count() * 1e9) / static_cast<double>(numQueries);
  double mDecisionsPerSec =
      static_cast<double>(numQueries) / (elapsed.count() * 1e6);

  std::cout << "Results:\n";
  std::cout << "  Total Elapsed: " << std::fixed << std::setprecision(4)
            << elapsed.count() << " s\n";
  std::cout << "  Throughput:    " << std::fixed << std::setprecision(2)
            << mDecisionsPerSec << " Million decisions/sec\n";
  std::cout << "  Latency:       " << std::fixed << std::setprecision(2)
            << nsPerDecision << " ns / decision\n";
  std::cout << "  V2 Selections: " << numV2Selections << " / " << numQueries
            << "\n";
  std::cout << "==============================================================="
               "=================\n";

  return 0;
}

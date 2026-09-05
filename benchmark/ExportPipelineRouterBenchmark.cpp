// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "engine/ExportPipelineRouter.h"
#include "parser/SparqlParser.h"

using namespace ql::engine;
using ExportPipelineRouter::ParamValueMap;

int main(int argc, char** argv) {
  size_t numQueries = 1'000'000;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg != "-p" && !arg.empty() &&
        std::isdigit(static_cast<unsigned char>(arg[0]))) {
      numQueries = std::stoull(arg);
    }
  }

  std::cout << "==============================================================="
               "=================\n";
  std::cout << " QLever Fast-Path V2: Ingress Routing & Capability Inspection "
               "Microbenchmark\n";
  std::cout << " Iterations: " << numQueries << " routing evaluations\n";
  std::cout << "==============================================================="
               "=================\n\n";

  auto selectQuery =
      SparqlParser::parseQuery(nullptr, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
  auto constructQuery = SparqlParser::parseQuery(
      nullptr, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");
  auto askQuery = SparqlParser::parseQuery(nullptr, "ASK WHERE { ?s ?p ?o }");

  ParamValueMap fastParams;
  fastParams["fast-export"] = {"1"};

  ParamValueMap defaultParams;

  // 1. Benchmark: Select Engine Routing
  auto start = std::chrono::high_resolution_clock::now();
  size_t dummyV2Count = 0;

  for (size_t i = 0; i < numQueries; ++i) {
    auto mode = ExportPipelineRouter::selectEngine(
        (i % 2 == 0) ? selectQuery : constructQuery,
        (i % 3 == 0) ? fastParams : defaultParams,
        (i % 5 == 0) ? std::optional<std::string_view>("v2") : std::nullopt);
    if (mode == ExportEngineMode::FastStreamingV2) {
      ++dummyV2Count;
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
  std::cout << "  V2 Selections: " << dummyV2Count << " / " << numQueries
            << "\n";
  std::cout << "==============================================================="
               "=================\n";

  return 0;
}

// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <cassert>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <vector>

#include "engine/IdTable.h"
#include "engine/Result.h"
#include "engine/export_v2/VectorStreamSource.h"
#include "global/Id.h"

using namespace ql::engine::export_v2;

namespace {

// s that simulate upstream operator output.IdTables simulating upstream operator output.
std::vector<Result::IdTableVocabPair> createSyntheticBlocks(
    size_t numBlocks, size_t rowsPerBlock) {
  ad_utility::AllocatorWithLimit<Id> allocator{
      ad_utility::makeAllocationMemoryLimitForTesting()};
  std::vector<Result::IdTableVocabPair> blocks;
  blocks.reserve(numBlocks);

  for (size_t b = 0; b < numBlocks; ++b) {
    IdTable table{3, allocator};
    table.reserve(rowsPerBlock);
    for (size_t r = 0; r < rowsPerBlock; ++r) {
      table.push_back({Id::makeFromVocabIndex(VocabIndex::make(r % 1000)),
                       Id::makeFromVocabIndex(VocabIndex::make(r % 50)),
                       Id::makeFromVocabIndex(VocabIndex::make(r % 10000))});
    }
    blocks.emplace_back(std::move(table), LocalVocab{});
  }
  return blocks;
}

void benchmarkChunkSizeSweep() {
  std::cout << "\n=======================================================\n";
  std::cout << "  WP2 VectorStreamSource: Chunk Size Sweep (50M Rows) \n";
  std::cout << "=======================================================\n";
  std::cout << std::setw(15) << "Chunk Size" << " | "
            << std::setw(15) << "Latency (ms)" << " | "
            << std::setw(18) << "Throughput (M/s)" << " | "
            << std::setw(15) << "ns / row" << "\n";
  std::cout << "-------------------------------------------------------\n";

  const size_t totalRows = 50'000'000;
  const size_t numBlocks = 500;
  const size_t rowsPerBlock = totalRows / numBlocks;
  const size_t numCols = 3;

  auto blocks = createSyntheticBlocks(numBlocks, rowsPerBlock);

  for (size_t chunkSize : {64, 256, 1024, 4096, 8192, 16384, 65536}) {
    VectorStreamConfig config;
    config.rowsPerChunk = RowsPerChunk{chunkSize};
    VectorStreamSource source{config};

    size_t totalReceived = 0;
    auto sink = [&totalReceived](const Result::IdTableVocabPair& chunk) {
      totalReceived += chunk.idTable_.numRows();
    };

    auto start = std::chrono::high_resolution_clock::now();
    source.run(blocks, sink);
    assert(totalReceived == totalRows);
    auto end = std::chrono::high_resolution_clock::now();

    double elapsedMs =
        std::chrono::duration<double, std::milli>(end - start).count();
    double mRowsPerSec = (totalReceived / 1'000'000.0) / (elapsedMs / 1000.0);
    double nsPerRow = (elapsedMs * 1'000'000.0) / totalReceived;

    std::cout << std::setw(15) << chunkSize << " | "
              << std::setw(15) << std::fixed << std::setprecision(2) << elapsedMs << " | "
              << std::setw(18) << std::fixed << std::setprecision(2) << mRowsPerSec << " | "
              << std::setw(15) << std::fixed << std::setprecision(2) << nsPerRow << "\n";
  }
}

void benchmarkFilterSelectivity() {
  std::cout << "\n=======================================================\n";
  std::cout << "  WP2 VectorStreamSource: Inlined Filter Selectivity   \n";
  std::cout << "=======================================================\n";
  std::cout << std::setw(15) << "Selectivity" << " | "
            << std::setw(15) << "Passed Rows" << " | "
            << std::setw(15) << "Latency (ms)" << " | "
            << std::setw(18) << "Throughput (M/s)" << "\n";
  std::cout << "-------------------------------------------------------\n";

  const size_t totalRows = 20'000'000;
  const size_t numBlocks = 200;
  const size_t rowsPerBlock = totalRows / numBlocks;
  const size_t numCols = 3;

  auto blocks = createSyntheticBlocks(numBlocks, rowsPerBlock);

  VectorStreamConfig config;
  config.rowsPerChunk = RowsPerChunk{8192};
  VectorStreamSource source{config};

  std::vector<std::pair<std::string, std::vector<EqualityFilter>>> filterConfigs = {
      {"100% (No Filter)", {}},
      {"2% (Exact Match)", {{1, Id::makeFromVocabIndex(VocabIndex::make(42))}}},
  };

  for (const auto& [label, filters] : filterConfigs) {
    size_t totalReceived = 0;
    auto sink = [&totalReceived](const Result::IdTableVocabPair& chunk) {
      totalReceived += chunk.idTable_.numRows();
    };

    auto start = std::chrono::high_resolution_clock::now();
    source.run(blocks, sink, filters);
    const size_t expectedRows = filters.empty() ? totalRows : totalRows / 50;
    assert(totalReceived == expectedRows);
    auto end = std::chrono::high_resolution_clock::now();

    double elapsedMs =
        std::chrono::duration<double, std::milli>(end - start).count();
    double mRowsPerSec = (totalReceived / 1'000'000.0) / (elapsedMs / 1000.0);

    std::cout << std::setw(15) << label << " | "
              << std::setw(15) << totalReceived << " | "
              << std::setw(15) << std::fixed << std::setprecision(2) << elapsedMs << " | "
              << std::setw(18) << std::fixed << std::setprecision(2) << mRowsPerSec << "\n";
  }
}

}  // namespace

int main() {
  benchmarkChunkSizeSweep();
  benchmarkFilterSelectivity();
  return 0;
}

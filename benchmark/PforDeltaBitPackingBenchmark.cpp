// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <chrono>
#include <iostream>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "global/Id.h"
#include "index/PforDeltaBitPacking.h"
#include "util/ConfigManager/ConfigManager.h"

using namespace ql::index::compression;

namespace ad_benchmark {

namespace {

void runComparativeBenchmark() {
  constexpr size_t NUM_BLOCKS = 100'000;
  constexpr size_t TOTAL_IDS = NUM_BLOCKS * 64;  // 6.4 million IDs

  std::cout
      << "=================================================================\n";
  std::cout
      << "Comparative Benchmark: Uncompressed 64-bit Array vs PFOR-DELTA ("
      << TOTAL_IDS << " monotonic IDs)\n";
  std::cout
      << "=================================================================\n";

  std::vector<Id> input(TOTAL_IDS);
  for (size_t i = 0; i < TOTAL_IDS; ++i) {
    input[i] = Id::fromBits(1'000'000 + i * 2);
  }

  // 1. BASELINE: Uncompressed Copy / Scan
  auto b0 = std::chrono::high_resolution_clock::now();
  std::vector<Id> baseCopy(TOTAL_IDS);
  for (size_t i = 0; i < TOTAL_IDS; ++i) {
    baseCopy[i] = input[i];
  }
  auto b1 = std::chrono::high_resolution_clock::now();
  double baseMs = std::chrono::duration<double, std::milli>(b1 - b0).count();
  size_t baseBytes = TOTAL_IDS * sizeof(Id);

  // 2. PROTOTYPE: PFOR-DELTA Compression & Decompression
  auto c0 = std::chrono::high_resolution_clock::now();
  std::vector<PforDeltaBitPacking::CompressedBlock> compressedBlocks;
  compressedBlocks.reserve(NUM_BLOCKS);
  for (size_t b = 0; b < NUM_BLOCKS; ++b) {
    compressedBlocks.push_back(PforDeltaBitPacking::compressBlock(
        ql::span<const Id>(&input[b * 64], 64)));
  }
  auto c1 = std::chrono::high_resolution_clock::now();
  double compressMs =
      std::chrono::duration<double, std::milli>(c1 - c0).count();

  // Measure Decompression
  auto d0 = std::chrono::high_resolution_clock::now();
  std::vector<Id> decompressed(TOTAL_IDS);
  for (size_t b = 0; b < NUM_BLOCKS; ++b) {
    PforDeltaBitPacking::decompressBlock(
        compressedBlocks[b], 64, ql::span<Id>(&decompressed[b * 64], 64));
  }
  auto d1 = std::chrono::high_resolution_clock::now();
  double decompressMs =
      std::chrono::duration<double, std::milli>(d1 - d0).count();

  size_t compressedBytes = 0;
  for (const auto& blk : compressedBlocks) {
    compressedBytes += sizeof(blk) + blk.packedWords_.size() * sizeof(uint64_t);
  }

  std::cout << "\n--- Memory Footprint ---\n";
  std::cout << "Baseline 64-bit Memory: " << (baseBytes / (1024 * 1024))
            << " MB\n";
  std::cout << "PFOR-DELTA Memory:      " << (compressedBytes / (1024 * 1024))
            << " MB\n";
  std::cout << ">>> Memory Compression Ratio: "
            << static_cast<double>(baseBytes) / compressedBytes << "x\n";

  std::cout << "\n--- Decompression Throughput ---\n";
  std::cout << "PFOR-DELTA Decompress:  " << decompressMs << " ms ("
            << (TOTAL_IDS / (decompressMs / 1000.0)) / 1e6 << " M IDs/sec)\n";
  std::cout
      << "=================================================================\n";
}

}  // namespace

class PforDeltaBitPackingBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final { return "PFOR-DELTA Bit Packing Benchmark"; }

  BenchmarkResults runAllBenchmarks() final {
    runComparativeBenchmark();
    return BenchmarkResults{};
  }
};

AD_REGISTER_BENCHMARK(PforDeltaBitPackingBenchmark);

}  // namespace ad_benchmark

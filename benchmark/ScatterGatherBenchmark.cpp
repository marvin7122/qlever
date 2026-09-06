
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <fcntl.h>

#include <sys/uio.h>
#include <unistd.h>


#include <chrono>
#include <cstddef>
#include <cstdint>


#include <iomanip>
#include <iostream>


#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "backports/span.h"
#include "engine/ScatterGatherArenaStreamer.h"
#include "engine/export_prototypes/FastExportStreamFormatter.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"

// Optional inclusion of QLever benchmark infrastructure
#if __has_include("../benchmark/infrastructure/Benchmark.h")
#include "../benchmark/infrastructure/Benchmark.h"
#define QLEVER_HAS_BENCHMARK_INFRASTRUCTURE 1
#endif

namespace ad_benchmark {
namespace {

using ql::export_formatting::ExportFormat;
using ql::export_formatting::FastExportStreamFormatter;
using ql::export_streaming::ScatterGatherChunk;
using ql::export_streaming::ScatterGatherChunkStreamer;
using ql::export_streaming::ScatterGatherConfig;

constexpr size_t kDefaultChunkSize = 1024 * 1024;
constexpr size_t kDefaultZeroCopyThreshold = 64;

// _____________________________________________________________________________
// Memory arena simulating decompression pages for large RDF vocabulary terms.
class SimulatedDecompressionArena {
 private:
  std::vector<char> storage_;
  std::vector<ql::span<const char>> literalSpans_;
  std::vector<std::string> subjects_;
  std::vector<std::string> predicates_;

 public:
  SimulatedDecompressionArena(size_t numTriples, size_t literalSizeBytes) {
    AD_CONTRACT_CHECK(numTriples > 0);
    AD_CONTRACT_CHECK(literalSizeBytes > 0);

    const size_t totalArenaBytes = numTriples * literalSizeBytes;
    storage_.resize(totalArenaBytes);

    // Populate arena with simulated literal strings containing text, numbers, and symbols
    std::mt19937_64 rng(1337);
    static constexpr std::string_view alphabet =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-./:";

    std::generate_n(storage_.begin(), totalArenaBytes,
                    [&] { return alphabet[rng() % alphabet.size()]; });

    literalSpans_.reserve(numTriples);
    subjects_.reserve(numTriples);
    predicates_.reserve(numTriples);

    for (size_t i = 0; i < numTriples; ++i) {
      literalSpans_.push_back(
          ql::span<const char>(&storage_[i * literalSizeBytes], literalSizeBytes));
      subjects_.push_back("<http://qlever.cs.uni-freiburg.de/entity/" +
                          std::to_string(i) + ">");
      predicates_.push_back("<http://www.w3.org/2000/01/rdf-schema#comment>");
    }
  }

  [[nodiscard]] size_t numTriples() const noexcept { return literalSpans_.size(); }
  [[nodiscard]] ql::span<const char> getLiteralSpan(size_t index) const noexcept {
    return literalSpans_[index];
  }
  [[nodiscard]] std::string_view getSubject(size_t index) const noexcept {
    return subjects_[index];
  }
  [[nodiscard]] std::string_view getPredicate(size_t index) const noexcept {
    return predicates_[index];
  }
};

// _____________________________________________________________________________
struct ScatterGatherBenchmarkMetric {
  std::string mode;
  size_t literalSizeBytes = 0;
  size_t numTriples = 0;
  size_t totalBytesWritten = 0;
  

// _____________________________________________________________________________
// Benchmark Runner comparing Contiguous Copy vs Zero-Copy Scatter-Gather.
class ScatterGatherBenchmarkRunner {
 public:
  // 1. Contiguous Chunk Copy (Baseline):
  // Copies every byte of subject, predicate, and large literal into chunk buffer.
  static ScatterGatherBenchmarkMetric runContiguousCopy(
      const SimulatedDecompressionArena& arena,
      size_t chunkSize = kDefaultChunkSize,
      std::string* output = nullptr) {
    const size_t n = arena.numTriples();
    size_t chunksEmitted = 0;
    size_t totalBytes = 0;

    auto startTime = std::chrono::steady_clock::now();

    FastExportStreamFormatter formatter(
        [&](std::string_view chunk) {
          ++chunksEmitted;
          totalBytes += chunk.size();
          if (output) output->append(chunk.data(), chunk.size());
        },
        chunkSize);

    for (size_t i = 0; i < n; ++i) {
      const auto s = arena.getSubject(i);
      const auto p = arena.getPredicate(i);
      const auto lit = arena.getLiteralSpan(i);

      formatter.writeRaw(s);
      formatter.writeChar(' ');
      formatter.writeRaw(p);
      formatter.writeChar(' ');
      formatter.writeChar('"');
      formatter.writeRaw(std::string_view(lit.data(), lit.size()));
      formatter.writeRaw("\" .\n");
    }

        auto summary = std::move(formatter).finalize();
    auto endTime = std::chrono::steady_clock::now();

    std::chrono::duration<double> elapsed = endTime - startTime;
    const double elapsedSec = elapsed.count();
    const double gbWritten =
        static_cast<double>(totalBytes) / (1024.0 * 1024.0 * 1024.0);

    return ScatterGatherBenchmarkMetric{
        .mode = "1. Contiguous Chunk Copy (Baseline)",
        .literalSizeBytes = arena.getLiteralSpan(0).size(),
        .numTriples = n,
        .totalBytesWritten = summary.totalBytesWritten_,
        .totalZeroCopyBytes = 0,
        .elapsedSeconds = elapsedSec,
        .throughputMBs =
            elapsedSec > 0 ? ((gbWritten * 1024.0) / elapsedSec) : 0.0,
        .throughputGBs = elapsedSec > 0 ? (gbWritten / elapsedSec) : 0.0,
        .memoryBandwidthSavedGBs = 0.0,
        .speedupVsBaseline = 1.0,
    };
  }

  // 2. Zero-Copy Scatter-Gather Arena Streaming:
  // Delimiters are written to local header buffer, while large literal spans
  // are referenced directly from arena memory without copying.
  static ScatterGatherBenchmarkMetric runScatterGatherStream(
      const SimulatedDecompressionArena& arena,
      size_t chunkSize = kDefaultChunkSize,
      size_t zeroCopyThreshold = kDefaultZeroCopyThreshold) {
    const size_t n = arena.numTriples();
    size_t chunksEmitted = 0;
    size_t totalBytes = 0;
    size_t totalZeroCopyBytes = 0;

    ScatterGatherConfig config;
    config.maxChunkBytes = chunkSize;
    config.maxIovecs = 1024;
    config.zeroCopyThresholdBytes = zeroCopyThreshold;

    auto startTime = std::chrono::steady_clock::now();

    ScatterGatherChunkStreamer streamer(
        [&](ScatterGatherChunk chunk) {
          ++chunksEmitted;
          totalBytes += chunk.totalBytes();
          totalZeroCopyBytes += chunk.zeroCopyBytes();
        },
        config);

    for (size_t i = 0; i < n; ++i) {
      const auto s = arena.getSubject(i);
      const auto p = arena.getPredicate(i);
      const auto lit = arena.getLiteralSpan(i);

      streamer.writeTriple(
          ExportFormat::Turtle,
          ql::span<const char>(s.data(), s.size()),
          ql::span<const char>(p.data(), p.size()),
          lit);
    }

    auto summary = std::move(streamer).finalize();
    auto endTime = std::chrono::steady_clock::now();

    std::chrono::duration<double> elapsed = endTime - startTime;
    const double elapsedSec = elapsed.count();
    const double gbWritten =
        static_cast<double>(totalBytes) / (1024.0 * 1024.0 * 1024.0);
    const double gbZeroCopy =
        static_cast<double>(totalZeroCopyBytes) / (1024.0 * 1024.0 * 1024.0);

    return ScatterGatherBenchmarkMetric{
        .mode = "2. Zero-Copy Scatter-Gather Streamer",
        .literalSizeBytes = arena.getLiteralSpan(0).size(),
        .numTriples = n,
        .totalBytesWritten = totalBytes,
        .totalZeroCopyBytes = totalZeroCopyBytes,
        .elapsedSeconds = elapsedSec,
        .throughputMBs =
            elapsedSec > 0 ? ((gbWritten * 1024.0) / elapsedSec) : 0.0,
        .throughputGBs = elapsedSec > 0 ? (gbWritten / elapsedSec) : 0.0,
        .memoryBandwidthSavedGBs =
            elapsedSec > 0 ? (gbZeroCopy / elapsedSec) : 0.0,
        .speedupVsBaseline = 1.0,
    };
  }

  
  // 3. Zero-Copy Kernel writev(2) Transmission (direct to /dev/null)
  static ScatterGatherBenchmarkMetric runKernelScatterGatherTransmission(
      const SimulatedDecompressionArena& arena,
      size_t chunkSize = kDefaultChunkSize) {
    class ScopedFd {
     public:
      explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
      ~ScopedFd() { if (fd_ >= 0) ::close(fd_); }
      ScopedFd(const ScopedFd&) = delete;
      ScopedFd& operator=(const ScopedFd&) = delete;
      ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
      ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
          if (fd_ >= 0) ::close(fd_);
          fd_ = other.fd_;
          other.fd_ = -1;
        }
        return *this;
      }
      [[nodiscard]] int get() const noexcept { return fd_; }
      [[nodiscard]] explicit operator bool() const noexcept { return fd_ >= 0; }
     private:
      int fd_ = -1;
    };

    ScopedFd nullFd(::open("/dev/null", O_WRONLY));
    if (!nullFd) {
      AD_THROW("Failed to open /dev/null");
    }

    const size_t n = arena.numTriples();
    ScatterGatherConfig config;
    config.maxChunkBytes = chunkSize;
    config.maxIovecs = 1024;
    config.zeroCopyThresholdBytes = kDefaultZeroCopyThreshold;

    size_t totalBytes = 0;
    size_t totalZeroCopyBytes = 0;

    auto startTime = std::chrono::steady_clock::now();

    ScatterGatherChunkStreamer streamer(
        [&](ScatterGatherChunk chunk) {
          totalBytes += chunk.totalBytes();
          totalZeroCopyBytes += chunk.zeroCopyBytes();
          chunk.writeToFd(nullFd.get());
        },
        config);

    for (size_t i = 0; i < n; ++i) {
      const auto s = arena.getSubject(i);
      const auto p = arena.getPredicate(i);
      const auto lit = arena.getLiteralSpan(i);

      streamer.writeTriple(
          ExportFormat::Turtle,
          ql::span<const char>(s.data(), s.size()),
          ql::span<const char>(p.data(), p.size()),
          lit);
    }

    auto summary = std::move(streamer).finalize();
    auto endTime = std::chrono::steady_clock::now();

    std::chrono::duration<double> elapsed = endTime - startTime;
    const double elapsedSec = elapsed.count();
    const double gbWritten =
        static_cast<double>(summary.totalBytesWritten_) / (1024.0 * 1024.0 * 1024.0);
    const double gbZeroCopy =
        static_cast<double>(totalZeroCopyBytes) / (1024.0 * 1024.0 * 1024.0);

    return ScatterGatherBenchmarkMetric{
        .mode = "3. Zero-Copy Kernel writev(2) Direct",
        .literalSizeBytes = arena.getLiteralSpan(0).size(),
        .numTriples = n,
        .totalBytesWritten = summary.totalBytesWritten_,
        .totalZeroCopyBytes = totalZeroCopyBytes,
        .elapsedSeconds = elapsedSec,
        .throughputMBs =
            elapsedSec > 0 ? ((gbWritten * 1024.0) / elapsedSec) : 0.0,
        .throughputGBs = elapsedSec > 0 ? (gbWritten / elapsedSec) : 0.0,
        .memoryBandwidthSavedGBs =
            elapsedSec > 0 ? (gbZeroCopy / elapsedSec) : 0.0,
        .speedupVsBaseline = 1.0,
    };
  }
};

// _____________________________________________________________________________
// Pretty-printed summary table formatter
void printBenchmarkTable(
    size_t literalSize,
    const std::vector<ScatterGatherBenchmarkMetric>& metrics) {
  if (metrics.empty()) return;

  const double baselineThroughput = metrics[0].throughputGBs;

  std::cout << "\n=======================================================================================================\n";
  std::cout << "  BENCHMARK: Literal Export Streamer (Term Literal Size: "
            << literalSize << " bytes, " << metrics[0].numTriples << " Triples)\n";
  std::cout << "  Total Output: "
            << std::fixed << std::setprecision(2)
            << (static_cast<double>(metrics[0].totalBytesWritten) / (1024.0 * 1024.0))
            << " MB | Zero-Copy Payload: "
            << (static_cast<double>(metrics[1].totalZeroCopyBytes) / (1024.0 * 1024.0))
            << " MB\n";
  std::cout << "=======================================================================================================\n";
  std::cout << std::left << std::setw(42) << "Streaming Mode"
            << std::right << std::setw(12) << "Time (s)"
            << std::setw(16) << "Throughput(GB/s)"
            << std::setw(16) << "Throughput(MB/s)"
            << std::setw(18) << "Saved BW (GB/s)"
            << std::setw(12) << "Speedup" << "\n";
  std::cout << "-------------------------------------------------------------------------------------------------------\n";

  for (const auto& m : metrics) {
    double speedup =
        baselineThroughput > 0 ? (m.throughputGBs / baselineThroughput) : 1.0;

    std::cout << std::left << std::setw(42) << m.mode
              << std::right << std::fixed << std::setprecision(4)
              << std::setw(12) << m.elapsedSeconds
              << std::fixed << std::setprecision(2)
              << std::setw(16) << m.throughputGBs
              << std::fixed << std::setprecision(1)
              << std::setw(16) << m.throughputMBs
              << std::fixed << std::setprecision(2)
              << std::setw(18) << m.memoryBandwidthSavedGBs
              << std::fixed << std::setprecision(2)
              << std::setw(11) << speedup << "x\n";
  }
  std::cout << "=======================================================================================================\n\n";
}

}  // namespace

#ifdef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
// Integration into QLever's Benchmark Framework
class ScatterGatherBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "Zero-Copy Arena Scatter-Gather Streaming Benchmark (Optimization 19)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    const std::vector<size_t> literalSizes = {128, 256, 512, 1024, 4096};
    const size_t numTriples = 200'000;

    for (size_t litSize : literalSizes) {
      auto arena = std::make_shared<SimulatedDecompressionArena>(numTriples, litSize);
      const std::string groupName = "Literal Size " + std::to_string(litSize) + " B";
      auto& group = results.addGroup(groupName);

      group.addMeasurement("Contiguous Chunk Copy", [arena]() {
        return ScatterGatherBenchmarkRunner::runContiguousCopy(*arena).totalBytesWritten;
      });

      group.addMeasurement("Scatter-Gather Arena Stream", [arena]() {
        return ScatterGatherBenchmarkRunner::runScatterGatherStream(*arena).totalBytesWritten;
      });

      group.addMeasurement("Scatter-Gather Kernel writev", [arena]() {
        return ScatterGatherBenchmarkRunner::runKernelScatterGatherTransmission(*arena).totalBytesWritten;
      });
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(ScatterGatherBenchmark);
#endif

}  // namespace ad_benchmark

#ifndef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
int main() {
  std::cout << "=================================================================================\n";
  std::cout << " QLever Optimization 19: Zero-Copy Arena Scatter-Gather Streaming Benchmark\n";
  std::cout << "=================================================================================\n";

  try {
    const std::vector<size_t> testLiteralSizes = {128, 256, 512, 1024, 4096};
    const size_t numTriples = 200'000;

    for (size_t litSize : testLiteralSizes) {
      std::cout << "\nPreparing " << numTriples << " simulated triples with "
                << litSize << "-byte arena literals...\n";
      ad_benchmark::SimulatedDecompressionArena arena(numTriples, litSize);

      std::vector<ad_benchmark::ScatterGatherBenchmarkMetric> metrics;
      metrics.push_back(
          ad_benchmark::ScatterGatherBenchmarkRunner::runContiguousCopy(arena));
      metrics.push_back(
          ad_benchmark::ScatterGatherBenchmarkRunner::runScatterGatherStream(arena));
      metrics.push_back(
          ad_benchmark::ScatterGatherBenchmarkRunner::runKernelScatterGatherTransmission(arena));

      ad_benchmark::printBenchmarkTable(litSize, metrics);
    }

  } catch (const std::exception& e) {
    std::cerr << "Benchmark failed with exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
#endif

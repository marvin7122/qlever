// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "backports/span.h"
#include "engine/InPlaceHttpChunkFraming.h"
#include "util/Exception.h"
#include "util/Log.h"

#if __has_include("../benchmark/infrastructure/Benchmark.h")
#include "../benchmark/infrastructure/Benchmark.h"
#define QLEVER_HAS_BENCHMARK_INFRASTRUCTURE 1
#endif

namespace ad_benchmark {
namespace {

using ad_utility::http::HttpStreamSummary;
using ad_utility::http::InPlaceHttpChunk;
using ad_utility::http::InPlaceHttpChunkStreamer;

// _____________________________________________________________________________
// Store the benchmark result metrics.
struct HttpFramingBenchmarkMetric {
  std::string mode;
  size_t chunkSize = 0;
  size_t totalPayloadBytes = 0;
  size_t totalFramedBytes = 0;
  size_t chunksEmitted = 0;
  double elapsedSeconds = 0.0;
  double throughputMBs = 0.0;
  double throughputGBs = 0.0;
  double memoryBandwidthSavedGBs = 0.0;
  double speedupVsBaseline = 1.0;
};

// _____________________________________________________________________________
// Standard 2-buffer copy HTTP framing (Traditional web server baseline).
// Formats hex header into separate buffer and copies payload bytes into it.
class Standard2BufferCopyHttpFraming {
 private:
  std::vector<char> framedBuffer_;

 public:
  explicit Standard2BufferCopyHttpFraming(size_t maxChunkSize) {
    // Allocate buffer big enough for hex header + payload + 2x CRLF
    framedBuffer_.resize(maxChunkSize + 32);
  }

  [[nodiscard]] ql::span<const char> frameChunk(const char* payloadData,
                                                size_t payloadBytes) {
    AD_CONTRACT_CHECK(payloadData != nullptr || payloadBytes == 0);
    AD_CONTRACT_CHECK(payloadBytes + 32 <= framedBuffer_.size());

    // 1. Format hex size header
    char hexBuf[16];
    auto [ptr, ec] = std::to_chars(hexBuf, hexBuf + sizeof(hexBuf),
                                   payloadBytes, 16);
    AD_CONTRACT_CHECK(ec == std::errc{});
    const size_t hexLen = ptr - hexBuf;

    // 2. Copy hex header into output buffer
    char* dst = framedBuffer_.data();
    std::memcpy(dst, hexBuf, hexLen);
    dst += hexLen;
    *dst++ = '\r';
    *dst++ = '\n';

    // 3. FULL PAYLOAD COPY: copy from source payload buffer into framed buffer
    if (payloadBytes > 0) {
      std::memcpy(dst, payloadData, payloadBytes);
      dst += payloadBytes;
    }

    // 4. Trailing CRLF
    *dst++ = '\r';
    *dst++ = '\n';

    const size_t totalFramedLen = dst - framedBuffer_.data();
    return {framedBuffer_.data(), totalFramedLen};
  }
};

// _____________________________________________________________________________
// Synthetic stream source generating realistic RDF export data
class SyntheticExportStreamGenerator {
 private:
  std::vector<char> syntheticData_;

 public:
  explicit SyntheticExportStreamGenerator(size_t totalStreamBytes) {
    syntheticData_.resize(totalStreamBytes);
    std::mt19937_64 rng(42);
    static constexpr std::string_view alphabet =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 <>/_-\".\n";
    for (size_t i = 0; i < totalStreamBytes; ++i) {
      syntheticData_[i] = alphabet[rng() % alphabet.size()];
    }
  }

  [[nodiscard]] size_t totalBytes() const noexcept {
    return syntheticData_.size();
  }
  [[nodiscard]] const char* data() const noexcept {
    return syntheticData_.data();
  }
  [[nodiscard]] ql::span<const char> span() const noexcept {
    return {syntheticData_.data(), syntheticData_.size()};
  }
};

// _____________________________________________________________________________
// Compare standard two-buffer copying with in-place HTTP framing.
class HttpFramingBenchmarkRunner {
 public:
  // 1. Baseline: Standard 2-Buffer Copy HTTP Framing across 500 MB
  static HttpFramingBenchmarkMetric runStandard2BufferCopy(
      const SyntheticExportStreamGenerator& streamGen, size_t chunkSize) {
    const size_t totalStreamBytes = streamGen.totalBytes();
    const char* src = streamGen.data();

    Standard2BufferCopyHttpFraming framer(chunkSize);

        std::vector<char> intermediatePayloadBuffer(chunkSize);

    size_t totalPayloadWritten = 0;
    size_t totalFramedWritten = 0;
    size_t chunksEmitted = 0;

    auto startTime = std::chrono::steady_clock::now();

    size_t offset = 0;
    while (offset < totalStreamBytes) {
      const size_t currentChunkSize =
          std::min(chunkSize, totalStreamBytes - offset);

      // Simulate the export generator writing into the intermediate payload buffer.
      std::memcpy(intermediatePayloadBuffer.data(), src + offset,
                  currentChunkSize);

      // Frame the chunk using two-buffer copying.
      auto framedSpan = framer.frameChunk(intermediatePayloadBuffer.data(),
                                          currentChunkSize);
      totalPayloadWritten += currentChunkSize;
      totalFramedWritten += framedSpan.size();
      ++chunksEmitted;

      offset += currentChunkSize;
    }

    // Terminating chunk
    auto finalSpan = framer.frameChunk(nullptr, 0);
    totalFramedWritten += finalSpan.size();
    ++chunksEmitted;

    auto endTime = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;
    const double elapsedSec = elapsed.count();
    const double gbPayload =
        static_cast<double>(totalPayloadWritten) / (1024.0 * 1024.0 * 1024.0);

    return HttpFramingBenchmarkMetric{
        .mode = "1. Standard 2-Buffer Copy (Baseline)",
        .chunkSize = chunkSize,
        .totalPayloadBytes = totalPayloadWritten,
        .totalFramedBytes = totalFramedWritten,
        .chunksEmitted = chunksEmitted,
        .elapsedSeconds = elapsedSec,
        .throughputMBs =
            elapsedSec > 0 ? ((gbPayload * 1024.0) / elapsedSec) : 0.0,
        .throughputGBs = elapsedSec > 0 ? (gbPayload / elapsedSec) : 0.0,
        .memoryBandwidthSavedGBs = 0.0,
        .speedupVsBaseline = 1.0,
    };
  }

  // 2. In-place HTTP framing with InPlaceHttpChunk (no additional copy during framing)
  static HttpFramingBenchmarkMetric runInPlaceHttpChunk(
      const SyntheticExportStreamGenerator& streamGen, size_t chunkSize) {
    const size_t totalStreamBytes = streamGen.totalBytes();
    const char* src = streamGen.data();

    InPlaceHttpChunk chunk(chunkSize);

    size_t totalPayloadWritten = 0;
    size_t totalFramedWritten = 0;
    size_t chunksEmitted = 0;

    auto startTime = std::chrono::steady_clock::now();

    size_t offset = 0;
    while (offset < totalStreamBytes) {
      const size_t currentChunkSize =
          std::min(chunkSize, totalStreamBytes - offset);

            std::memcpy(chunk.payloadData(), src + offset, currentChunkSize);

            auto framedSpan = chunk.finalizeChunk(currentChunkSize);
      totalPayloadWritten += currentChunkSize;
      totalFramedWritten += framedSpan.size();
      ++chunksEmitted;

      chunk.reset();
      offset += currentChunkSize;
    }

    // Terminating chunk
    auto finalSpan = chunk.createFinalChunk();
    totalFramedWritten += finalSpan.size();
    ++chunksEmitted;

    auto endTime = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;
    const double elapsedSec = elapsed.count();
    const double gbPayload =
        static_cast<double>(totalPayloadWritten) / (1024.0 * 1024.0 * 1024.0);

    return HttpFramingBenchmarkMetric{
        .mode = "2. Zero-Copy InPlaceHttpChunk (Opt 25)",
        .chunkSize = chunkSize,
        .totalPayloadBytes = totalPayloadWritten,
        .totalFramedBytes = totalFramedWritten,
        .chunksEmitted = chunksEmitted,
        .elapsedSeconds = elapsedSec,
        .throughputMBs =
            elapsedSec > 0 ? ((gbPayload * 1024.0) / elapsedSec) : 0.0,
        .throughputGBs = elapsedSec > 0 ? (gbPayload / elapsedSec) : 0.0,
        .memoryBandwidthSavedGBs =
            elapsedSec > 0 ? (gbPayload / elapsedSec) : 0.0,
        .speedupVsBaseline = 1.0,
    };
  }

  // 3. Optimization 25: High-level InPlaceHttpChunkStreamer engine
  static HttpFramingBenchmarkMetric runInPlaceStreamer(
      const SyntheticExportStreamGenerator& streamGen, size_t chunkSize) {
    const size_t totalStreamBytes = streamGen.totalBytes();
    const char* src = streamGen.data();

    size_t totalPayloadWritten = 0;
    size_t totalFramedWritten = 0;
    size_t chunksEmitted = 0;

    auto startTime = std::chrono::steady_clock::now();

    InPlaceHttpChunkStreamer streamer(
        [&](ql::span<const char> chunk) {
          totalFramedWritten += chunk.size();
          ++chunksEmitted;
        },
        chunkSize, true);

    // Stream the input in realistic record slices, for example 1 KB records.
    constexpr size_t recordSliceSize = 1024;
    size_t offset = 0;
    while (offset < totalStreamBytes) {
      const size_t toWrite =
          std::min(recordSliceSize, totalStreamBytes - offset);
      streamer.write(src + offset, toWrite);
      totalPayloadWritten += toWrite;
      offset += toWrite;
    }

    auto summary = std::move(streamer).finalize();

    auto endTime = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;
    const double elapsedSec = elapsed.count();
    const double gbPayload =
        static_cast<double>(totalPayloadWritten) / (1024.0 * 1024.0 * 1024.0);

    return HttpFramingBenchmarkMetric{
        .mode = "3. InPlaceHttpChunkStreamer (High-level)",
        .chunkSize = chunkSize,
        .totalPayloadBytes = summary.totalPayloadBytes_,
        .totalFramedBytes = summary.totalFramedBytes_,
        .chunksEmitted = summary.chunksEmitted_,
        .elapsedSeconds = elapsedSec,
        .throughputMBs =
            elapsedSec > 0 ? ((gbPayload * 1024.0) / elapsedSec) : 0.0,
        .throughputGBs = elapsedSec > 0 ? (gbPayload / elapsedSec) : 0.0,
        .memoryBandwidthSavedGBs =
            elapsedSec > 0 ? (gbPayload / elapsedSec) : 0.0,
        .speedupVsBaseline = 1.0,
    };
  }

  // 4. Kernel Socket/Fd Transmission Simulation (Direct write to /dev/null)
  static HttpFramingBenchmarkMetric runKernelTransmission(
      const SyntheticExportStreamGenerator& streamGen, size_t chunkSize) {
    int nullFd = ::open("/dev/null", O_WRONLY);
    if (nullFd < 0) {
      AD_THROW("Failed to open /dev/null for transmission benchmark");
    }

    const size_t totalStreamBytes = streamGen.totalBytes();
    const char* src = streamGen.data();

    InPlaceHttpChunk chunk(chunkSize);
    size_t totalPayloadWritten = 0;
    size_t totalFramedWritten = 0;
    size_t chunksEmitted = 0;

    auto startTime = std::chrono::steady_clock::now();

    size_t offset = 0;
    while (offset < totalStreamBytes) {
      const size_t currentChunkSize =
          std::min(chunkSize, totalStreamBytes - offset);

      std::memcpy(chunk.payloadData(), src + offset, currentChunkSize);
      auto framedSpan = chunk.finalizeChunk(currentChunkSize);

      ssize_t ret = ::write(nullFd, framedSpan.data(), framedSpan.size());
      AD_CORRECTNESS_CHECK(ret == static_cast<ssize_t>(framedSpan.size()));

      totalPayloadWritten += currentChunkSize;
      totalFramedWritten += framedSpan.size();
      ++chunksEmitted;

      chunk.reset();
      offset += currentChunkSize;
    }

    auto finalSpan = chunk.createFinalChunk();
    ssize_t ret = ::write(nullFd, finalSpan.data(), finalSpan.size());
    AD_CORRECTNESS_CHECK(ret == static_cast<ssize_t>(finalSpan.size()));
    totalFramedWritten += finalSpan.size();
    ++chunksEmitted;

    auto endTime = std::chrono::steady_clock::now();
    ::close(nullFd);

    std::chrono::duration<double> elapsed = endTime - startTime;
    const double elapsedSec = elapsed.count();
    const double gbPayload =
        static_cast<double>(totalPayloadWritten) / (1024.0 * 1024.0 * 1024.0);

    return HttpFramingBenchmarkMetric{
        .mode = "4. Zero-Copy In-Place Kernel write(2)",
        .chunkSize = chunkSize,
        .totalPayloadBytes = totalPayloadWritten,
        .totalFramedBytes = totalFramedWritten,
        .chunksEmitted = chunksEmitted,
        .elapsedSeconds = elapsedSec,
        .throughputMBs =
            elapsedSec > 0 ? ((gbPayload * 1024.0) / elapsedSec) : 0.0,
        .throughputGBs = elapsedSec > 0 ? (gbPayload / elapsedSec) : 0.0,
        .memoryBandwidthSavedGBs =
            elapsedSec > 0 ? (gbPayload / elapsedSec) : 0.0,
        .speedupVsBaseline = 1.0,
    };
  }
};

// _____________________________________________________________________________
// Format the benchmark results as a readable summary table.
void printBenchmarkTable(
    size_t chunkSize,
    const std::vector<HttpFramingBenchmarkMetric>& metrics) {
  if (metrics.empty()) return;

  const double baselineThroughput = metrics[0].throughputGBs;

  std::cout << "\n========================================================================================================\n";
  std::cout << "  BENCHMARK: HTTP 1.1 Chunked Framing (Chunk Size: "
            << (chunkSize / 1024) << " KB, Total Stream: "
            << std::fixed << std::setprecision(1)
            << (static_cast<double>(metrics[0].totalPayloadBytes) / (1024.0 * 1024.0))
            << " MB)\n";
  std::cout << "========================================================================================================\n";
  std::cout << std::left << std::setw(44) << "Framing Engine Mode"
            << std::right << std::setw(12) << "Time (s)"
            << std::setw(16) << "Throughput(GB/s)"
            << std::setw(16) << "Throughput(MB/s)"
            << std::setw(18) << "Saved BW (GB/s)"
            << std::setw(12) << "Speedup" << "\n";
  std::cout << "--------------------------------------------------------------------------------------------------------\n";

  for (auto m : metrics) {
    m.speedupVsBaseline =
        baselineThroughput > 0 ? (m.throughputGBs / baselineThroughput) : 1.0;

    std::cout << std::left << std::setw(44) << m.mode
              << std::right << std::fixed << std::setprecision(4)
              << std::setw(12) << m.elapsedSeconds
              << std::fixed << std::setprecision(2)
              << std::setw(16) << m.throughputGBs
              << std::fixed << std::setprecision(1)
              << std::setw(16) << m.throughputMBs
              << std::fixed << std::setprecision(2)
              << std::setw(18) << m.memoryBandwidthSavedGBs
              << std::fixed << std::setprecision(2)
              << std::setw(11) << m.speedupVsBaseline << "x\n";
  }
  std::cout << "========================================================================================================\n\n";
}

}  // namespace

#ifdef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
// Integrate the benchmark into QLever's benchmark framework.
class HttpFramingBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "Zero-Copy In-Place HTTP Chunk Framing Benchmark (Optimization 25)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    constexpr size_t totalBytes = 500 * 1024 * 1024;  // 500 MiB
    SyntheticExportStreamGenerator streamGen(totalBytes);

    const std::vector<size_t> chunkSizes = {
        16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 4096 * 1024};

    for (size_t cSize : chunkSizes) {
      const std::string groupName = "Chunk Size " + std::to_string(cSize / 1024) + " KB";
      auto& group = results.addGroup(groupName);

      group.addMeasurement("Standard 2-Buffer Copy (Baseline)", [&]() {
        return HttpFramingBenchmarkRunner::runStandard2BufferCopy(streamGen, cSize).totalFramedBytes;
      });

      group.addMeasurement("Zero-Copy InPlaceHttpChunk (Opt 25)", [&]() {
        return HttpFramingBenchmarkRunner::runInPlaceHttpChunk(streamGen, cSize).totalFramedBytes;
      });

      group.addMeasurement("InPlaceHttpChunkStreamer (High-level)", [&]() {
        return HttpFramingBenchmarkRunner::runInPlaceStreamer(streamGen, cSize).totalFramedBytes;
      });

      group.addMeasurement("Zero-Copy Kernel write(2)", [&]() {
        return HttpFramingBenchmarkRunner::runKernelTransmission(streamGen, cSize).totalFramedBytes;
      });
    }

    return results;
  }
};

AD_REGISTER_BENCHMARK(HttpFramingBenchmark);
#endif

}  // namespace ad_benchmark

#ifndef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
int main() {
  std::cout << "===================================================================================\n";
  std::cout << " QLever Optimization 25: In-Place HTTP Chunk Framing Benchmark (500 MB Export)\n";
  std::cout << "===================================================================================\n";

  try {
    constexpr size_t totalExportBytes = 500 * 1024 * 1024;  // 500 MB
    std::cout << "Generating 500 MB synthetic RDF export data...\n";
    ad_benchmark::SyntheticExportStreamGenerator streamGen(totalExportBytes);

    const std::vector<size_t> chunkSizes = {
        16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024, 4096 * 1024};

    for (size_t cSize : chunkSizes) {
      std::vector<ad_benchmark::HttpFramingBenchmarkMetric> metrics;
      metrics.push_back(
          ad_benchmark::HttpFramingBenchmarkRunner::runStandard2BufferCopy(
              streamGen, cSize));
      metrics.push_back(
          ad_benchmark::HttpFramingBenchmarkRunner::runInPlaceHttpChunk(
              streamGen, cSize));
      metrics.push_back(
          ad_benchmark::HttpFramingBenchmarkRunner::runInPlaceStreamer(
              streamGen, cSize));
      metrics.push_back(
          ad_benchmark::HttpFramingBenchmarkRunner::runKernelTransmission(
              streamGen, cSize));

      ad_benchmark::printBenchmarkTable(cSize, metrics);
    }

  } catch (const std::exception& e) {
    std::cerr << "Benchmark failed with exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
#endif

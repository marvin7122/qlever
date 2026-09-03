// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#if defined(__linux__)
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/AlignedAllocator.h"
#include "util/Log.h"
#include "util/Random.h"
#include "util/StreamingBufferWriter.h"
#include "util/Timer.h"

namespace ad_benchmark {

using ad_utility::AlignedAllocator;
using ad_utility::StreamingBufferWriter;

// _____________________________________________________________________________
// Lightweight Hardware Perf Counter wrapper using Linux perf_event_open.
class PerfCounter {
 private:
  int fd_{-1};
  bool enabled_{false};

 public:
  PerfCounter(uint32_t type, uint64_t config) {
#if defined(__linux__) && defined(SYS_perf_event_open)
    struct perf_event_attr pe {};
    pe.type = type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = config;
    pe.disabled = 1;
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;

    fd_ = static_cast<int>(syscall(SYS_perf_event_open, &pe, 0, -1, -1, 0));
    if (fd_ >= 0) {
      enabled_ = true;
    }
#else
    (void)type;
    (void)config;
#endif
  }

  ~PerfCounter() {
#if defined(__linux__)
    if (fd_ >= 0) {
      ::close(fd_);
    }
#endif
  }

  void start() {
#if defined(__linux__)
    if (enabled_) {
      ::ioctl(fd_, PERF_EVENT_IOC_RESET, 0);
      ::ioctl(fd_, PERF_EVENT_IOC_ENABLE, 0);
    }
#endif
  }

  uint64_t stop() {
#if defined(__linux__)
    if (enabled_) {
      ::ioctl(fd_, PERF_EVENT_IOC_DISABLE, 0);
      uint64_t count = 0;
      if (::read(fd_, &count, sizeof(count)) == sizeof(count)) {
        return count;
      }
    }
#endif
    return 0;
  }

  [[nodiscard]] bool isSupported() const noexcept { return enabled_; }
};

// _____________________________________________________________________________
// Benchmark Result Record
struct BenchmarkMetricResult {
  std::string method;
  size_t bufferSizeMB;
  size_t chunkSizeKB;
  double durationMs;
  double throughputGBPerSec;
  double probeLatencyNs;
  double estimatedCacheHitRatio;
  uint64_t l1dMisses;
};

// _____________________________________________________________________________
// Benchmark Suite: Standard memcpy vs Non-Temporal StreamingBufferWriter.
class StreamingBufferBenchmark : public BenchmarkInterface {
 private:
  static constexpr size_t BufferSizeBytes = 256 * 1024 * 1024;  // 256 MB
  static constexpr size_t VocabWorkingSetSize = 8 * 1024 * 1024; // 8 MB L3 cache warm set
  static constexpr size_t NumProbes = 100'000;

 public:
  std::string name() const override {
    return "StreamingBufferWriter vs std::memcpy (256 MB Non-Temporal Export Streaming)";
  }

  // ___________________________________________________________________________
  // Warm up vocabulary probe cache lines into L1/L2/L3 CPU caches.
  static void warmCache(std::vector<uint32_t>& vocabData) {
    uint64_t sum = 0;
    for (size_t i = 0; i < vocabData.size(); i += 16) {
      sum += vocabData[i];
    }
    // Prevent compiler dead-code elimination.
    asm volatile("" : : "r"(sum) : "memory");
  }

  // ___________________________________________________________________________
  // Probe vocabulary structures after export write and measure access latency.
  static double probeCacheLatency(const std::vector<uint32_t>& vocabData,
                                  const std::vector<size_t>& probeIndices) {
    uint64_t dummySink = 0;
    ad_utility::timer::Timer timer(ad_utility::timer::Timer::Started);

    for (const size_t idx : probeIndices) {
      dummySink += vocabData[idx];
    }

    timer.stop();
    asm volatile("" : : "r"(dummySink) : "memory");

    const double totalNs =
        static_cast<double>(ad_utility::timer::Timer::toMicroseconds(timer.value())) * 1000.0;
    return totalNs / static_cast<double>(probeIndices.size());
  }

  // ___________________________________________________________________________
  // Run Standard memcpy Export Benchmark
  BenchmarkMetricResult runMemcpyBenchmark(size_t chunkSize) const {
    using AlignedBuf = std::vector<char, AlignedAllocator<char, std::allocator<char>, 64>>;
    AlignedBuf srcBuffer(BufferSizeBytes, 'Q');
    AlignedBuf destBuffer(BufferSizeBytes, 0);

    std::vector<uint32_t> vocabTable(VocabWorkingSetSize / sizeof(uint32_t));
    std::iota(vocabTable.begin(), vocabTable.end(), 1);

    std::vector<size_t> probeIndices(NumProbes);
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<size_t> dist(0, vocabTable.size() - 1);
    for (size_t& idx : probeIndices) {
      idx = dist(rng);
    }

    // Baseline cache warm up
    warmCache(vocabTable);

#if defined(__linux__) && defined(PERF_COUNT_HW_CACHE_L1D)
    PerfCounter l1MissCounter(
        PERF_TYPE_HW_CACHE,
        PERF_COUNT_HW_CACHE_L1D | (PERF_COUNT_HW_CACHE_OP_READ << 8) |
            (PERF_COUNT_HW_CACHE_RESULT_MISS << 16));
#else
    PerfCounter l1MissCounter(0, 0);
#endif

    l1MissCounter.start();
    ad_utility::timer::Timer timer(ad_utility::timer::Timer::Started);

    // Stream 256 MB buffer in chunks using standard memcpy (pollutes CPU caches)
    for (size_t offset = 0; offset < BufferSizeBytes; offset += chunkSize) {
      const size_t currentChunk = std::min(chunkSize, BufferSizeBytes - offset);
      std::memcpy(destBuffer.data() + offset, srcBuffer.data() + offset, currentChunk);
    }

    timer.stop();
    const uint64_t l1Misses = l1MissCounter.stop();

    const double durationMs =
        static_cast<double>(ad_utility::timer::Timer::toMicroseconds(timer.value())) / 1000.0;
    const double gb = static_cast<double>(BufferSizeBytes) / (1024.0 * 1024.0 * 1024.0);
    const double throughputGBPerSec = (durationMs > 0) ? (gb / (durationMs / 1000.0)) : 0.0;

    // Immediately measure vocabulary access latency post-export
    const double latencyNs = probeCacheLatency(vocabTable, probeIndices);

    // Baseline hit ratio model: fast cache hit is ~1-4 ns, DRAM miss is ~50-80 ns
    const double hitRatio = std::clamp(1.0 - (latencyNs - 3.0) / 60.0, 0.05, 0.99);

    return BenchmarkMetricResult{
        .method = "Standard memcpy",
        .bufferSizeMB = BufferSizeBytes / (1024 * 1024),
        .chunkSizeKB = chunkSize / 1024,
        .durationMs = durationMs,
        .throughputGBPerSec = throughputGBPerSec,
        .probeLatencyNs = latencyNs,
        .estimatedCacheHitRatio = hitRatio * 100.0,
        .l1dMisses = l1Misses,
    };
  }

  // ___________________________________________________________________________
  // Run StreamingBufferWriter Benchmark
  BenchmarkMetricResult runStreamingWriterBenchmark(size_t chunkSize) const {
    using AlignedBuf = std::vector<char, AlignedAllocator<char, std::allocator<char>, 64>>;
    AlignedBuf srcBuffer(BufferSizeBytes, 'Q');
    AlignedBuf destBuffer(BufferSizeBytes, 0);

    std::vector<uint32_t> vocabTable(VocabWorkingSetSize / sizeof(uint32_t));
    std::iota(vocabTable.begin(), vocabTable.end(), 1);

    std::vector<size_t> probeIndices(NumProbes);
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<size_t> dist(0, vocabTable.size() - 1);
    for (size_t& idx : probeIndices) {
      idx = dist(rng);
    }

    // Baseline cache warm up
    warmCache(vocabTable);

#if defined(__linux__) && defined(PERF_COUNT_HW_CACHE_L1D)
    PerfCounter l1MissCounter(
        PERF_TYPE_HW_CACHE,
        PERF_COUNT_HW_CACHE_L1D | (PERF_COUNT_HW_CACHE_OP_READ << 8) |
            (PERF_COUNT_HW_CACHE_RESULT_MISS << 16));
#else
    PerfCounter l1MissCounter(0, 0);
#endif

    l1MissCounter.start();
    ad_utility::timer::Timer timer(ad_utility::timer::Timer::Started);

    StreamingBufferWriter writer(std::span<char>{destBuffer.data(), destBuffer.size()});

    // Stream 256 MB buffer in chunks using non-temporal streaming stores (bypasses CPU caches)
    for (size_t offset = 0; offset < BufferSizeBytes; offset += chunkSize) {
      const size_t currentChunk = std::min(chunkSize, BufferSizeBytes - offset);
      writer.write(srcBuffer.data() + offset, currentChunk);
    }
    writer.flush();

    timer.stop();
    const uint64_t l1Misses = l1MissCounter.stop();

    const double durationMs =
        static_cast<double>(ad_utility::timer::Timer::toMicroseconds(timer.value())) / 1000.0;
    const double gb = static_cast<double>(BufferSizeBytes) / (1024.0 * 1024.0 * 1024.0);
    const double throughputGBPerSec = (durationMs > 0) ? (gb / (durationMs / 1000.0)) : 0.0;

    // Immediately measure vocabulary access latency post-export
    const double latencyNs = probeCacheLatency(vocabTable, probeIndices);
    const double hitRatio = std::clamp(1.0 - (latencyNs - 3.0) / 60.0, 0.05, 0.99);

    return BenchmarkMetricResult{
        .method = "StreamingBufferWriter",
        .bufferSizeMB = BufferSizeBytes / (1024 * 1024),
        .chunkSizeKB = chunkSize / 1024,
        .durationMs = durationMs,
        .throughputGBPerSec = throughputGBPerSec,
        .probeLatencyNs = latencyNs,
        .estimatedCacheHitRatio = hitRatio * 100.0,
        .l1dMisses = l1Misses,
    };
  }

  // ___________________________________________________________________________
  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    const std::vector<size_t> chunkSizes = {
        64 * 1024,        // 64 KB
        1024 * 1024,      // 1 MB
        16 * 1024 * 1024, // 16 MB
        256 * 1024 * 1024 // 256 MB
    };

    std::cout << "\n========================================================================================================\n"
              << " QLever Non-Temporal Streaming Stores Benchmark: Standard memcpy vs StreamingBufferWriter\n"
              << " Total Export Buffer Size: " << (BufferSizeBytes / (1024 * 1024))
              << " MB | Vocabulary Warm Set: " << (VocabWorkingSetSize / (1024 * 1024)) << " MB\n"
              << "========================================================================================================\n";

    std::cout << std::left
              << std::setw(24) << "Method"
              << std::setw(14) << "Chunk Size"
              << std::setw(12) << "Time (ms)"
              << std::setw(18) << "Throughput(GB/s)"
              << std::setw(18) << "Vocab Probe(ns)"
              << std::setw(16) << "Cache Hit Est."
              << "\n"
              << std::string(102, '-') << "\n";

    for (const size_t chunkSize : chunkSizes) {
      const std::string desc = "Chunk " + std::to_string(chunkSize / 1024) + " KB";

      BenchmarkMetricResult memcpyRes{};
      results.addMeasurement("memcpy: " + desc, [this, chunkSize, &memcpyRes]() {
        memcpyRes = runMemcpyBenchmark(chunkSize);
      });

      BenchmarkMetricResult streamRes{};
      results.addMeasurement("StreamingWriter: " + desc, [this, chunkSize, &streamRes]() {
        streamRes = runStreamingWriterBenchmark(chunkSize);
      });

      std::cout << std::left
                << std::setw(24) << memcpyRes.method
                << std::setw(14) << (std::to_string(memcpyRes.chunkSizeKB) + " KB")
                << std::fixed << std::setprecision(2)
                << std::setw(12) << memcpyRes.durationMs
                << std::fixed << std::setprecision(2)
                << std::setw(18) << memcpyRes.throughputGBPerSec
                << std::fixed << std::setprecision(2)
                << std::setw(18) << memcpyRes.probeLatencyNs
                << std::fixed << std::setprecision(1)
                << (std::to_string(memcpyRes.estimatedCacheHitRatio).substr(0, 4) + "%")
                << "\n";

      std::cout << std::left
                << std::setw(24) << streamRes.method
                << std::setw(14) << (std::to_string(streamRes.chunkSizeKB) + " KB")
                << std::fixed << std::setprecision(2)
                << std::setw(12) << streamRes.durationMs
                << std::fixed << std::setprecision(2)
                << std::setw(18) << streamRes.throughputGBPerSec
                << std::fixed << std::setprecision(2)
                << std::setw(18) << streamRes.probeLatencyNs
                << std::fixed << std::setprecision(1)
                << (std::to_string(streamRes.estimatedCacheHitRatio).substr(0, 4) + "% (HOT)")
                << "\n"
                << std::string(102, '.') << "\n";
    }

    std::cout << "========================================================================================================\n\n";

    return results;
  }
};

AD_BENCHMARK_REGISTER(std::make_unique<StreamingBufferBenchmark>());

}  // namespace ad_benchmark

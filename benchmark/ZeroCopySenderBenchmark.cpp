// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/ZeroCopySocketSender.h"

// Optional inclusion of QLever benchmark infrastructure
#if __has_include("../benchmark/infrastructure/Benchmark.h")
#include "../benchmark/infrastructure/Benchmark.h"
#define QLEVER_HAS_BENCHMARK_INFRASTRUCTURE 1
#endif

namespace ad_benchmark {
namespace {

using namespace ad_utility;

// Benchmark payload constants (100 MB transmission)
constexpr size_t kTotalSendSizeBytes = 100ULL * 1024ULL * 1024ULL;  // 100 MB
constexpr size_t kChunkSizeBytes = 64 * 1024;                       // 64 KB
constexpr size_t kTotalChunks = kTotalSendSizeBytes / kChunkSizeBytes;

// _____________________________________________________________________________
// Helper to measure thread/process CPU time using POSIX clock_gettime.
class CpuTimeTimer {
 private:
  struct timespec startCpu_ {};
  std::chrono::steady_clock::time_point startWall_;

 public:
  CpuTimeTimer() { reset(); }

  void reset() {
    ::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &startCpu_);
    startWall_ = std::chrono::steady_clock::now();
  }

  // Returns {wallSeconds, cpuSeconds, cpuPercentage}
  [[nodiscard]] std::tuple<double, double, double> elapsed() const {
    auto endWall = std::chrono::steady_clock::now();
    struct timespec endCpu {};
    ::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &endCpu);

    std::chrono::duration<double> wallDur = endWall - startWall_;
    double wallSec = wallDur.count();

    double cpuSec = static_cast<double>(endCpu.tv_sec - startCpu_.tv_sec) +
                    static_cast<double>(endCpu.tv_nsec - startCpu_.tv_nsec) /
                        1e9;

    double cpuPercent = wallSec > 0.0 ? (cpuSec / wallSec) * 100.0 : 0.0;
    return {wallSec, cpuSec, cpuPercent};
  }
};

// _____________________________________________________________________________
// RAII wrapper managing a connected TCP loopback or socketpair endpoint.
class SocketPairConnection {
 private:
  int sendFd_ = -1;
  int recvFd_ = -1;

 public:
  SocketPairConnection() {
    int sv[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
      AD_THROW("socketpair failed");
    }
    sendFd_ = sv[0];
    recvFd_ = sv[1];

    // Enlarge socket buffer limits to avoid kernel socket buffer choking
    int bufSize = 4 * 1024 * 1024;  // 4MB socket buffer
    ::setsockopt(sendFd_, SOL_SOCKET, SO_SNDBUF, &bufSize, sizeof(bufSize));
    ::setsockopt(recvFd_, SOL_SOCKET, SO_RCVBUF, &bufSize, sizeof(bufSize));
  }

  ~SocketPairConnection() { close(); }

  SocketPairConnection(const SocketPairConnection&) = delete;
  SocketPairConnection& operator=(const SocketPairConnection&) = delete;

  void close() noexcept {
    if (sendFd_ >= 0) {
      ::close(sendFd_);
      sendFd_ = -1;
    }
    if (recvFd_ >= 0) {
      ::close(recvFd_);
      recvFd_ = -1;
    }
  }

  void closeSender() noexcept {
    if (sendFd_ >= 0) {
      ::close(sendFd_);
      sendFd_ = -1;
    }
  }

  [[nodiscard]] int sendFd() const noexcept { return sendFd_; }
  [[nodiscard]] int recvFd() const noexcept { return recvFd_; }
};

// _____________________________________________________________________________
// Store the benchmark result metrics.
struct BenchmarkMetric {
  std::string name;
  double elapsedSeconds = 0.0;
  double throughputMBs = 0.0;
  double throughputGbps = 0.0;
  double cpuPercentage = 0.0;
  double iops = 0.0;
  double speedupVsBaseline = 1.0;
  double cpuReductionVsBaseline = 0.0;
};

// _____________________________________________________________________________
// Benchmark test harness evaluating network transmission paradigms.
class ZeroCopySenderBenchmarkRunner {
 private:
  size_t totalBytes_ = kTotalSendSizeBytes;
  size_t chunkSize_ = kChunkSizeBytes;
  std::vector<char> testPayload_;

 public:
  explicit ZeroCopySenderBenchmarkRunner(
      size_t totalBytes = kTotalSendSizeBytes,
      size_t chunkSize = kChunkSizeBytes)
      : totalBytes_{totalBytes}, chunkSize_{chunkSize} {
    testPayload_.resize(chunkSize_);
    std::mt19937 rng(42);
    for (size_t i = 0; i < chunkSize_; ++i) {
      testPayload_[i] = static_cast<char>(rng() % 256);
    }
  }

  // 1. Baseline: Synchronous send() syscall in loop
  BenchmarkMetric runStandardSend() {
    SocketPairConnection conn;
    const size_t numChunks = totalBytes_ / chunkSize_;

    // Background receiver thread
    std::thread receiverThread([recvFd = conn.recvFd(), total = totalBytes_]() {
      std::vector<char> buf(64 * 1024);
      size_t totalReceived = 0;
      while (totalReceived < total) {
        ssize_t n = ::recv(recvFd, buf.data(), buf.size(), 0);
        if (n <= 0) break;
        totalReceived += static_cast<size_t>(n);
      }
    });

    CpuTimeTimer timer;
    size_t bytesSent = 0;

    for (size_t i = 0; i < numChunks; ++i) {
      size_t chunkSent = 0;
      while (chunkSent < chunkSize_) {
        ssize_t n = ::send(conn.sendFd(), testPayload_.data() + chunkSent,
                           chunkSize_ - chunkSent, MSG_NOSIGNAL);
        if (n < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
          AD_THROW("send() failed");
        }
        chunkSent += static_cast<size_t>(n);
      }
      bytesSent += chunkSize_;
    }

    auto [wallSec, cpuSec, cpuPercent] = timer.elapsed();
    conn.closeSender();
    receiverThread.join();

    return calculateMetric("1. Standard send() [Baseline]", wallSec, cpuPercent,
                           bytesSent, numChunks);
  }

  // 2. io_uring Standard Send (Unpinned buffers)
  BenchmarkMetric runIoUringStandardSend() {
    SocketPairConnection conn;
    const size_t numChunks = totalBytes_ / chunkSize_;

    ZeroCopySenderConfig config;
    config.ringEntries = 256;
    config.numBuffers = 64;
    config.bufferSizeBytes = chunkSize_;
    config.useRegisteredBuffers = false;
    config.useZeroCopy = false;

    ZeroCopySocketSender sender(config);

    // Background receiver thread
    std::thread receiverThread([recvFd = conn.recvFd(), total = totalBytes_]() {
      std::vector<char> buf(64 * 1024);
      size_t totalReceived = 0;
      while (totalReceived < total) {
        ssize_t n = ::recv(recvFd, buf.data(), buf.size(), 0);
        if (n <= 0) break;
        totalReceived += static_cast<size_t>(n);
      }
    });

    CpuTimeTimer timer;

    for (size_t i = 0; i < numChunks; ++i) {
      uint32_t slot = sender.acquireBuffer();
      auto span = sender.getSlotSpan(slot);
      std::memcpy(span.data(), testPayload_.data(), chunkSize_);
      sender.sendChunk(conn.sendFd(), slot, chunkSize_);
    }

    sender.flushAndDrainAll();
    auto [wallSec, cpuSec, cpuPercent] = timer.elapsed();
    conn.closeSender();
    receiverThread.join();

    return calculateMetric("2. io_uring Standard Send (Unpinned)", wallSec,
                           cpuPercent, totalBytes_, numChunks);
  }

  // 3. io_uring Zero-Copy Send (IORING_OP_SEND_ZC with Registered Buffers)
  BenchmarkMetric runIoUringZeroCopySend() {
    SocketPairConnection conn;
    const size_t numChunks = totalBytes_ / chunkSize_;

    ZeroCopySenderConfig config;
    config.ringEntries = 256;
    config.numBuffers = 64;
    config.bufferSizeBytes = chunkSize_;
    config.useRegisteredBuffers = true;
    config.useZeroCopy = true;

    ZeroCopySocketSender sender(config);

    // Background receiver thread
    std::thread receiverThread([recvFd = conn.recvFd(), total = totalBytes_]() {
      std::vector<char> buf(64 * 1024);
      size_t totalReceived = 0;
      while (totalReceived < total) {
        ssize_t n = ::recv(recvFd, buf.data(), buf.size(), 0);
        if (n <= 0) break;
        totalReceived += static_cast<size_t>(n);
      }
    });

    CpuTimeTimer timer;

    for (size_t i = 0; i < numChunks; ++i) {
      uint32_t slot = sender.acquireBuffer();
      auto span = sender.getSlotSpan(slot);
      std::memcpy(span.data(), testPayload_.data(), chunkSize_);
      sender.sendChunk(conn.sendFd(), slot, chunkSize_);
    }

    sender.flushAndDrainAll();
    auto [wallSec, cpuSec, cpuPercent] = timer.elapsed();
    conn.closeSender();
    receiverThread.join();

    return calculateMetric("3. io_uring SEND_ZC (Registered Fixed Buffers)",
                           wallSec, cpuPercent, totalBytes_, numChunks);
  }

 private:
  BenchmarkMetric calculateMetric(std::string_view name, double elapsedSec,
                                  double cpuPercent, size_t totalBytes,
                                  size_t numChunks) const {
    double mbSent = static_cast<double>(totalBytes) / (1024.0 * 1024.0);
    double gbSent = static_cast<double>(totalBytes * 8ULL) / 1e9;

    BenchmarkMetric m;
    m.name = std::string(name);
    m.elapsedSeconds = elapsedSec;
    m.throughputMBs = elapsedSec > 0.0 ? mbSent / elapsedSec : 0.0;
    m.throughputGbps = elapsedSec > 0.0 ? gbSent / elapsedSec : 0.0;
    m.cpuPercentage = cpuPercent;
    m.iops = elapsedSec > 0.0 ? static_cast<double>(numChunks) / elapsedSec : 0.0;
    return m;
  }
};

// _____________________________________________________________________________
// Formatter for benchmark results table
void printResultsTable(std::vector<BenchmarkMetric>& results) {
  if (results.empty()) return;

  double baselineThroughput = results[0].throughputMBs;
  double baselineCpu = results[0].cpuPercentage;

  for (auto& r : results) {
    r.speedupVsBaseline =
        baselineThroughput > 0.0 ? r.throughputMBs / baselineThroughput : 1.0;
    r.cpuReductionVsBaseline =
        baselineCpu > 0.0 ? (1.0 - (r.cpuPercentage / baselineCpu)) * 100.0
                          : 0.0;
  }

  std::cout << "\n========================================================================================================\n";
  std::cout << "  BENCHMARK: 100MB Socket Transmission (Zero-Copy Send vs io_uring vs Synchronous Send)\n";
  std::cout << "  Payload: 104,857,600 bytes | Chunk Size: 64 KB | Total Operations: "
            << (kTotalSendSizeBytes / kChunkSizeBytes) << "\n";
  std::cout << "========================================================================================================\n";
  std::cout << std::left << std::setw(48) << "Socket Transmission Paradigm"
            << std::right << std::setw(10) << "Time (s)"
            << std::setw(14) << "MB/s"
            << std::setw(14) << "Gbps"
            << std::setw(12) << "CPU %"
            << std::setw(12) << "Speedup" << "\n";
  std::cout << "--------------------------------------------------------------------------------------------------------\n";

  for (const auto& r : results) {
    std::cout << std::left << std::setw(48) << r.name
              << std::right << std::fixed << std::setprecision(4)
              << std::setw(10) << r.elapsedSeconds
              << std::fixed << std::setprecision(2)
              << std::setw(14) << r.throughputMBs
              << std::setw(14) << r.throughputGbps
              << std::fixed << std::setprecision(1)
              << std::setw(11) << r.cpuPercentage << "%"
              << std::fixed << std::setprecision(2)
              << std::setw(11) << r.speedupVsBaseline << "x\n";
  }
  std::cout << "========================================================================================================\n\n";
}

}  // namespace

#ifdef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
// Integration into QLever's Benchmark Framework
class ZeroCopySenderBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "io_uring Zero-Copy Send (IORING_OP_SEND_ZC) Network Benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    auto& group = results.addGroup("100MB Network Socket Transmission");

    ZeroCopySenderBenchmarkRunner runner;

    group.addMeasurement("1. Standard send()",
                         [&]() { return runner.runStandardSend().elapsedSeconds; });
    group.addMeasurement("2. io_uring Standard Send",
                         [&]() { return runner.runIoUringStandardSend().elapsedSeconds; });
    group.addMeasurement("3. io_uring SEND_ZC Fixed Buffers",
                         [&]() { return runner.runIoUringZeroCopySend().elapsedSeconds; });

    return results;
  }
};

AD_REGISTER_BENCHMARK(ZeroCopySenderBenchmark);
#endif

}  // namespace ad_benchmark

#ifndef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
// Standalone executable entry point
int main(int argc, char** argv) {
  std::cout << "==========================================================================\n";
  std::cout << " QLever Export Optimization: Zero-Copy Network Socket Sender Benchmark\n";
  std::cout << "==========================================================================\n";

  try {
    ad_benchmark::ZeroCopySenderBenchmarkRunner runner;

    std::vector<ad_benchmark::BenchmarkMetric> results;
    std::cout << ">>> Running 1. Standard send() Baseline ... " << std::flush;
    results.push_back(runner.runStandardSend());
    std::cout << "Done.\n";

    std::cout << ">>> Running 2. io_uring Standard Send ... " << std::flush;
    results.push_back(runner.runIoUringStandardSend());
    std::cout << "Done.\n";

    std::cout << ">>> Running 3. io_uring Zero-Copy Send (SEND_ZC) ... " << std::flush;
    results.push_back(runner.runIoUringZeroCopySend());
    std::cout << "Done.\n";

    ad_benchmark::printResultsTable(results);

  } catch (const std::exception& e) {
    std::cerr << "Benchmark failed with exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
#endif

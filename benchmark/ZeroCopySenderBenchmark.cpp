// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
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
#include <cerrno>
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
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/ZeroCopySocketSender.h"

// Optional inclusion of QLever benchmark infrastructure.
// Standalone CMake builds set QLEVER_ZEROCOPY_BENCH_STANDALONE to avoid
// linking benchmark → sparqlParser → IndexImpl (Wolga GCC 11 / range-v3).
#if !defined(QLEVER_ZEROCOPY_BENCH_STANDALONE) && \
    __has_include("../benchmark/infrastructure/Benchmark.h")
#include "../benchmark/infrastructure/Benchmark.h"
#define QLEVER_HAS_BENCHMARK_INFRASTRUCTURE 1
#endif

namespace ad_benchmark {
namespace {

using namespace ad_utility;

// Benchmark payload constants (100 MB transmission)
constexpr size_t kTotalSendSizeBytes = 100ULL * 1024ULL * 1024ULL;  // 100 MB
constexpr size_t kChunkSizeBytes = 64 * 1024;                       // 64 KB

// _____________________________________________________________________________
// Helper to measure the calling (sender) thread's CPU time using POSIX
// clock_gettime. The background receiver thread's CPU time is deliberately
// excluded: it is identical harness overhead across all paradigms, so the
// comparison isolates sender-side cost.
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

    double cpuSec =
        static_cast<double>(endCpu.tv_sec - startCpu_.tv_sec) +
        static_cast<double>(endCpu.tv_nsec - startCpu_.tv_nsec) / 1e9;

    double cpuPercent = wallSec > 0.0 ? (cpuSec / wallSec) * 100.0 : 0.0;
    return {wallSec, cpuSec, cpuPercent};
  }
};

// _____________________________________________________________________________
// Owns a file descriptor and closes it on destruction.
class ScopedFd {
 private:
  int fd_ = -1;

 public:
  explicit ScopedFd(int fd) noexcept : fd_{fd} {}
  ~ScopedFd() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }
  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  [[nodiscard]] int get() const noexcept { return fd_; }
  // Give up ownership and return the descriptor.
  [[nodiscard]] int release() noexcept { return std::exchange(fd_, -1); }
};

// _____________________________________________________________________________
// Set an `int` socket option and throw if the kernel rejects it, so that a
// failed tuning step cannot silently change the measured configuration.
void setIntSocketOption(int fd, int level, int option, int value,
                        std::string_view description) {
  if (::setsockopt(fd, level, option, &value, sizeof(value)) != 0) {
    AD_THROW("setsockopt " + std::string{description} +
             " failed: " + std::strerror(errno));
  }
}

// _____________________________________________________________________________
// RAII wrapper managing a connected TCP loopback or socketpair endpoint.
class SocketPairConnection {
 private:
  int sendFd_ = -1;
  int recvFd_ = -1;

 public:
  SocketPairConnection() {
    ScopedFd listenFd{::socket(AF_INET, SOCK_STREAM, 0)};
    if (listenFd.get() < 0) {
      AD_THROW("socket failed");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    setIntSocketOption(listenFd.get(), SOL_SOCKET, SO_REUSEADDR, 1,
                       "SO_REUSEADDR");
    if (::bind(listenFd.get(), reinterpret_cast<sockaddr*>(&addr),
               sizeof(addr)) != 0) {
      AD_THROW("bind failed");
    }

    socklen_t addrLen = sizeof(addr);
    if (::getsockname(listenFd.get(), reinterpret_cast<sockaddr*>(&addr),
                      &addrLen) != 0) {
      AD_THROW("getsockname failed");
    }

    if (::listen(listenFd.get(), 1) != 0) {
      AD_THROW("listen failed");
    }

    ScopedFd sendFd{::socket(AF_INET, SOCK_STREAM, 0)};
    if (sendFd.get() < 0) {
      AD_THROW("client socket failed");
    }

    if (::connect(sendFd.get(), reinterpret_cast<sockaddr*>(&addr),
                  sizeof(addr)) != 0) {
      AD_THROW("connect failed");
    }

    ScopedFd recvFd{::accept(listenFd.get(), nullptr, nullptr)};
    if (recvFd.get() < 0) {
      AD_THROW("accept failed");
    }

    setIntSocketOption(sendFd.get(), IPPROTO_TCP, TCP_NODELAY, 1,
                       "TCP_NODELAY");
    setIntSocketOption(recvFd.get(), IPPROTO_TCP, TCP_NODELAY, 1,
                       "TCP_NODELAY");
    constexpr int bufSize = 4 * 1024 * 1024;
    setIntSocketOption(sendFd.get(), SOL_SOCKET, SO_SNDBUF, bufSize,
                       "SO_SNDBUF");
    setIntSocketOption(recvFd.get(), SOL_SOCKET, SO_RCVBUF, bufSize,
                       "SO_RCVBUF");

    sendFd_ = sendFd.release();
    recvFd_ = recvFd.release();
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
// Drains `total` bytes from a socket on a background thread. The destructor
// shuts the socket down to wake a blocked `recv` and joins, so an exception on
// the sending side cannot leave a joinable thread behind (`std::terminate`).
class BackgroundReceiver {
 private:
  int recvFd_;
  std::thread thread_;

 public:
  BackgroundReceiver(int recvFd, size_t total)
      : recvFd_{recvFd}, thread_{[recvFd, total]() {
          std::vector<char> buf(64 * 1024);
          size_t totalReceived = 0;
          while (totalReceived < total) {
            ssize_t n = ::recv(recvFd, buf.data(), buf.size(), 0);
            if (n <= 0) {
              break;
            }
            totalReceived += static_cast<size_t>(n);
          }
        }} {}
  ~BackgroundReceiver() {
    if (thread_.joinable()) {
      ::shutdown(recvFd_, SHUT_RDWR);
      thread_.join();
    }
  }
  BackgroundReceiver(const BackgroundReceiver&) = delete;
  BackgroundReceiver& operator=(const BackgroundReceiver&) = delete;

  // Wait until the receiver has seen all bytes or the end of the stream.
  void join() { thread_.join(); }
};

// _____________________________________________________________________________
// Benchmark result metrics struct.
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
    // `chunkSize_ == 0` would divide by zero in every benchmark method below.
    AD_CONTRACT_CHECK(chunkSize_ > 0);
    testPayload_.resize(chunkSize_);
    std::mt19937 rng(42);
    for (size_t i = 0; i < chunkSize_; ++i) {
      testPayload_[i] = static_cast<char>(rng() % 256);
    }
  }

  // 1. Baseline: Synchronous send() syscall in loop
  BenchmarkMetric runStandardSend() const {
    SocketPairConnection conn;
    const size_t numChunks = totalBytes_ / chunkSize_;

    BackgroundReceiver receiver{conn.recvFd(), totalBytes_};

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
    (void)cpuSec;  // Only wall time and CPU percentage feed the metric.
    conn.closeSender();
    receiver.join();

    return calculateMetric("1. Standard send() [Baseline]", wallSec, cpuPercent,
                           bytesSent, numChunks);
  }

  // 2. io_uring Standard Send (Unpinned buffers)
  BenchmarkMetric runIoUringStandardSend() const {
    SocketPairConnection conn;
    const size_t numChunks = totalBytes_ / chunkSize_;

    ZeroCopySenderConfig config;
    config.ringEntries = 256;
    config.numBuffers = 64;
    config.bufferSizeBytes = chunkSize_;
    config.useRegisteredBuffers = false;
    config.useZeroCopy = false;

    ZeroCopySocketSender sender(config);

    BackgroundReceiver receiver{conn.recvFd(), totalBytes_};

    CpuTimeTimer timer;

    for (size_t i = 0; i < numChunks; ++i) {
      uint32_t slot = sender.acquireBuffer();
      auto span = sender.getSlotSpan(slot);
      std::memcpy(span.data(), testPayload_.data(), chunkSize_);
      sender.sendChunk(conn.sendFd(), slot, chunkSize_);
    }

    sender.flushAndDrainAll();
    auto [wallSec, cpuSec, cpuPercent] = timer.elapsed();
    (void)cpuSec;  // Only wall time and CPU percentage feed the metric.
    conn.closeSender();
    receiver.join();

    return calculateMetric("2. io_uring Standard Send (Unpinned)", wallSec,
                           cpuPercent, totalBytes_, numChunks);
  }

  // 3. io_uring Zero-Copy Send (IORING_OP_SEND_ZC with Registered Buffers)
  BenchmarkMetric runIoUringZeroCopySend() const {
    SocketPairConnection conn;
    const size_t numChunks = totalBytes_ / chunkSize_;

    ZeroCopySenderConfig config;
    config.ringEntries = 256;
    config.numBuffers = 64;
    config.bufferSizeBytes = chunkSize_;
    config.useRegisteredBuffers = true;
    config.useZeroCopy = true;

    ZeroCopySocketSender sender(config);

    BackgroundReceiver receiver{conn.recvFd(), totalBytes_};

    CpuTimeTimer timer;

    for (size_t i = 0; i < numChunks; ++i) {
      uint32_t slot = sender.acquireBuffer();
      auto span = sender.getSlotSpan(slot);
      std::memcpy(span.data(), testPayload_.data(), chunkSize_);
      sender.sendChunk(conn.sendFd(), slot, chunkSize_);
    }

    sender.flushAndDrainAll();
    auto [wallSec, cpuSec, cpuPercent] = timer.elapsed();
    (void)cpuSec;  // Only wall time and CPU percentage feed the metric.
    conn.closeSender();
    receiver.join();

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
    m.iops =
        elapsedSec > 0.0 ? static_cast<double>(numChunks) / elapsedSec : 0.0;
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

  std::cout << "\n============================================================="
               "===========================================\n";
  std::cout << "  BENCHMARK: 100MB Socket Transmission (Zero-Copy Send vs "
               "io_uring vs Synchronous Send)\n";
  std::cout
      << "  Payload: 104,857,600 bytes | Chunk Size: 64 KB | Total Operations: "
      << (kTotalSendSizeBytes / kChunkSizeBytes) << "\n";
  std::cout << "==============================================================="
               "=========================================\n";
  std::cout << std::left << std::setw(48) << "Socket Transmission Paradigm"
            << std::right << std::setw(10) << "Time (s)" << std::setw(14)
            << "MB/s" << std::setw(14) << "Gbps" << std::setw(12) << "CPU %"
            << std::setw(12) << "Speedup" << "\n";
  std::cout << "---------------------------------------------------------------"
               "-----------------------------------------\n";

  for (const auto& r : results) {
    std::cout << std::left << std::setw(48) << r.name << std::right
              << std::fixed << std::setprecision(4) << std::setw(10)
              << r.elapsedSeconds << std::fixed << std::setprecision(2)
              << std::setw(14) << r.throughputMBs << std::setw(14)
              << r.throughputGbps << std::fixed << std::setprecision(1)
              << std::setw(11) << r.cpuPercentage << "%" << std::fixed
              << std::setprecision(2) << std::setw(11) << r.speedupVsBaseline
              << "x\n";
  }
  std::cout << "==============================================================="
               "=========================================\n\n";
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

    group.addMeasurement("1. Standard send()", [&]() {
      return runner.runStandardSend().elapsedSeconds;
    });
    group.addMeasurement("2. io_uring Standard Send", [&]() {
      return runner.runIoUringStandardSend().elapsedSeconds;
    });
    group.addMeasurement("3. io_uring SEND_ZC Fixed Buffers", [&]() {
      return runner.runIoUringZeroCopySend().elapsedSeconds;
    });

    return results;
  }
};

AD_REGISTER_BENCHMARK(ZeroCopySenderBenchmark);
#endif

}  // namespace ad_benchmark

#ifndef QLEVER_HAS_BENCHMARK_INFRASTRUCTURE
// Standalone executable entry point
int main([[maybe_unused]] int argc, [[maybe_unused]] char** argv) {
  (void)argc;
  (void)argv;
  std::cout << "==============================================================="
               "===========\n";
  std::cout << " QLever Export Optimization: Zero-Copy Network Socket Sender "
               "Benchmark\n";
  std::cout << "==============================================================="
               "===========\n";

  try {
    ad_benchmark::ZeroCopySenderBenchmarkRunner runner;

    std::vector<ad_benchmark::BenchmarkMetric> results;
    std::cout << ">>> Running 1. Standard send() Baseline ... " << std::flush;
    results.push_back(runner.runStandardSend());
    std::cout << "Done.\n";

    std::cout << ">>> Running 2. io_uring Standard Send ... " << std::flush;
    results.push_back(runner.runIoUringStandardSend());
    std::cout << "Done.\n";

    std::cout << ">>> Running 3. io_uring Zero-Copy Send (SEND_ZC) ... "
              << std::flush;
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

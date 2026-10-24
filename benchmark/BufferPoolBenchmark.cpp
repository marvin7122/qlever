// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Standalone micro-benchmark for `BufferPool` (the vmcache-style pool for the
// on-disk vocabulary): many small, scattered reads of one synthetic file,
// served by plain `pread` (the no-pool baseline), by the pool with a budget
// that fits the file (all hits), and by the pool with a small budget
// (eviction churn), single-threaded and with 8 reader threads. Prints one
// CSV line per scenario (median of 5 repetitions), for before/after
// comparisons of pool changes. Usage: BufferPoolBenchmark [fileMiB]

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "util/BufferPool.h"

namespace {
using ad_utility::BufferPool;
using Clock = std::chrono::steady_clock;

// The OS page size for the synthetic file layout (queried here, so this
// benchmark also compiles against pool versions without a page-size API).
const size_t P = [] {
  const long queried = ::sysconf(_SC_PAGESIZE);
  return queried > 0 ? static_cast<size_t>(queried) : size_t{4096};
}();

struct Ranges {
  std::vector<size_t> numBytes;
  std::vector<uint64_t> offsets;
  std::vector<std::string> buffers;
  std::vector<char*> targets;
};

Ranges makeBatch(std::mt19937_64& random, uint64_t limit, size_t count) {
  Ranges batch;
  for (size_t i = 0; i < count; ++i) {
    const uint64_t offset = random() % (limit - 4096);
    const size_t size = 16 + random() % 2000;
    batch.offsets.push_back(offset);
    batch.numBytes.push_back(size);
    batch.buffers.emplace_back(size, 'X');
  }
  for (auto& buffer : batch.buffers) {
    batch.targets.push_back(buffer.data());
  }
  return batch;
}

uint64_t checksumOf(const Ranges& batch) {
  uint64_t sum = 0;
  for (const auto& buffer : batch.buffers) {
    sum += static_cast<unsigned char>(buffer.front()) +
           static_cast<unsigned char>(buffer.back());
  }
  return sum;
}

bool matchesContents(const Ranges& batch, const std::string& contents) {
  for (size_t i = 0; i < batch.buffers.size(); ++i) {
    if (batch.buffers[i] !=
        contents.substr(batch.offsets[i], batch.numBytes[i])) {
      return false;
    }
  }
  return true;
}

// Run `batches` through `serve` until at least `minSeconds` have passed;
// return {ns per read, checksum}. `serve` gets a batch index and the batch.
template <typename Serve>
std::pair<double, uint64_t> timeBatches(std::vector<Ranges>& batches,
                                        Serve&& serve) {
  uint64_t checksum = 0;
  uint64_t reads = 0;
  const auto start = Clock::now();
  size_t round = 0;
  double seconds = 0;
  while (seconds < 1.0) {
    for (size_t i = 0; i < batches.size(); ++i) {
      serve(batches[i]);
      checksum += checksumOf(batches[i]);
      reads += batches[i].buffers.size();
    }
    ++round;
    seconds = std::chrono::duration<double>(Clock::now() - start).count();
    if (round >= 20) break;
  }
  return {seconds * 1e9 / static_cast<double>(reads), checksum};
}

double median(std::vector<double> values) {
  std::sort(values.begin(), values.end());
  return values[values.size() / 2];
}

void report(const char* scenario, double nsPerRead, uint64_t checksum,
            const BufferPool* pool) {
  std::printf("%s,%.1f,%.0f,%llu,%llu,%llu\n", scenario, nsPerRead,
              1e9 / nsPerRead, static_cast<unsigned long long>(checksum),
              static_cast<unsigned long long>(pool ? pool->numPageLoads() : 0),
              static_cast<unsigned long long>(pool ? pool->numEvictions() : 0));
  std::fflush(stdout);
}
}  // namespace

int main(int argc, char** argv) {
  const size_t fileMiB = argc > 1 ? std::stoul(argv[1]) : 256;
  const std::string filename = "bufferPoolBenchmark.dat";
  const size_t fileBytes = fileMiB << 20;

  // Synthetic contents: the byte at `i` also depends on its page, so a copy
  // from a wrong page is noticed (same scheme as `BufferPoolTest`).
  std::string contents(fileBytes, 'a');
  for (size_t i = 0; i < fileBytes; ++i) {
    contents[i] = static_cast<char>('a' + (i + i / P) % 26);
  }
  {
    FILE* file = std::fopen(filename.c_str(), "wb");
    if (file == nullptr || std::fwrite(contents.data(), 1, contents.size(),
                                       file) != contents.size()) {
      std::fprintf(stderr, "cannot write %s\n", filename.c_str());
      return 1;
    }
    std::fclose(file);
  }

  constexpr size_t numBatches = 64;
  constexpr size_t batchSize = 512;
  std::printf(
      "scenario,ns_per_read,reads_per_s,checksum,page_loads,"
      "evictions\n");

  // 1. Baseline: one `pread` per range (the no-pool read path, page-cache
  // warm after the file write).
  {
    const int fd = ::open(filename.c_str(), O_RDONLY);
    std::mt19937_64 random{42};
    std::vector<Ranges> batches;
    for (size_t i = 0; i < numBatches; ++i) {
      batches.push_back(makeBatch(random, fileBytes, batchSize));
    }
    auto serve = [fd](Ranges& batch) {
      for (size_t i = 0; i < batch.buffers.size(); ++i) {
        const ssize_t n = ::pread(fd, batch.targets[i], batch.numBytes[i],
                                  static_cast<off_t>(batch.offsets[i]));
        if (n != static_cast<ssize_t>(batch.numBytes[i])) std::abort();
      }
    };
    serve(batches.front());
    if (!matchesContents(batches.front(), contents)) std::abort();
    std::vector<double> times;
    uint64_t checksum = 0;
    for (int rep = 0; rep < 5; ++rep) {
      auto [ns, sum] = timeBatches(batches, serve);
      times.push_back(ns);
      checksum = sum;
    }
    report("pread_baseline", median(times), checksum, nullptr);
    ::close(fd);
  }

  // 2.-5. Pool scenarios: a budget that fits the file (all hits after the
  // warmup) and one sixteenth of it (eviction churn), each with 1 and with
  // 8 reader threads.
  for (size_t budgetDivisor : {size_t{1}, size_t{16}}) {
    for (size_t numThreads : {size_t{1}, size_t{8}}) {
      std::unique_ptr<BufferPool> poolOwner;
      try {
        poolOwner = std::make_unique<BufferPool>(filename, fileBytes,
                                                 fileBytes / budgetDivisor);
      } catch (const std::exception& e) {
        std::fprintf(stderr, "pool creation failed: %s\n", e.what());
        return 1;
      }
      BufferPool& pool = *poolOwner;
      std::vector<std::vector<Ranges>> perThread(numThreads);
      for (size_t t = 0; t < numThreads; ++t) {
        std::mt19937_64 random{1000 + t};
        for (size_t i = 0; i < numBatches / numThreads * 2; ++i) {
          perThread[t].push_back(makeBatch(random, fileBytes, batchSize));
        }
      }
      // Warmup (loads pages; also verifies the bytes once per thread).
      for (size_t t = 0; t < numThreads; ++t) {
        Ranges& batch = perThread[t].front();
        pool.readAll(batch.numBytes, batch.offsets, batch.targets);
        if (!matchesContents(batch, contents)) std::abort();
      }
      // Fixed-count measurement: each thread runs its batches exactly
      // `passes` times; the wall time gives ns per read exactly.
      constexpr size_t passes = 3;
      uint64_t checksum = 0;
      std::vector<double> fixedTimes;
      for (int rep = 0; rep < 5; ++rep) {
        std::atomic<uint64_t> total{0};
        auto work = [&](size_t t) {
          uint64_t sum = 0;
          for (size_t pass = 0; pass < passes; ++pass) {
            for (auto& batch : perThread[t]) {
              pool.readAll(batch.numBytes, batch.offsets, batch.targets);
              sum += checksumOf(batch);
            }
          }
          total.fetch_add(sum, std::memory_order_relaxed);
        };
        const auto start = Clock::now();
        std::vector<std::thread> threads;
        for (size_t t = 1; t < numThreads; ++t) {
          threads.emplace_back(work, t);
        }
        work(0);
        for (auto& thread : threads) thread.join();
        const double seconds =
            std::chrono::duration<double>(Clock::now() - start).count();
        const uint64_t reads = static_cast<uint64_t>(passes) * batchSize * [&] {
          size_t n = 0;
          for (const auto& v : perThread) n += v.size();
          return n;
        }();
        fixedTimes.push_back(seconds * 1e9 / static_cast<double>(reads));
        checksum = total.load();
      }
      const std::string scenario = std::string{"pool_"} +
                                   (budgetDivisor == 1 ? "hit" : "churn") +
                                   "_t" + std::to_string(numThreads);
      report(scenario.c_str(), median(fixedTimes), checksum, &pool);
    }
  }
  std::remove(filename.c_str());
  return 0;
}

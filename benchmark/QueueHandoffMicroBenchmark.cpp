// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel (marvin.stoetzel@mailbox.org)

// Microbenchmark for the producer/consumer handoff that
// `runStreamAsync` (see `util/AsyncStream.h`) performs for every HTTP export
// chunk. Two arms move the same items through two queues:
//   - "tsq":  `ad_utility::data_structures::ThreadSafeQueue` (mutex + two
//             condition variables), as used by `runStreamAsync`.
//   - "spsc": a single-producer single-consumer ring buffer with the
//             techniques of "Optimizing a Lock-Free Ring Buffer" (D. Alvarez
//             Rosa): cache-line-aligned head/tail, acquire/release instead of
//             seq_cst, cached copies of the other side's index, power-of-two
//             capacity with a mask. It spins (then yields) on full/empty.
// Payloads:
//   - "chunk1m" / "chunk64k": a `std::string` built by copying 1 MiB / 64 KiB
//     from a source buffer (what `convertStreamGeneratorForChunkedTransfer`
//     does for every chunk), moved through the queue; the consumer reads one
//     byte per 4 KiB page and frees the string.
//   - "u64": a `uint64_t` (the regime of the blog post).
// Environment: QUEUE_BENCH_ARM, QUEUE_BENCH_PAYLOAD, QUEUE_BENCH_CAPACITY
// (default 100, as in `setBody`), QUEUE_BENCH_MIN_SECONDS (default 10).
// Every measurement runs whole rounds until at least MIN_SECONDS have passed
// and reports ns per item.

#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "util/ThreadSafeQueue.h"
#include "util/jthread.h"

namespace ad_benchmark {
namespace {

// Single-producer single-consumer ring buffer ("V5" of the blog post).
template <typename T>
class SpscRing {
  static constexpr size_t kLine = 64;
  std::vector<T> buffer_;
  size_t mask_;
  alignas(kLine) std::atomic<size_t> head_{0};
  alignas(kLine) size_t tailCached_{0};  // producer's copy of `tail_`
  alignas(kLine) std::atomic<size_t> tail_{0};
  alignas(kLine) size_t headCached_{0};  // consumer's copy of `head_`
  alignas(kLine) std::atomic<bool> finished_{false};

 public:
  explicit SpscRing(size_t capacity)
      : buffer_(std::bit_ceil(capacity + 1)),
        mask_{std::bit_ceil(capacity + 1) - 1} {}

  void push(T value) {
    const size_t head = head_.load(std::memory_order_relaxed);
    const size_t next = (head + 1) & mask_;
    size_t spins = 0;
    while (next == tailCached_) {
      tailCached_ = tail_.load(std::memory_order_acquire);
      if (next != tailCached_) break;
      if (++spins > 64) std::this_thread::yield();
    }
    buffer_[head] = std::move(value);
    head_.store(next, std::memory_order_release);
  }

  void finish() { finished_.store(true, std::memory_order_release); }

  bool pop(T& out) {
    const size_t tail = tail_.load(std::memory_order_relaxed);
    size_t spins = 0;
    while (tail == headCached_) {
      headCached_ = head_.load(std::memory_order_acquire);
      if (tail != headCached_) break;
      if (finished_.load(std::memory_order_acquire)) {
        headCached_ = head_.load(std::memory_order_acquire);
        if (tail == headCached_) return false;
        break;
      }
      if (++spins > 64) std::this_thread::yield();
    }
    out = std::move(buffer_[tail]);
    tail_.store((tail + 1) & mask_, std::memory_order_release);
    return true;
  }
};

std::string envOr(const char* name, std::string def) {
  const char* v = std::getenv(name);
  return v ? std::string{v} : def;
}

struct Config {
  std::string arm = envOr("QUEUE_BENCH_ARM", "tsq");
  std::string payload = envOr("QUEUE_BENCH_PAYLOAD", "chunk1m");
  size_t capacity = std::stoull(envOr("QUEUE_BENCH_CAPACITY", "100"));
  double minSeconds = std::stod(envOr("QUEUE_BENCH_MIN_SECONDS", "10"));
};

// Items per round, chosen so that one round takes roughly 0.1-1 s.
size_t itemsPerRound(const Config& c) {
  if (c.payload == "u64") return 10'000'000;
  if (c.payload == "chunk64k") return 20'000;
  return 2'000;
}

size_t chunkBytes(const Config& c) {
  return c.payload == "chunk64k" ? size_t{64} << 10 : size_t{1} << 20;
}

// Run one round; return a checksum so that no work is optimized away.
template <typename T, typename MakeItem, typename Consume>
uint64_t runRound(const Config& c, size_t n, MakeItem makeItem,
                  Consume consume) {
  uint64_t checksum = 0;
  if (c.arm == "tsq") {
    ad_utility::data_structures::ThreadSafeQueue<T> queue{c.capacity};
    ad_utility::JThread producer{[&] {
      for (size_t i = 0; i < n; ++i) {
        if (!queue.push(makeItem(i))) return;
      }
      queue.finish();
    }};
    while (auto item = queue.pop()) checksum += consume(*item);
  } else {
    SpscRing<T> ring{c.capacity};
    ad_utility::JThread producer{[&] {
      for (size_t i = 0; i < n; ++i) ring.push(makeItem(i));
      ring.finish();
    }};
    T item{};
    while (ring.pop(item)) checksum += consume(item);
  }
  return checksum;
}

class QueueHandoffMicroBenchmark : public BenchmarkInterface {
  std::string name() const final {
    return "Producer/consumer queue handoff (ThreadSafeQueue vs SPSC ring)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    const Config c;
    const size_t n = itemsPerRound(c);
    const size_t bytes = chunkBytes(c);
    const std::string source(bytes, 'x');

    size_t totalItems = 0;
    double seconds = 0;
    uint64_t checksum = 0;
    auto label = c.arm + "/" + c.payload + "/cap" + std::to_string(c.capacity);
    auto& m = results.addMeasurement(label, [&] {
      auto start = std::chrono::steady_clock::now();
      do {
        if (c.payload == "u64") {
          checksum += runRound<uint64_t>(
              c, n, [](size_t i) { return uint64_t{i}; },
              [](uint64_t v) { return v; });
        } else {
          checksum += runRound<std::string>(
              c, n,
              [&source](size_t) {
                return std::string{source.data(), source.size()};
              },
              [](const std::string& s) {
                uint64_t sum = 0;
                for (size_t i = 0; i < s.size(); i += 4096) {
                  sum += static_cast<unsigned char>(s[i]);
                }
                return sum;
              });
        }
        totalItems += n;
        seconds = std::chrono::duration<double>(
                      std::chrono::steady_clock::now() - start)
                      .count();
      } while (seconds < c.minSeconds);
    });
    const double nsPerItem = seconds * 1e9 / static_cast<double>(totalItems);
    m.metadata().addKeyValuePair("items", totalItems);
    m.metadata().addKeyValuePair("seconds", seconds);
    m.metadata().addKeyValuePair("nsPerItem", nsPerItem);
    m.metadata().addKeyValuePair("checksum", checksum);
    std::cout << "RESULT arm=" << c.arm << " payload=" << c.payload
              << " capacity=" << c.capacity << " items=" << totalItems
              << " seconds=" << seconds << " ns_per_item=" << nsPerItem
              << std::endl;
    return results;
  }
};

}  // namespace

AD_REGISTER_BENCHMARK(QueueHandoffMicroBenchmark);

}  // namespace ad_benchmark

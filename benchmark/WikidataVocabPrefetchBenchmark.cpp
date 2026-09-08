// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

// Wikidata-backed vocabulary resolution benchmark. It memory-maps the real
// Wikidata external vocabulary (`words.external` plus
// `words.external.offsets`, 1.58B entries) and compares three resolution
// shapes over uniform-random IDs: a plain sequential baseline, a
// pipelined-no-prefetch loop (extra index pass plus per-row callback), and
// software-prefetched loops at distances K = 8 and K = 16. The arm order
// rotates every repetition so page-cache warmth is balanced across arms.
// Repetition 0 of each arm is the coldest touch; the reported warm median
// covers repetitions 1 to 4.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <iostream>
#include <random>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <xmmintrin.h>
#endif

namespace {

template <typename T>
inline void escape(T& val) {
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" : "+r,m"(val) : : "memory");
#endif
}

inline void prefetchAddr(const void* address) noexcept {
#if defined(__x86_64__) || defined(_M_X64)
  _mm_prefetch(static_cast<const char*>(address), _MM_HINT_T0);
#elif defined(__GNUC__) || defined(__clang__)
  __builtin_prefetch(address, 0, 3);
#endif
}

struct Mapping {
  const uint8_t* data{nullptr};
  size_t size{0};
};

Mapping mmapFile(const char* path) {
  Mapping mapping;
  const int fd = ::open(path, O_RDONLY);
  if (fd < 0) {
    std::perror("open");
    std::exit(1);
  }
  struct stat fileStat;
  if (::fstat(fd, &fileStat) != 0) {
    std::perror("fstat");
    std::exit(1);
  }
  void* base = ::mmap(nullptr, static_cast<size_t>(fileStat.st_size),
                      PROT_READ, MAP_SHARED, fd, 0);
  if (base == MAP_FAILED) {
    std::perror("mmap");
    std::exit(1);
  }
  ::close(fd);
  mapping.data = static_cast<const uint8_t*>(base);
  mapping.size = static_cast<size_t>(fileStat.st_size);
  return mapping;
}

// Resolve one ID and fold every word byte into the checksum, so the data
// page is really touched and cannot be skipped by the optimizer.
inline void resolveOne(const uint64_t* offsets, const uint8_t* words,
                       size_t wordsSize, uint64_t id, uint64_t& checksum) {
  const uint64_t begin = offsets[id];
  const uint64_t end = offsets[id + 1];
  if (begin > end || end > wordsSize) {
    std::fprintf(stderr, "bad offsets for id %llu begin %llu end %llu\n",
                 static_cast<unsigned long long>(id),
                 static_cast<unsigned long long>(begin),
                 static_cast<unsigned long long>(end));
    std::exit(1);
  }
  for (uint64_t pos = begin; pos < end; ++pos) {
    checksum += words[pos];
  }
}

double medianOf(std::vector<double> values) {
  std::sort(values.begin(), values.end());
  return values[values.size() / 2];
}

}  // namespace

int main(int argc, char** argv) {
  const char* wordsPath = argc > 1
                              ? argv[1]
                              : "/local/data-ssd/stoetzem/wikidata/"
                                "wikidata.vocabulary.words.external";
  const char* offsetsPath =
      argc > 2 ? argv[2]
               : "/local/data-ssd/stoetzem/wikidata/"
                 "wikidata.vocabulary.words.external.offsets";
  const size_t numLookups = argc > 3 ? std::stoull(argv[3]) : 1'000'000;
  const size_t numReps = 5;

  const Mapping words = mmapFile(wordsPath);
  const Mapping offsetsMap = mmapFile(offsetsPath);
  const auto* offsets =
      reinterpret_cast<const uint64_t*>(offsetsMap.data);
  const size_t numEntries = offsetsMap.size / sizeof(uint64_t) - 1;

  std::mt19937_64 rng(42);
  std::uniform_int_distribution<uint64_t> dist(0, numEntries - 1);
  std::vector<uint64_t> ids(numLookups);
  for (size_t i = 0; i < numLookups; ++i) {
    ids[i] = dist(rng);
  }

  std::cout << "config words_bytes=" << words.size
            << " offsets_bytes=" << offsetsMap.size
            << " entries=" << numEntries << " lookups=" << numLookups
            << " reps=" << numReps << "\n";

  // Arms: 0 = baseline, 1 = pipelined-no-prefetch, 2 = prefetch K = 8,
  // 3 = prefetch K = 16. The order rotates every repetition.
  const char* armNames[4] = {"baseline", "restructure", "prefetch-K8",
                             "prefetch-K16"};
  const size_t armDistances[4] = {0, 0, 8, 16};
  std::vector<std::vector<double>> armMs(4, std::vector<double>(numReps));

  for (size_t rep = 0; rep < numReps; ++rep) {
    for (size_t slot = 0; slot < 4; ++slot) {
      const size_t arm = (slot + rep) % 4;
      uint64_t checksum = 0;
      const auto start = std::chrono::steady_clock::now();
      if (arm == 0) {
        for (size_t i = 0; i < numLookups; ++i) {
          resolveOne(offsets, words.data, words.size, ids[i], checksum);
        }
      } else {
        // Extra index pass plus per-row callback, as in the PR87 resolver.
        std::vector<uint64_t> raw(numLookups);
        for (size_t i = 0; i < numLookups; ++i) {
          raw[i] = ids[i];
        }
        auto store = [&checksum](uint64_t byte) { checksum += byte; };
        const size_t distance = armDistances[arm];
        for (size_t i = 0; i < numLookups; ++i) {
          if (arm >= 2 && i + distance < numLookups) {
            const uint64_t pfId = raw[i + distance];
            prefetchAddr(&offsets[pfId]);
          }
          if (arm >= 2 && i + distance / 2 < numLookups) {
            const uint64_t midId = raw[i + distance / 2];
            const uint64_t midBegin = offsets[midId];
            if (midBegin < words.size) {
              prefetchAddr(words.data + midBegin);
            }
          }
          const uint64_t id = raw[i];
          const uint64_t begin = offsets[id];
          const uint64_t end = offsets[id + 1];
          if (begin > end || end > words.size) {
            std::fprintf(stderr, "bad offsets\n");
            std::exit(1);
          }
          for (uint64_t pos = begin; pos < end; ++pos) {
            store(words.data[pos]);
          }
        }
      }
      const auto end = std::chrono::steady_clock::now();
      escape(checksum);
      const double ms =
          std::chrono::duration<double, std::milli>(end - start).count();
      armMs[arm][rep] = ms;
      std::cout << "rep arm=" << armNames[arm] << " rep=" << rep
                << " ms=" << ms << " checksum=" << checksum << "\n";
    }
  }

  for (size_t arm = 0; arm < 4; ++arm) {
    std::vector<double> warm(armMs[arm].begin() + 1, armMs[arm].end());
    const double coldMs = armMs[arm][0];
    const double warmMs = medianOf(warm);
    const double allMs = medianOf(armMs[arm]);
    std::cout << "RESULT arm=" << armNames[arm] << " cold_ms=" << coldMs
              << " warm_median_ms=" << warmMs << " all_median_ms=" << allMs
              << " cold_M_per_s="
              << (static_cast<double>(numLookups) / 1e6) / (coldMs / 1e3)
              << " warm_M_per_s="
              << (static_cast<double>(numLookups) / 1e6) / (warmMs / 1e3)
              << "\n";
  }
  return 0;
}

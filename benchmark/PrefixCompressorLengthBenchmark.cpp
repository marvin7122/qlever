// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Long-running, allocation-counting benchmark of `PrefixCompressor` decoding
// (measurement tool for ad-freiburg/qlever#3523, not part of the PR). The same
// source is built on the previous stack part (only `decompress` exists) and on
// this part (`decompress`, `decompressInto`, `maxDecompressedSize`), so the
// `decompress` and `batch-vector` modes compare the two commits and the
// `decompress-into` and `batch-arena` modes compare the APIs of this part.
//
// Usage: PrefixCompressorLengthBenchmark <mode> <numWords> <minSeconds>
//   mode: decompress | batch-vector | decompress-into | batch-arena
// Output: one line of key=value pairs. Every allocation through the global
// `operator new` is counted; `allocs_per_word` is taken over the timed loop.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <vector>

#include "CompilationInfo.h"
#include "backports/span.h"
#include "index/vocabulary/PrefixCompressor.h"

namespace {
uint64_t numAllocations = 0;
uint64_t numAllocatedBytes = 0;

void* countedAlloc(std::size_t size) {
  ++numAllocations;
  numAllocatedBytes += size;
  if (void* p = std::malloc(size == 0 ? 1 : size)) {
    return p;
  }
  throw std::bad_alloc{};
}
}  // namespace

void* operator new(std::size_t size) { return countedAlloc(size); }
void* operator new[](std::size_t size) { return countedAlloc(size); }
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
  ++numAllocations;
  numAllocatedBytes += size;
  return std::malloc(size == 0 ? 1 : size);
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  ++numAllocations;
  numAllocatedBytes += size;
  return std::malloc(size == 0 ? 1 : size);
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace {

// A variable template, so that the requirement is checked by substitution
// (and yields `false`) on commits without the in-place API.
template <typename C>
constexpr bool hasDecompressIntoV =
    requires(const C& p, std::string_view s, ql::span<char> o) {
      p.decompressInto(s, o);
      p.maxDecompressedSize(s);
    };
constexpr bool hasDecompressInto = hasDecompressIntoV<PrefixCompressor>;

// The word set of `benchmark/PrefixCompressorBenchmark.cpp`, generalized to
// `numWords`: three of four words are 77-byte Wikidata entity IRIs, one is a
// 53-byte literal without a codebook prefix.
struct Data {
  PrefixCompressor compressor;
  std::vector<std::string> storage;
  std::vector<std::string_view> compressed;
  size_t totalDecompressedSize = 0;
  size_t maxWordSize = 0;
};

Data makeData(size_t numWords) {
  Data d;
  d.compressor.buildCodebook(std::vector<std::string>{
      "http://www.wikidata.org/entity/", "http://www.wikidata.org/prop/direct/",
      "http://www.wikidata.org/value/", "http://schema.org/",
      "http://www.w3.org/2000/01/rdf-schema#",
      "<http://example.org/property/"});
  constexpr std::string_view alphabet{
      "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
  d.storage.reserve(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    std::string suffix;
    for (size_t c = 0; c < 45; ++c) {
      suffix += alphabet[(i * 17 + c * 31) % alphabet.size()];
    }
    const std::string word = (i % 4 == 3)
                                 ? "literal-" + suffix
                                 : "http://www.wikidata.org/entity/Q" + suffix;
    d.totalDecompressedSize += word.size();
    d.maxWordSize = std::max(d.maxWordSize, word.size());
    d.storage.push_back(d.compressor.compress(word));
  }
  d.compressed.assign(d.storage.begin(), d.storage.end());
  return d;
}

// FNV-1a over the decoded bytes of one pass, to check that all modes and both
// commits produce identical output.
uint64_t fnv(uint64_t h, std::string_view s) {
  for (unsigned char c : s) {
    h = (h ^ c) * 1099511628211ULL;
  }
  return h;
}

// One pass over all words. Returns decoded bytes; updates `checksum` if
// `withChecksum` (only in the untimed verification pass).
// `C` is always `PrefixCompressor`; as a template parameter it makes the
// `decompressInto` calls dependent, so they are not compiled on commits
// without the in-place API.
template <bool withChecksum, typename C = PrefixCompressor>
size_t runPass(const Data& d, const std::string& mode, std::string& buffer,
               char* arena, uint64_t& checksum) {
  const C& compressor = d.compressor;
  size_t bytes = 0;
  if (mode == "decompress") {
    for (auto w : d.compressed) {
      std::string s = d.compressor.decompress(w);
      bytes += s.size();
      if constexpr (withChecksum) checksum = fnv(checksum, s);
    }
  } else if (mode == "batch-vector") {
    std::vector<std::string> out;
    out.reserve(d.compressed.size());
    for (auto w : d.compressed) {
      out.push_back(d.compressor.decompress(w));
      bytes += out.back().size();
    }
    if constexpr (withChecksum) {
      for (const auto& s : out) checksum = fnv(checksum, s);
    }
  } else {
    if constexpr (hasDecompressIntoV<C>) {
      if (mode == "decompress-into") {
        for (auto w : d.compressed) {
          size_t n = compressor.decompressInto(
              w, ql::span<char>{buffer.data(), buffer.size()});
          bytes += n;
          if constexpr (withChecksum)
            checksum = fnv(checksum, std::string_view{buffer.data(), n});
        }
      } else if (mode == "batch-arena") {
        size_t offset = 0;
        for (auto w : d.compressed) {
          size_t n = compressor.decompressInto(
              w,
              ql::span<char>{arena + offset, d.totalDecompressedSize - offset});
          offset += n;
        }
        bytes += offset;
        if constexpr (withChecksum)
          checksum = fnv(checksum, std::string_view{arena, offset});
      } else {
        std::fprintf(stderr, "unknown mode %s\n", mode.c_str());
        std::exit(2);
      }
    } else {
      std::fprintf(stderr,
                   "mode %s needs decompressInto (not in this commit)\n",
                   mode.c_str());
      std::exit(3);
    }
  }
  return bytes;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && (std::string_view{argv[1]} == "--version" ||
                    std::string_view{argv[1]} == "--help")) {
    std::printf(
        "PrefixCompressorLengthBenchmark git %.*s decompressInto=%d\n"
        "usage: %s <decompress|batch-vector|decompress-into|batch-arena> "
        "<numWords> <minSeconds>\n",
        static_cast<int>(qlever::version::GitShortHash.size()),
        qlever::version::GitShortHash.data(), hasDecompressInto ? 1 : 0,
        argv[0]);
    return 0;
  }
  if (argc != 4) {
    std::fprintf(stderr, "usage: %s <mode> <numWords> <minSeconds>\n", argv[0]);
    return 2;
  }
  const std::string mode = argv[1];
  const size_t numWords = std::strtoull(argv[2], nullptr, 10);
  const double minSeconds = std::strtod(argv[3], nullptr);
  const Data d = makeData(numWords);
  std::string buffer(d.maxWordSize, '\0');
  auto arena = std::make_unique<char[]>(d.totalDecompressedSize);

  // Untimed verification pass (checksum) and warm-up of about 0.5 s.
  // With `PCL_PASSES=<n>` in the environment the warm-up is skipped and the
  // timed loop runs exactly `n` passes, so that instruction and allocation
  // counters (valgrind) see a fixed amount of work.
  const char* fixedPassesEnv = std::getenv("PCL_PASSES");
  const size_t fixedPasses =
      fixedPassesEnv ? std::strtoull(fixedPassesEnv, nullptr, 10) : 0;
  uint64_t checksum = 14695981039346656037ULL;
  size_t verifyBytes = runPass<true>(d, mode, buffer, arena.get(), checksum);
  using Clock = std::chrono::steady_clock;
  uint64_t dummy = 0;
  auto warmStart = Clock::now();
  while (fixedPasses == 0 &&
         std::chrono::duration<double>(Clock::now() - warmStart).count() <
             0.5) {
    verifyBytes += runPass<false>(d, mode, buffer, arena.get(), dummy);
  }

  // Timed loop: whole passes until at least `minSeconds` have elapsed.
  const uint64_t allocsBefore = numAllocations;
  const uint64_t allocBytesBefore = numAllocatedBytes;
  size_t passes = 0;
  size_t bytes = 0;
  const auto start = Clock::now();
  double elapsed = 0;
  do {
    bytes += runPass<false>(d, mode, buffer, arena.get(), dummy);
    ++passes;
    elapsed = std::chrono::duration<double>(Clock::now() - start).count();
  } while (fixedPasses > 0 ? passes < fixedPasses : elapsed < minSeconds);
  const uint64_t allocs = numAllocations - allocsBefore;
  const uint64_t allocBytes = numAllocatedBytes - allocBytesBefore;
  const double decodes = static_cast<double>(passes) * numWords;
  std::printf(
      "git=%.*s mode=%s words=%zu passes=%zu decodes=%.0f seconds=%.6f "
      "ns_per_word=%.3f allocs_per_word=%.4f alloc_bytes_per_word=%.2f "
      "bytes=%zu checksum=%016llx\n",
      static_cast<int>(qlever::version::GitShortHash.size()),
      qlever::version::GitShortHash.data(), mode.c_str(), numWords, passes,
      decodes, elapsed, elapsed * 1e9 / decodes, allocs / decodes,
      allocBytes / decodes, bytes, static_cast<unsigned long long>(checksum));
  return verifyBytes == 0 ? 1 : 0;
}

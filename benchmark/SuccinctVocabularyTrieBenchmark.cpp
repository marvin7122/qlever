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

#include "index/vocabulary/SuccinctVocabularyTrie.h"

using namespace ql::index::vocab;

int main() {
  constexpr size_t NUM_QUERIES = 20'000'000;
  constexpr size_t BIT_WORDS = 100'000;  // 6.4 million bits

  std::cout << "Benchmarking SuccinctVocabularyTrie POPCNT Rank1 across " << NUM_QUERIES << " queries...\n";

  std::vector<uint64_t> bits(BIT_WORDS, 0xAAAAAAAAAAAAAAAAULL);  // alternating bits
  std::vector<char> labels(BIT_WORDS, 'x');

  SuccinctVocabularyTrie trie;
  trie.setMockTopology(bits, labels);

  auto t0 = std::chrono::high_resolution_clock::now();
  size_t totalRank = 0;
  for (size_t i = 0; i < NUM_QUERIES; ++i) {
    size_t bitIdx = (i * 137) % (BIT_WORDS * 64);
    totalRank += trie.rank1(bitIdx);
  }
  auto t1 = std::chrono::high_resolution_clock::now();

  double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  std::cout << "Rank1 Throughput: " << (NUM_QUERIES / (ms / 1000.0)) / 1e6 << " M queries/sec ("
            << ms << " ms, sum: " << totalRank << ")\n";

  return 0;
}

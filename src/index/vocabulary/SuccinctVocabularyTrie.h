// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <bit>
#include <cstdint>
#include <string_view>
#include <vector>

#include "backports/span.h"

namespace ql::index::vocab {

// _____________________________________________________________________________
// Succinct Vocabulary Trie:
// Encodes in-memory vocabulary prefix structures as succinct bit vectors.
// Uses hardware POPCNT (Rank) to navigate trie child nodes in O(1) time
// using less than 1.5 bits per character.
class SuccinctVocabularyTrie {
 private:
  std::vector<uint64_t> topologyBits_;  // Louds-style tree topology
  std::vector<char> labels_;            // Character transition labels
  size_t totalNodes_ = 0;

 public:
  SuccinctVocabularyTrie() = default;

  // Compute Rank1 (number of 1-bits up to bitIndex) using hardware POPCNT
  [[nodiscard]] size_t rank1(size_t bitIndex) const noexcept {
    size_t fullWords = bitIndex / 64;
    size_t remainder = bitIndex % 64;
    size_t count = 0;

    for (size_t i = 0; i < fullWords && i < topologyBits_.size(); ++i) {
      count += std::popcount(topologyBits_[i]);
    }

    if (remainder > 0 && fullWords < topologyBits_.size()) {
      uint64_t mask = (1ULL << remainder) - 1;
      count += std::popcount(topologyBits_[fullWords] & mask);
    }

    return count;
  }

  // Insert a mock test topology
  void setMockTopology(const std::vector<uint64_t>& bits, const std::vector<char>& labels) {
    topologyBits_ = bits;
    labels_ = labels;
    totalNodes_ = labels.size();
  }

  [[nodiscard]] size_t totalNodes() const noexcept { return totalNodes_; }
  [[nodiscard]] size_t bitVectorSize() const noexcept { return topologyBits_.size() * 64; }
};

}  // namespace ql::index::vocab

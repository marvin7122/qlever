// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include "index/vocabulary/SuccinctVocabularyTrie.h"

using namespace ql::index::vocab;

TEST(SuccinctVocabularyTrieTest, HardwarePopcntRank1) {
  SuccinctVocabularyTrie trie;

  // Bits: 0b1011 (11 in decimal) -> 3 set bits in first 4 positions
  std::vector<uint64_t> bits = {0b1011ULL, 0xFFULL};
  std::vector<char> labels = {'a', 'b', 'c'};
  trie.setMockTopology(bits, labels);

  EXPECT_EQ(trie.rank1(0), 0u);
  EXPECT_EQ(trie.rank1(1), 1u);
  EXPECT_EQ(trie.rank1(2), 2u);
  EXPECT_EQ(trie.rank1(3), 2u);
  EXPECT_EQ(trie.rank1(4), 3u);
  EXPECT_EQ(trie.rank1(64), 3u);
  EXPECT_EQ(trie.rank1(72), 11u);  // 3 from first word + 8 from second word
}

// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "engine/BranchlessTypeDispatcher.h"
#include "util/Exception.h"

using ql::engine::BranchlessTypeDispatcher;

namespace {
// Format `id` with `rawTerm` into a local buffer and return the result.
std::string format(ValueId id, std::string_view rawTerm,
                   const BranchlessTypeDispatcher::LookupTable& lut =
                       BranchlessTypeDispatcher::defaultLut()) {
  std::array<char, 256> buffer{};
  char* end = BranchlessTypeDispatcher::dispatchTermFormat(id, rawTerm,
                                                           buffer.data(), lut);
  return {buffer.data(), end};
}
}  // namespace

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcher, FormatsNumbersAndVocabTerms) {
  EXPECT_EQ(format(ValueId::makeFromInt(-42), ""),
            "\"-42\"^^<http://www.w3.org/2001/XMLSchema#integer>");
  EXPECT_EQ(format(ValueId::makeFromBool(true), "",
                   BranchlessTypeDispatcher::turtleLut()),
            "true");
  EXPECT_EQ(format(ValueId::makeFromVocabIndex(VocabIndex::make(3)),
                   "http://example.org/a"),
            "<http://example.org/a>");
}

// _____________________________________________________________________________
// Every valid datatype except `Undefined` has a real formatter in all tables;
// in particular `SecondaryVocabIndex` is formatted like `VocabIndex`.
TEST(BranchlessTypeDispatcher, SecondaryVocabIndexIsFormattedLikeVocabIndex) {
  const auto vocabId = ValueId::makeFromVocabIndex(VocabIndex::make(3));
  const auto secondaryId =
      ValueId::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(3));
  for (const auto* lut : {&BranchlessTypeDispatcher::defaultLut(),
                          &BranchlessTypeDispatcher::turtleLut(),
                          &BranchlessTypeDispatcher::rawVocabLut()}) {
    EXPECT_EQ(format(secondaryId, "http://example.org/b", *lut),
              format(vocabId, "http://example.org/b", *lut));
    EXPECT_FALSE(format(secondaryId, "http://example.org/b", *lut).empty());
  }
}

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcher, CheckedBatchFormatRejectsTooSmallBuffer) {
  const std::vector<ValueId> ids{
      ValueId::makeFromInt(7),
      ValueId::makeFromVocabIndex(VocabIndex::make(1))};
  const std::vector<std::string_view> rawTerms{"", "http://example.org/c"};

  std::vector<char> buffer(
      BranchlessTypeDispatcher::maxFormattedBytes(rawTerms[0]) +
      BranchlessTypeDispatcher::maxFormattedBytes(rawTerms[1]));
  const size_t written = BranchlessTypeDispatcher::dispatchBatchTermFormat(
      ids, rawTerms, ql::span<char>{buffer});
  EXPECT_EQ(std::string_view(buffer.data(), written),
            "\"7\"^^<http://www.w3.org/2001/XMLSchema#integer>"
            "<http://example.org/c>");

  std::vector<char> tooSmall(written - 1);
  EXPECT_THROW(BranchlessTypeDispatcher::dispatchBatchTermFormat(
                   ids, rawTerms, ql::span<char>{tooSmall}),
               ad_utility::Exception);
}

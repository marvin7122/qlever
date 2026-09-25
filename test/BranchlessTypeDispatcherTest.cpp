// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <string_view>

#include "engine/BranchlessTypeDispatcher.h"
#include "global/ValueId.h"

using ql::engine::BranchlessTypeDispatcher;

// Every datatype except `Undefined` must have a real formatter in each of the
// three lookup tables. A datatype that is missing from a table would silently
// be dropped from the export (this happened for `SecondaryVocabIndex`).
TEST(BranchlessTypeDispatcher, EveryDatatypeHasAFormatter) {
  const std::array<const BranchlessTypeDispatcher::LookupTable*, 3> luts{
      &BranchlessTypeDispatcher::defaultLut(),
      &BranchlessTypeDispatcher::turtleLut(),
      &BranchlessTypeDispatcher::rawVocabLut()};
  for (const auto* lut : luts) {
    for (size_t i = 0; i <= static_cast<size_t>(Datatype::MaxValue); ++i) {
      const auto datatype = static_cast<Datatype>(i);
      const auto& descriptor = (*lut)[i];
      ASSERT_NE(descriptor.formatFn_, nullptr) << toString(datatype);
      if (datatype == Datatype::Undefined) {
        EXPECT_EQ(descriptor.formatFn_, &ql::engine::detail::formatUndefined);
      } else {
        EXPECT_NE(descriptor.formatFn_, &ql::engine::detail::formatUndefined)
            << toString(datatype);
      }
    }
  }
}

// `SecondaryVocabIndex` terms are rendered like `VocabIndex` terms.
TEST(BranchlessTypeDispatcher, SecondaryVocabIndexMatchesVocabIndex) {
  const auto& lut = BranchlessTypeDispatcher::defaultLut();
  const auto& vocab = lut[static_cast<size_t>(Datatype::VocabIndex)];
  const auto& secondary =
      lut[static_cast<size_t>(Datatype::SecondaryVocabIndex)];
  EXPECT_EQ(secondary.prefix_, vocab.prefix_);
  EXPECT_EQ(secondary.suffix_, vocab.suffix_);
  EXPECT_EQ(secondary.formatFn_, vocab.formatFn_);
}

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "backports/string.h"

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteExactSize) {
  std::string s = "initial";
  const std::string text = "Hello, World!";
  ql::resize_and_overwrite(s, text.size(), [&](char* buf, size_t count) {
    EXPECT_EQ(count, text.size());
    std::memcpy(buf, text.data(), text.size());
    return text.size();
  });
  EXPECT_EQ(s, text);
  EXPECT_EQ(s.size(), text.size());
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteSmallerSize) {
  std::string s;
  const std::string full = "abcdefghij";
  ql::resize_and_overwrite(s, full.size(), [&](char* buf, size_t count) {
    EXPECT_EQ(count, full.size());
    std::memcpy(buf, full.data(), full.size());
    return 4u;  // Only keep first 4 characters
  });
  EXPECT_EQ(s, "abcd");
  EXPECT_EQ(s.size(), 4u);
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteZeroSize) {
  std::string s = "not empty";
  ql::resize_and_overwrite(s, 20, [](char*, size_t) {
    return 0u;
  });
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.size(), 0u);
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteZeroCapacity) {
  std::string s;
  ql::resize_and_overwrite(s, 0, [](char*, size_t count) {
    EXPECT_EQ(count, 0u);
    return 0u;
  });
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.size(), 0u);
}

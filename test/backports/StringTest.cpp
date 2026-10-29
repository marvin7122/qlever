// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "backports/string.h"
#include "util/Exception.h"

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
  // Longer than the small string buffer, so that `data()` is stable only if
  // the heap buffer is reused.
  std::string s(100, 'x');
  const char* dataBefore = s.data();
  const std::string full = "abcdefghij";
  ql::resize_and_overwrite(s, full.size(), [&](char* buf, size_t count) {
    EXPECT_EQ(count, full.size());
    std::memcpy(buf, full.data(), full.size());
    return 4;
  });
  EXPECT_EQ(s, "abcd");
  EXPECT_EQ(s.size(), 4u);
  EXPECT_EQ(s.data(), dataBefore);
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteZeroSize) {
  std::string s = "not empty";
  size_t requestedCount = 0;
  ql::resize_and_overwrite(s, 20, [&requestedCount](char*, size_t count) {
    requestedCount = count;
    return 0u;
  });
  EXPECT_EQ(requestedCount, 20u);
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.size(), 0u);
}

// _____________________________________________________________________________
TEST(StringTest, ResizeAndOverwriteZeroCapacity) {
  std::string s;
  bool wasCalled = false;
  ql::resize_and_overwrite(s, 0, [&wasCalled](char*, size_t count) {
    wasCalled = true;
    EXPECT_EQ(count, 0u);
    return 0u;
  });
  EXPECT_TRUE(wasCalled);
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.size(), 0u);
}

// _____________________________________________________________________________
// Oversize results violate the contract on both branches.
TEST(StringTest, ResizeAndOverwriteOversizedResultThrows) {
  std::string s;
  try {
    ql::resize_and_overwrite(s, 4, [](char*, size_t) { return 5u; });
    FAIL() << "An oversized result must throw.";
  } catch (const ad_utility::Exception& e) {
    EXPECT_THAT(e.what(), ::testing::HasSubstr("newSize <= count"));
  }
}

// _____________________________________________________________________________
// Move-only rvalue operations work via the moved-into lambda.
TEST(StringTest, ResizeAndOverwriteMoveOnlyOperation) {
  std::string s;
  const std::string text = "move-only";
  auto op = [payload = std::make_unique<std::string>(text)](char* buf,
                                                            size_t count) {
    EXPECT_EQ(count, payload->size());
    std::memcpy(buf, payload->data(), payload->size());
    return payload->size();
  };
  ql::resize_and_overwrite(s, text.size(), std::move(op));
  EXPECT_EQ(s, text);
  EXPECT_EQ(s.size(), text.size());
}

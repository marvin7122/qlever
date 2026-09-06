// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "util/FastIntToString.h"

namespace {
using namespace ad_utility;

// _____________________________________________________________________________
TEST(FastIntToStringTest, NumDigitsUInt64) {
  EXPECT_EQ(numDigits(0ULL), 1U);
  EXPECT_EQ(numDigits(1ULL), 1U);
  EXPECT_EQ(numDigits(9ULL), 1U);
  EXPECT_EQ(numDigits(10ULL), 2U);
  EXPECT_EQ(numDigits(99ULL), 2U);
  EXPECT_EQ(numDigits(100ULL), 3U);
  EXPECT_EQ(numDigits(999ULL), 3U);
  EXPECT_EQ(numDigits(1000ULL), 4U);
  EXPECT_EQ(numDigits(9999ULL), 4U);
  EXPECT_EQ(numDigits(10000ULL), 5U);
  EXPECT_EQ(numDigits(99999ULL), 5U);
  EXPECT_EQ(numDigits(100000ULL), 6U);
  EXPECT_EQ(numDigits(999999ULL), 6U);
  EXPECT_EQ(numDigits(1000000ULL), 7U);
  EXPECT_EQ(numDigits(9999999ULL), 7U);
  EXPECT_EQ(numDigits(10000000ULL), 8U);
  EXPECT_EQ(numDigits(99999999ULL), 8U);
  EXPECT_EQ(numDigits(100000000ULL), 9U);
  EXPECT_EQ(numDigits(999999999ULL), 9U);
  EXPECT_EQ(numDigits(1000000000ULL), 10U);
  EXPECT_EQ(numDigits(9999999999ULL), 10U);
  EXPECT_EQ(numDigits(10000000000ULL), 11U);
  EXPECT_EQ(numDigits(99999999999ULL), 11U);
  EXPECT_EQ(numDigits(100000000000ULL), 12U);
  EXPECT_EQ(numDigits(999999999999ULL), 12U);
  EXPECT_EQ(numDigits(1000000000000ULL), 13U);
  EXPECT_EQ(numDigits(9999999999999ULL), 13U);
  EXPECT_EQ(numDigits(10000000000000ULL), 14U);
  EXPECT_EQ(numDigits(99999999999999ULL), 14U);
  EXPECT_EQ(numDigits(100000000000000ULL), 15U);
  EXPECT_EQ(numDigits(999999999999999ULL), 15U);
  EXPECT_EQ(numDigits(1000000000000000ULL), 16U);
  EXPECT_EQ(numDigits(9999999999999999ULL), 16U);
  EXPECT_EQ(numDigits(10000000000000000ULL), 17U);
  EXPECT_EQ(numDigits(99999999999999999ULL), 17U);
  EXPECT_EQ(numDigits(100000000000000000ULL), 18U);
  EXPECT_EQ(numDigits(999999999999999999ULL), 18U);
  EXPECT_EQ(numDigits(1000000000000000000ULL), 19U);
  EXPECT_EQ(numDigits(9999999999999999999ULL), 19U);
  EXPECT_EQ(numDigits(10000000000000000000ULL), 20U);
  EXPECT_EQ(numDigits(std::numeric_limits<uint64_t>::max()), 20U);
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, NumDigitsUInt32) {
  EXPECT_EQ(numDigits(0U), 1U);
  EXPECT_EQ(numDigits(1U), 1U);
  EXPECT_EQ(numDigits(9U), 1U);
  EXPECT_EQ(numDigits(10U), 2U);
  EXPECT_EQ(numDigits(99U), 2U);
  EXPECT_EQ(numDigits(100U), 3U);
  EXPECT_EQ(numDigits(999U), 3U);
  EXPECT_EQ(numDigits(1000U), 4U);
  EXPECT_EQ(numDigits(9999U), 4U);
  EXPECT_EQ(numDigits(10000U), 5U);
  EXPECT_EQ(numDigits(99999U), 5U);
  EXPECT_EQ(numDigits(100000U), 6U);
  EXPECT_EQ(numDigits(999999U), 6U);
  EXPECT_EQ(numDigits(1000000U), 7U);
  EXPECT_EQ(numDigits(9999999U), 7U);
  EXPECT_EQ(numDigits(10000000U), 8U);
  EXPECT_EQ(numDigits(99999999U), 8U);
  EXPECT_EQ(numDigits(100000000U), 9U);
  EXPECT_EQ(numDigits(999999999U), 9U);
  EXPECT_EQ(numDigits(1000000000U), 10U);
  EXPECT_EQ(numDigits(std::numeric_limits<uint32_t>::max()), 10U);
}

namespace {
template <typename T, typename Gen>
void runBranchlessFormatTest(const char* label,
                             std::string_view (*formatFn)(T, char*),
                             std::vector<T> boundaryValues,
                             size_t iterations,
                             Gen& gen) {
  char buf[64];
  auto testValue = [&](T val) {
    std::string_view actual = formatFn(val, buf);
    EXPECT_EQ(actual, std::to_string(val)) << "Failed on " << label << " value: "
                                            << val;
  };

  for (const T& val : boundaryValues) {
    testValue(val);
  }

  for (size_t i = 0; i < iterations; ++i) {
    testValue(static_cast<T>(gen()));
  }
}

std::string_view formatUIntBranchlessView(uint64_t val, char* buf) {
  return std::string_view(buf, formatUIntBranchless(val, buf) - buf);
}

std::string_view formatUInt32BranchlessView(uint32_t val, char* buf) {
  return std::string_view(buf, formatUInt32Branchless(val, buf) - buf);
}

std::string_view formatIntBranchlessView(int64_t val, char* buf) {
  return std::string_view(buf, formatIntBranchless(val, buf) - buf);
}

std::string_view formatInt32BranchlessView(int32_t val, char* buf) {
  return std::string_view(buf, formatInt32Branchless(val, buf) - buf);
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatUIntBranchless) {
  std::mt19937_64 gen(42);
  runBranchlessFormatTest<uint64_t>(
      "uint64", formatUIntBranchlessView,
      {0ULL,          1ULL,         9ULL,         10ULL,
       42ULL,         99ULL,        100ULL,       999ULL,
       1000ULL,       9999ULL,      10000ULL,     99999ULL,
       100000ULL,     999999ULL,    1000000ULL,   9999999ULL,
       10000000ULL,   99999999ULL,  100000000ULL, 999999999ULL,
       1000000000ULL, 123456789ULL, 9999999999ULL, 10000000000ULL,
       123456789012345678ULL, 10000000000000000000ULL,
       std::numeric_limits<uint64_t>::max()},
      50000, gen);
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatUInt32Branchless) {
  std::mt19937 gen(1337);
  runBranchlessFormatTest<uint32_t>(
      "uint32", formatUInt32BranchlessView,
      {0U, 1U, 42U, 99U, 100U, 12345U, 123456789U,
       std::numeric_limits<uint32_t>::max()},
      10000, gen);
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatIntBranchless) {
  std::mt19937_64 gen(999);
  runBranchlessFormatTest<int64_t>(
      "int64", formatIntBranchlessView,
      {0, 1, -1, 42, -42, 100, -100, 123456789, -123456789,
       std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::min()},
      50000, gen);
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatInt32Branchless) {
  std::mt19937 gen(2026);
  runBranchlessFormatTest<int32_t>(
      "int32", formatInt32BranchlessView,
      {0, 1, -1, 42, -42, 100, -100,
       std::numeric_limits<int32_t>::max(), std::numeric_limits<int32_t>::min()},
      10000, gen);
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatQidAndPid) {
  char buf[128];

  // QID tests
  {
    char* end = formatQid(42ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf), "http://www.wikidata.org/entity/Q42");
  }
  {
    char* end = formatQid(1234567ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf), "http://www.wikidata.org/entity/Q1234567");
  }
  {
    char* end = formatQid(115858349ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf), "http://www.wikidata.org/entity/Q115858349");
  }

  // PID tests
  {
    char* end = formatPid(31ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf), "http://www.wikidata.org/prop/direct/P31");
  }

  // Custom prefix tests
  {
    char* end = formatPrefixedId("http://example.org/item/", 98765ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf), "http://example.org/item/98765");
  }
  {
    char* end = formatPrefixedInt("http://example.org/temp/-", 555LL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf), "http://example.org/temp/-555");
  }
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatDetailFixedDigits) {
  char buf[16];

  // 8 digits formatting
  detail::format8Digits(12345678U, buf);
  EXPECT_EQ(std::string_view(buf, 8), "12345678");

  detail::format8Digits(42U, buf);
  EXPECT_EQ(std::string_view(buf, 8), "00000042");

  detail::format8Digits(0U, buf);
  EXPECT_EQ(std::string_view(buf, 8), "00000000");

  detail::format8Digits(99999999U, buf);
  EXPECT_EQ(std::string_view(buf, 8), "99999999");

  // 4 digits formatting
  detail::format4Digits(1234U, buf);
  EXPECT_EQ(std::string_view(buf, 4), "1234");

  detail::format4Digits(7U, buf);
  EXPECT_EQ(std::string_view(buf, 4), "0007");

  // 2 digits formatting
  detail::format2Digits(42U, buf);
  EXPECT_EQ(std::string_view(buf, 2), "42");

  detail::format2Digits(5U, buf);
  EXPECT_EQ(std::string_view(buf, 2), "05");
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, StringWrappers) {
  EXPECT_EQ(formatUIntToString(42ULL), "42");
  EXPECT_EQ(formatUIntToString(1234567890ULL), "1234567890");
  EXPECT_EQ(formatIntToString(-987654321LL), "-987654321");
  EXPECT_EQ(formatIntToString(0LL), "0");
  EXPECT_EQ(formatQidToString(42ULL), "http://www.wikidata.org/entity/Q42");
  EXPECT_EQ(formatQidToString(0ULL), "http://www.wikidata.org/entity/Q0");
  EXPECT_EQ(formatQidToString(9999999999999999999ULL),
            "http://www.wikidata.org/entity/Q9999999999999999999");
}

}  // namespace

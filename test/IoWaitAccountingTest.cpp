// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <string>

#include "util/IoWaitAccounting.h"

namespace ad_utility::ioWait {
// A disabled wrapper calls through without touching the counters.
TEST(IoWaitAccountingTest, disabledTimedCallsThroughWithoutCounting) {
  setEnabled(false);
  const ThreadCounters before = total();
  const int value = timed(preadCounters, []() { return 42; });
  EXPECT_EQ(value, 42);
  const ThreadCounters after = total();
  EXPECT_EQ(after.pread_.calls_, before.pread_.calls_);
  EXPECT_EQ(after.pread_.nanos_, before.pread_.nanos_);
}

// An enabled wrapper counts each call, including `void` callables.
TEST(IoWaitAccountingTest, enabledTimedCountsCalls) {
  setEnabled(true);
  const ThreadCounters before = total();
  timed(preadCounters, []() {});
  timed(ioUringWaitCounters, []() { return 0; });
  timed(ioUringSubmitCounters, []() { return 1; });
  const ThreadCounters after = total();
  EXPECT_EQ(after.pread_.calls_, before.pread_.calls_ + 1);
  EXPECT_EQ(after.ioUringWait_.calls_, before.ioUringWait_.calls_ + 1);
  EXPECT_EQ(after.ioUringSubmit_.calls_, before.ioUringSubmit_.calls_ + 1);
  EXPECT_GE(after.pread_.nanos_, before.pread_.nanos_);
  setEnabled(false);
}

// The report renders the current totals with all three call sites.
TEST(IoWaitAccountingTest, reportContainsAllCallSites) {
  setEnabled(true);
  timed(preadCounters, []() {});
  const std::string text = report();
  EXPECT_NE(text.find("io-wait-accounting"), std::string::npos);
  EXPECT_NE(text.find("enabled=1"), std::string::npos);
  EXPECT_NE(text.find("pread_calls="), std::string::npos);
  EXPECT_NE(text.find("iouring_waits="), std::string::npos);
  EXPECT_NE(text.find("iouring_submits="), std::string::npos);
  setEnabled(false);
}
}  // namespace ad_utility::ioWait

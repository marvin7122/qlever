// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <string>
#include <thread>

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

// The totals of a thread that has exited are kept.
TEST(IoWaitAccountingTest, exitedThreadTotalsAreKept) {
  setEnabled(true);
  const ThreadCounters before = total();
  std::thread{[]() {
    timed(ioUringWaitCounters, []() {});
    timed(ioUringWaitCounters, []() {});
  }}.join();
  const ThreadCounters after = total();
  EXPECT_EQ(after.ioUringWait_.calls_, before.ioUringWait_.calls_ + 2);
  setEnabled(false);
}

// `addTo` adds every call site's totals.
TEST(IoWaitAccountingTest, addToSumsAllCallSites) {
  ThreadCounters sum{{1, 2}, {3, 4}, {5, 6}};
  addTo(sum, ThreadCounters{{10, 20}, {30, 40}, {50, 60}});
  EXPECT_EQ(sum.pread_.nanos_, 11u);
  EXPECT_EQ(sum.pread_.calls_, 22u);
  EXPECT_EQ(sum.ioUringWait_.nanos_, 33u);
  EXPECT_EQ(sum.ioUringWait_.calls_, 44u);
  EXPECT_EQ(sum.ioUringSubmit_.nanos_, 55u);
  EXPECT_EQ(sum.ioUringSubmit_.calls_, 66u);
}

// `ticksFromStat` sums utime (field 14) and stime (field 15), also when the
// comm field contains spaces and parentheses, and yields 0 for malformed lines.
TEST(IoWaitAccountingTest, ticksFromStat) {
  using detail::ticksFromStat;
  EXPECT_EQ(ticksFromStat("42 (iou-wrk-7) S 1 2 3 4 5 6 7 8 9 10 70 30 0 0"),
            100u);
  EXPECT_EQ(ticksFromStat("42 (a (b) c) R 1 2 3 4 5 6 7 8 9 10 1 2 3 4"), 3u);
  // Truncated before stime.
  EXPECT_EQ(ticksFromStat("42 (x) S 1 2 3 4 5 6 7 8 9 10 70"), 0u);
  // No comm field at all.
  EXPECT_EQ(ticksFromStat("garbage"), 0u);
  EXPECT_EQ(ticksFromStat(""), 0u);
}
}  // namespace ad_utility::ioWait

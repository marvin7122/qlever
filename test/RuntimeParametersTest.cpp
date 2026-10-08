// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>

#include "global/RuntimeParameters.h"
#include "util/GTestHelpers.h"

using ::testing::AllOf;
using ::testing::HasSubstr;

// Test setting a runtime parameter from a single `<name>=<value>` string, as
// used by the `--set-runtime-parameter` option of `qlever-server`.
TEST(RuntimeParameters, setFromAssignment) {
  RuntimeParameters params;

  // A valid assignment sets the parameter.
  params.setFromAssignment("default-query-timeout=300s");
  EXPECT_EQ(params.defaultQueryTimeout_.get(), std::chrono::seconds{300});

  // The string is split at the FIRST `=`, so values containing `=` work.
  params.setFromAssignment("default-query-timeout=150s");
  EXPECT_EQ(params.defaultQueryTimeout_.get(), std::chrono::seconds{150});

  // A missing `=` is rejected with a readable message.
  AD_EXPECT_THROW_WITH_MESSAGE(
      params.setFromAssignment("no-equals-sign"),
      HasSubstr("assignment of the form <name>=<value>"));

  // An unknown parameter name is rejected, and the message lists the
  // available parameters.
  AD_EXPECT_THROW_WITH_MESSAGE(
      params.setFromAssignment("no-such-parameter=42"),
      AllOf(HasSubstr("No parameter with name no-such-parameter"),
            HasSubstr("Available parameters are:"),
            HasSubstr("default-query-timeout")));

  // An invalid value for an existing parameter is rejected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      params.setFromAssignment("default-query-timeout=banana"),
      HasSubstr("Could not set parameter default-query-timeout"));

  // The use case that prompted this option (see #3031): set the maximal
  // number of redirects for query federation at startup.
  params.setFromAssignment("service-max-redirects=5");
  EXPECT_EQ(params.serviceMaxRedirects_.get(), 5u);
}

// Test that the value `0` is rejected for `lazy-index-scan-num-threads`
// (the implementation of the lazy scans requires at least one thread).
TEST(RuntimeParameters, lazyIndexScanNumThreadsIsStrictlyPositive) {
  RuntimeParameters params;
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      params.setFromAssignment("lazy-index-scan-num-threads=0"),
      AllOf(HasSubstr("lazy-index-scan-num-threads"),
            HasSubstr("strictly positive")),
      std::runtime_error);
  EXPECT_NO_THROW(params.setFromAssignment("lazy-index-scan-num-threads=1"));
  EXPECT_EQ(params.lazyIndexScanNumThreads_.get(), 1u);
}

TEST(RuntimeParameters, selectExportTermCacheMinHitRateIsInUnitInterval) {
  RuntimeParameters params;
  auto& parameter = params.selectExportTermCacheMinHitRate_;
  EXPECT_DOUBLE_EQ(parameter.get(), 0.25);

  for (double value : {0.0, 0.5, 1.0}) {
    EXPECT_NO_THROW(parameter.set(value));
    EXPECT_DOUBLE_EQ(parameter.get(), value);
    EXPECT_NO_THROW(
        params.setFromString(parameter.name(), std::to_string(value)));
    EXPECT_DOUBLE_EQ(parameter.get(), value);
  }

  for (double value : {-0.1, 1.1, std::numeric_limits<double>::infinity(),
                       -std::numeric_limits<double>::infinity(),
                       std::numeric_limits<double>::quiet_NaN()}) {
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        parameter.set(value),
        AllOf(HasSubstr(parameter.name()), HasSubstr("[0, 1]")),
        std::runtime_error);
    EXPECT_DOUBLE_EQ(parameter.get(), 1.0);
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        params.setFromString(parameter.name(), std::to_string(value)),
        AllOf(HasSubstr(parameter.name()), HasSubstr("[0, 1]")),
        std::runtime_error);
    EXPECT_DOUBLE_EQ(parameter.get(), 1.0);
  }
}

// Test that `getKeys` and `toMap` (the building blocks of
// `--set-runtime-parameter help`) are consistent with each other.
TEST(RuntimeParameters, getKeysAndToMapAreConsistent) {
  RuntimeParameters params;
  auto keys = params.getKeys();
  auto map = params.toMap();
  EXPECT_FALSE(keys.empty());
  EXPECT_EQ(keys.size(), map.size());
  for (const auto& key : keys) {
    EXPECT_TRUE(map.contains(key)) << key;
  }
}

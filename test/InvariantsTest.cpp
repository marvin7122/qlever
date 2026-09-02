// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <stdexcept>
#include <type_traits>
#include <utility>

#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/Invariants.h"

namespace {

// _____________________________________________________________________________
// Define a mock stateful class that counts invocations of `checkInvariants()`.
class MockInvariantClass
    : public ad_utility::WithInvariants<MockInvariantClass> {
 public:
  // Counts invariant-check invocations; mutable for use in checkInvariants() const.
  mutable size_t checkCount_{0};
  // Test control state used to simulate violated entry or exit invariants.
  bool failInvariants_{false};

  void checkInvariants() const {
    ++checkCount_;
    AD_CORRECTNESS_CHECK(!failInvariants_);
  }

  void doMutatingOperation() {
    auto guard = makeInvariantGuard();
    // Enter and leave the operation without changing the object state.
  }

  void doBrokenOperation() {
    auto guard = makeInvariantGuard();
    failInvariants_ = true;
  }
};

// _____________________________________________________________________________
// Define a class that lacks `checkInvariants() const` to verify concept failure.
struct ClassWithoutInvariants {};

// Check the invariant-stateful-class concept for supported and unsupported types.
static_assert(ad_utility::InvariantStatefulClass<MockInvariantClass>);
static_assert(!ad_utility::InvariantStatefulClass<ClassWithoutInvariants>);

// _____________________________________________________________________________
TEST(InvariantsTest, InvariantGuardChecksOnEntryAndExit) {
  MockInvariantClass instance;
  EXPECT_EQ(instance.checkCount_, 0u);

  {
    ad_utility::InvariantGuard guard{&instance};

    EXPECT_EQ(instance.checkCount_, 1u);
  }

  EXPECT_EQ(instance.checkCount_, 2u);
}

// _____________________________________________________________________________
TEST(InvariantsTest, InvariantGuardRejectsViolatedEntryInvariant) {
  MockInvariantClass instance;
  instance.failInvariants_ = true;

  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)ad_utility::InvariantGuard<MockInvariantClass>{&instance},
      ::testing::HasSubstr("!failInvariants_"));
}

// _____________________________________________________________________________
TEST(InvariantsTest, WithInvariantsMixinWorksOnMutatingMethods) {
  MockInvariantClass instance;
  EXPECT_EQ(instance.checkCount_, 0u);

  instance.doMutatingOperation();
  EXPECT_EQ(instance.checkCount_, 2u);
}

// _____________________________________________________________________________
TEST(InvariantsTest, ViolatedInvariantOnExitThrows) {
  MockInvariantClass instance;
  AD_EXPECT_THROW_WITH_MESSAGE(instance.doBrokenOperation(),
                               ::testing::HasSubstr("!failInvariants_"));
}

// _____________________________________________________________________________
TEST(InvariantsTest, GuardRejectsNullInstanceOnConstruction) {
  MockInvariantClass* nullInstance = nullptr;
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)ad_utility::InvariantGuard<MockInvariantClass>{nullInstance},
      ::testing::HasSubstr("self_ != nullptr"));
}

TEST(InvariantsTest, InvariantGuardSkipsExitCheckDuringExceptionUnwinding) {
  MockInvariantClass instance;

  EXPECT_THROW(
      [&] {
        ad_utility::InvariantGuard guard{&instance};
        throw std::runtime_error{"failure"};
      }(),
      std::runtime_error);

  EXPECT_EQ(instance.checkCount_, 1u);
}

TEST(InvariantsTest, ContractsPreConditionEnforcement) {
  auto compute = [](int x) {
    QL_PRE(x > 0);
    return x * 2;
  };

  EXPECT_EQ(compute(5), 10);
  AD_EXPECT_THROW_WITH_MESSAGE(compute(0), ::testing::HasSubstr("x > 0"));
}

TEST(InvariantsTest, ContractsAssertEnforcement) {
  auto checkCheckpoint = [](int x) {
    QL_CONTRACT_ASSERT(x != 42);
    return x;
  };

  EXPECT_EQ(checkCheckpoint(10), 10);
  AD_EXPECT_THROW_WITH_MESSAGE(checkCheckpoint(42),
                               ::testing::HasSubstr("x != 42"));
}

TEST(InvariantsTest, ContractsPostConditionNormalExit) {
  auto succeed = [](int val) {
    int result = 0;
    QL_POST(result > 0);
    result = val;
    return result;
  };

  EXPECT_EQ(succeed(5), 5);
  AD_EXPECT_THROW_WITH_MESSAGE(succeed(-1), ::testing::HasSubstr("result > 0"));
}

TEST(InvariantsTest, ContractsPostConditionSkipsDuringExceptionUnwinding) {
  EXPECT_THROW(
      [&] {
        int result = -1;
        QL_POST(result > 0);  // Would fail if checked on exit!
        throw std::runtime_error{"operation failed"};
      }(),
      std::runtime_error);
}

template <typename T, typename = void>
struct HasRvalueInvariantGuard : std::false_type {};

template <typename T>
struct HasRvalueInvariantGuard<
    T, std::void_t<decltype(std::declval<T&&>().makeInvariantGuard())>>
    : std::true_type {};

static_assert(!HasRvalueInvariantGuard<MockInvariantClass>::value);

}  // namespace

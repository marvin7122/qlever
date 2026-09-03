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

class MockInvariantClass
    : public ad_utility::WithInvariants<MockInvariantClass> {
 public:
    // Count calls to `checkInvariants() const`; keep this member mutable.
  mutable size_t checkCount_{0};
    // Use `failInvariants_` to simulate violated entry or exit invariants.
  bool failInvariants_{false};

  void checkInvariants() const {
    ++checkCount_;
    AD_CORRECTNESS_CHECK(!failInvariants_);
  }

  void doMutatingOperation() {
    auto guard = makeInvariantGuard();
        // Enter and leave the operation without changing failInvariants_.
  }

  void doBrokenOperation() {
    auto guard = makeInvariantGuard();
    failInvariants_ = true;
  }
};

// _____________________________________________________________________________

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

template <typename T, typename = void>
struct HasRvalueInvariantGuard : std::false_type {};

template <typename T>
struct HasRvalueInvariantGuard<
    T, std::void_t<decltype(std::declval<T&&>().makeInvariantGuard())>>
    : std::true_type {};

static_assert(!HasRvalueInvariantGuard<MockInvariantClass>::value);

}  // namespace

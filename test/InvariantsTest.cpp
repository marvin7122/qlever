// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/Invariants.h"
#include <gmock/gmock.h>

namespace {

// A mock stateful class that counts invocations of `checkInvariants()`.
class MockInvariantClass
    : public ad_utility::WithInvariants<MockInvariantClass> {
 public:
  mutable size_t checkCount_{0};
  bool failInvariants_{false};

  void checkInvariants() const {
    ++checkCount_;
    AD_CORRECTNESS_CHECK(!failInvariants_);
  }

  void doMutatingOperation() {
    auto guard = makeInvariantGuard();
    // Simulate mutating work
  }

  void doBrokenOperation() {
    auto guard = makeInvariantGuard();
    failInvariants_ = true;
  }
};

// A class that lacks `checkInvariants() const` to verify concept failure.
struct ClassWithoutInvariants {};

// Formal compile-time concept verification.
static_assert(ad_utility::InvariantStatefulClass<MockInvariantClass>);
static_assert(!ad_utility::InvariantStatefulClass<ClassWithoutInvariants>);

// _____________________________________________________________________________
TEST(InvariantsTest, InvariantGuardChecksOnEntryAndExit) {
  MockInvariantClass instance;
  EXPECT_EQ(instance.checkCount_, 0u);

  {
    ad_utility::InvariantGuard guard{&instance};
    // Checked once on entry
    EXPECT_EQ(instance.checkCount_, 1u);
  }
  // Checked again on scope exit (destructor)
  EXPECT_EQ(instance.checkCount_, 2u);
}

// _____________________________________________________________________________
TEST(InvariantsTest, WithInvariantsMixinWorksOnMutatingMethods) {
  MockInvariantClass instance;
  EXPECT_EQ(instance.checkCount_, 0u);

  instance.doMutatingOperation();
  // Checked once on entry and once on exit
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

}  // namespace

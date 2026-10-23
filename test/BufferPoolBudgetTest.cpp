// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <vector>

#include "util/BufferPoolBudget.h"
#include "util/GTestHelpers.h"

namespace {
using ad_utility::BufferPoolBudget;
using ad_utility::MemorySignals;

constexpr size_t MiB = size_t{1} << 20;
constexpr size_t GiB = size_t{1} << 30;
constexpr size_t kReserve = 4 * GiB;

// Plenty of free memory and no pressure: the budget may grow.
constexpr MemorySignals kRelaxed{64 * GiB, 0.0};
}  // namespace

// _____________________________________________________________________________
TEST(BufferPoolBudget, GrowToCeiling) {
  const size_t ceiling = 3 * GiB + 512 * MiB;
  BufferPoolBudget budget{kReserve, {ceiling, 10 * GiB}, 0};
  EXPECT_EQ(budget.target(), BufferPoolBudget::kFloor);
  EXPECT_EQ(budget.ceiling(), ceiling);

  // Three full steps of 1 GiB fit below the ceiling.
  for (size_t i = 1; i <= 3; ++i) {
    EXPECT_EQ(budget.tick(kRelaxed), 256 * MiB + i * GiB);
    EXPECT_EQ(budget.target(), 256 * MiB + i * GiB);
  }
  // The fourth step is cut off at the ceiling, and the target stays there.
  EXPECT_EQ(budget.tick(kRelaxed), ceiling);
  EXPECT_EQ(budget.tick(kRelaxed), ceiling);
  EXPECT_EQ(budget.target(), ceiling);
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, ShrinkOnLowMemory) {
  BufferPoolBudget budget{kReserve, {100 * GiB}, 8 * GiB};
  // A deficit of 2 GiB is larger than 10 % of the target, so the whole deficit
  // is cut.
  EXPECT_EQ(budget.tick({kReserve - 2 * GiB, 0.0}), 6 * GiB);

  // A deficit of 100 MiB is smaller than 10 % of the target, so the target
  // shrinks by 10 %.
  BufferPoolBudget budget2{kReserve, {100 * GiB}, 10 * GiB};
  EXPECT_EQ(budget2.tick({kReserve - 100 * MiB, 0.0}), 9 * GiB);

  // No free memory at all: the deficit is the whole reserve of 4 GiB.
  EXPECT_EQ(budget2.tick({0, 0.0}), 5 * GiB);

  // A deficit larger than the whole target drops to the floor.
  BufferPoolBudget budget3{16 * GiB, {100 * GiB}, 2 * GiB};
  EXPECT_EQ(budget3.tick({0, 0.0}), BufferPoolBudget::kFloor);
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, ShrinkOnPressure) {
  BufferPoolBudget budget{kReserve, {100 * GiB}, 10 * GiB};
  // High pressure shrinks by 10 % even with plenty of free memory.
  EXPECT_EQ(budget.tick({64 * GiB, 0.2}), 9 * GiB);

  // Mild pressure holds the target, it neither shrinks nor grows.
  EXPECT_EQ(budget.tick({64 * GiB, 0.05}), 9 * GiB);
  // Exactly the threshold also holds.
  EXPECT_EQ(budget.tick({64 * GiB, BufferPoolBudget::kPressureThreshold}),
            9 * GiB);
  // Free memory exactly at the reserve and no pressure holds.
  EXPECT_EQ(budget.tick({kReserve, 0.0}), 9 * GiB);

  // Pressure outside [0, 1] is a contract violation and leaves the target
  // unchanged.
  AD_EXPECT_THROW_WITH_MESSAGE(budget.tick({64 * GiB, -0.1}),
                               ::testing::HasSubstr("[0, 1]"));
  AD_EXPECT_THROW_WITH_MESSAGE(budget.tick({64 * GiB, 1.5}),
                               ::testing::HasSubstr("[0, 1]"));
  EXPECT_EQ(budget.target(), 9 * GiB);
  // The boundaries themselves are valid.
  EXPECT_NO_THROW(budget.tick({64 * GiB, 0.0}));
  EXPECT_NO_THROW(budget.tick({64 * GiB, 1.0}));
}

// _____________________________________________________________________________
TEST(BufferPoolBudget, Bounds) {
  // The ceiling is the smallest file size.
  BufferPoolBudget budget{kReserve, {4 * GiB, 2 * GiB, 8 * GiB}, 1 * GiB};
  EXPECT_EQ(budget.floor(), 256 * MiB);
  EXPECT_EQ(budget.ceiling(), 2 * GiB);

  // Repeated severe shrinking stops at the floor.
  for (size_t i = 0; i < 10; ++i) {
    budget.tick({0, 1.0});
    EXPECT_GE(budget.target(), 256 * MiB);
  }
  EXPECT_EQ(budget.target(), 256 * MiB);
  // Shrinking at the floor by 10 % also stays at the floor.
  EXPECT_EQ(budget.tick({64 * GiB, 0.5}), 256 * MiB);

  // Repeated growing stops at the ceiling.
  for (size_t i = 0; i < 10; ++i) {
    budget.tick(kRelaxed);
    EXPECT_LE(budget.target(), 2 * GiB);
  }
  EXPECT_EQ(budget.target(), 2 * GiB);

  // A file smaller than the floor raises the ceiling to the floor, so the
  // target is pinned there.
  BufferPoolBudget tiny{kReserve, {1 * MiB, 8 * GiB}, 1 * GiB};
  EXPECT_EQ(tiny.ceiling(), tiny.floor());
  EXPECT_EQ(tiny.target(), tiny.floor());
  EXPECT_EQ(tiny.tick(kRelaxed), tiny.floor());
  EXPECT_EQ(tiny.tick({0, 1.0}), tiny.floor());

  // Initial targets outside the bounds are clamped.
  EXPECT_EQ((BufferPoolBudget{kReserve, {2 * GiB}, 0}.target()), 256 * MiB);
  EXPECT_EQ((BufferPoolBudget{kReserve, {2 * GiB}, 100 * GiB}.target()),
            2 * GiB);

  // At least one file size is required.
  EXPECT_ANY_THROW((BufferPoolBudget{kReserve, {}, 1 * GiB}));
}

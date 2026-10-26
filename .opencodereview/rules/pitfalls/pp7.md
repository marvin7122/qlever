---
kind: rule
severity: SHOULD
language: cpp
tags: [qlever, pitfall]
last_verified: 2026-09-22
---
# PP7: Weak test assertions and repetitive test setup

## Statement
Tests use gmock matchers (`EXPECT_THAT`, `ElementsAre`, custom matchers via `AD_FIELD`) rather than element-wise `EXPECT_EQ`; repeated setup code is factored into helpers/parameterized suites.

## Sources
- review: `.ai-review/personal_pitfalls.md#PP7` Mined from maintainer review comments, file dated 2026-09-22.
- review: `QLever PR #105` "The two tests repeat the same compression/decompression setup and validate results with element-wise `EXPECT_EQ` loops; prefer a shared helper or parameterized test."
- review: `QLever PR #107` "No direct test exercises this new public function; add a boundary and collision case."
- review: `QLever PR #77` "The short-word lifetime test's stack clobber is only a practical tripwire outside ASan; document the limitation."
- review: `QLever PR #78` "Duplicate assertion: the same `ASSERT_FALSE(comp(\"vivæ\", \"vivae\", ...))` appears twice consecutively."
- review: `QLever PR #70` "The test `ResizeAndOverwriteSmallerSize` assumes that shrinking a `std::string` preserves the original data pointer (`s.data() == dataBefore`). While `std::string` implementations often reuse the buffer, this is not guaranteed by the C++ standard for `resize()` (which the fallback implementation uses). This test could potentially fail or be brittle. I recommend removing the assertion on pointer stability."
- review: `QLever PR #120` "Test 10 relies on fixed 2–40ms sleep timings that may not invert completion order on loaded CI runners, weakening the unordered-emission assertion."
- review: `QLever PR #120` "The same fixed sleep timings may not invert completion order under realistic scheduler load."
- review: `QLever PR #2710` "Please use gmock machers, here ... then the tests will be much more r[eadable]"
- review: `QLever PR #116` "The test uses a mutable `checkCount_` member variable that's modified in `checkInvariants()`, which is accessed concurrently if multiple threads call `checkInvariants()` on the same instance."
- review: `QLever PR #91` "The AutoFlushOnChunkByteLimit test uses a weak assertion (`EXPECT_GT(chunksEmitted_, 1)`) instead of verifying the exact expected chunk count."
- review: `QLever PR #118` "The test relies on real time sleeps ... This can make the test flaky on slower or heavily loaded CI machines."
- review: `QLever PR #118` "Similar use of `std::this_thread::sleep_for` appears in multiple tests ... Real sleeps can slow down the test suite and introduce nondeterminism."
- review: `QLever PR #140` "This is a busy-wait loop that spins until 'release' is set, causing high CPU consumption during the test."
- review: `QLever PR #140` "The call to `pool.wait()` has no timeout and could block indefinitely ... Consider using a timed wait."
- review: `QLever PR #138` "The test no longer verifies the exception message ... providing better regression detection."
- review: `QLever PR #78` "The test fixture ... uses a static `TwoSplitVocabulary sv_` member shared across all tests ... leading to flaky or order-dependent failures."
- review: `QLever PR #78` "Same issue: manual cleanup at end of test body without RAII guard. If assertions fail before cleanup, temp files leak."
- review: `QLever PR #2837` "A lot of duplication here, this can be a parameterized test suite"
- review: `QLever PR #3007` "My most concern is really ugly repetition of setup code in the `IoUringManagerTest`."
- review: `QLever PR #112` "Missing edge case tests: Empty sketch, single element, large cardinality, merge with empty/identical sketches, and different Precision values."
- review: `QLever PR #134` "The new rejection logic in enqueueMorsel() ... lacks test coverage."
- review: `QLever PR #140` "Potential race condition in test synchronization ... account for pending morsels."
- review: `QLever PR #140` "The test uses `std::memory_order_relaxed` for postedCount increments ... the main thread might see stale values."
- review: `QLever PR #140` "The test uses boost::asio::static_thread_pool on the stack without calling stop() before destruction."
- review: `QLever PR #87` "The test validates prefetchDistance values 1, 8, 64 as valid and 0, 129 as invalid ... 128 should be explicitly tested as valid."
- review: `QLever PR #87` "The test passes an empty positions span but a results vector of size 1 with ids of size 1."
- review: `.opencodereview/rule.json#146` QLever OCR fan-out project rule, file dated 2026-09-23.

## Notes
Frequency in the mined comments: ~110 (of 639 review comments by joka921/hannahbast).
Fan-out rule wording: Weak test assertions and repetitive test setup: Tests use gmock matchers (`EXPECT_THAT`, `ElementsAre`, custom matchers via `AD_FIELD`) rather than element-wise `EXPECT_EQ`; repeated setup code is factored into helpers/parameterized suites.
Fan-out review trigger words: `EXPECT_THAT`, `ElementsAre`, `AD_FIELD`, `gmock`, `TEST\(`.

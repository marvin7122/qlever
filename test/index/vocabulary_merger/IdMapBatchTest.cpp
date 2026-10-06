// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <cstdint>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#if GTEST_HAS_DEATH_TEST && defined(__unix__)
#include <sys/resource.h>
#endif

#include "VocabularyMergerTestHelpers.h"
#include "backports/filesystem.h"
#include "index/vocabulary_merger/IdMapBatch.h"

using namespace ad_utility::vocabulary_merger;
using namespace vocabularyMergerTestHelpers;
using ad_utility::vocabulary_merger::detail::IdMapBatch;
using ad_utility::vocabulary_merger::detail::IdMapBatchWriter;
using ad_utility::vocabulary_merger::detail::LocalIdxToBatchMapping;
using ad_utility::vocabulary_merger::detail::LocalIdxToBatchMappings;

namespace {
// The basename of the partial vocabularies that the tests below create. It
// needs no test-specific part, because each test runs in its own working
// directory (see `makePartialVocabularyFilenamesInFreshDirectory`).
const std::string partialVocabBasename = "vocab-";

// Create an `IdMapBatch` from the given `mappings` and `globalIds`. In
// contrast to the `WordBatchBuilder` (which allocates the mappings in
// advance), the `numMappings_` here is simply the size of the `mappings`.
IdMapBatch makeBatch(const std::vector<LocalIdxToBatchMapping>& mappings,
                     const std::vector<Id>& globalIds) {
  LocalIdxToBatchMappings localIdxMappings;
  localIdxMappings.mappings_.insert(localIdxMappings.mappings_.end(),
                                    mappings.begin(), mappings.end());
  localIdxMappings.numMappings_ = mappings.size();
  return IdMapBatch{std::move(localIdxMappings), globalIds};
}

// Exercise cache eviction and later reopening with interleaved mappings in two
// batches, each larger than a writer's buffer for every nonempty map. Leave the
// last map empty. Return distinct error codes for use in a subprocess as well
// as the ordinary tests. All files and writers are cleaned up before returning.
int exerciseManyIdMaps(size_t numFiles, bool explicitFinish,
                       bool writeEntries = true) {
  int errorCode = 20;
  try {
    auto [filenames, cleanup] = makePartialVocabularyFilenamesInFreshDirectory(
        partialVocabBasename, numFiles);
    const size_t entriesPerBatch =
        idMapWriterBufferSize.getBytes() / sizeof(IdMapEntry) + 17;
    const size_t entriesPerFile = writeEntries ? 2 * entriesPerBatch : 0;
    auto checkReadback = [&]() {
      errorCode = 24;
      for (size_t file = 0; file < numFiles; ++file) {
        const auto entries = getIdMapFromFile(filenames.idMapFiles_[file]);
        const size_t expectedSize = file + 1 == numFiles ? 0 : entriesPerFile;
        if (entries.size() != expectedSize) {
          return 30;
        }
        for (size_t i = 0; i < expectedSize; ++i) {
          if (entries[i] !=
              IdMapEntry{L(file * entriesPerFile + i), V(100 + i)}) {
            return 31;
          }
        }
      }
      return 0;
    };
    {
      errorCode = 20;
      IdMapBatchWriter writer{
          ad_utility::InputRangeTypeErased{filenames.idMapFiles_}};
      errorCode = 21;
      if (writeEntries) {
        for (size_t batch = 0; batch < 2; ++batch) {
          std::vector<LocalIdxToBatchMapping> mappings;
          std::vector<Id> globalIds;
          for (size_t i = 0; i < entriesPerBatch; ++i) {
            const size_t index = batch * entriesPerBatch + i;
            globalIds.push_back(V(100 + index));
            for (size_t file = 0; file + 1 < numFiles; ++file) {
              mappings.push_back(LocalIdxToBatchMapping{
                  static_cast<uint32_t>(file), static_cast<uint32_t>(i),
                  L(file * entriesPerFile + index)});
            }
          }
          writer.writeBatch(makeBatch(mappings, globalIds));
        }
      }
      if (explicitFinish) {
        errorCode = 22;
        writer.finish();
        writer.finish();
        // Read while the writer is still alive: finish must release the cache
        // and flush all stream buffers, including the final size headers.
        return checkReadback();
      }
      errorCode = 23;
    }
    return checkReadback();
  } catch (...) {
    return errorCode;
  }
}
}  // namespace

// _____________________________________________________________________________
TEST(IdMapBatchWriter, manyMapsWithExplicitFinish) {
  EXPECT_EQ(exerciseManyIdMaps(96, true), 0);
}

// _____________________________________________________________________________
TEST(IdMapBatchWriter, manyMapsWithDestructorFinish) {
  EXPECT_EQ(exerciseManyIdMaps(96, false), 0);
}

// _____________________________________________________________________________
TEST(IdMapBatchWriter, manyEmptyMaps) {
  EXPECT_EQ(exerciseManyIdMaps(96, true, false), 0);
  EXPECT_EQ(exerciseManyIdMaps(96, false, false), 0);
}

#if GTEST_HAS_DEATH_TEST && defined(__unix__)
namespace {
class IdMapBatchIoFailureDeathTest : public ::testing::Test {
 private:
  std::string oldStyle_;

 protected:
  void SetUp() override {
    oldStyle_ = ::testing::FLAGS_gtest_death_test_style;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    if (!ql::filesystem::exists("/dev/full")) {
      GTEST_SKIP() << "/dev/full is unavailable";
    }
  }
  void TearDown() override {
    ::testing::FLAGS_gtest_death_test_style = oldStyle_;
  }
};

IdMapBatch makeLargeBatch() {
  const size_t numEntries =
      idMapWriterBufferSize.getBytes() / sizeof(IdMapEntry) + 17;
  std::vector<LocalIdxToBatchMapping> mappings;
  for (size_t i = 0; i < numEntries; ++i) {
    mappings.push_back({0, 0, L(i)});
  }
  return makeBatch(mappings, {V(10)});
}

template <typename F>
std::string expectIoFailure(const F& operation) {
  try {
    operation();
  } catch (const std::exception& error) {
    EXPECT_THAT(error.what(), ::testing::HasSubstr("ID map file /dev/full"));
    return error.what();
  }
  ADD_FAILURE() << "Expected a checked ID map I/O failure";
  return {};
}
}  // namespace

// _____________________________________________________________________________
TEST_F(IdMapBatchIoFailureDeathTest, destructorOnlyIoFailure) {
  auto run = [] {
    {
      std::vector<std::string> filenames{"/dev/full"};
      IdMapBatchWriter writer{ad_utility::InputRangeTypeErased{filenames}};
      writer.writeBatch(makeBatch({{0, 0, L(7)}}, {V(10)}));
    }
    std::_Exit(0);
  };
  EXPECT_EXIT(run(), ::testing::ExitedWithCode(0), "");
}

// _____________________________________________________________________________
TEST_F(IdMapBatchIoFailureDeathTest, largeWriteFailureCaughtBeforeDestruction) {
  auto run = [] {
    {
      std::vector<std::string> filenames{"/dev/full"};
      IdMapBatchWriter writer{ad_utility::InputRangeTypeErased{filenames}};
      auto first =
          expectIoFailure([&] { writer.writeBatch(makeLargeBatch()); });
      // A small batch would normally stay buffered. Even that operation must
      // rethrow the first error without touching the damaged cache/buffer.
      EXPECT_EQ(expectIoFailure([&] {
                  writer.writeBatch(makeBatch({{0, 0, L(7)}}, {V(10)}));
                }),
                first);
      EXPECT_EQ(expectIoFailure([&] { writer.finish(); }), first);
      EXPECT_EQ(expectIoFailure([&] { writer.finish(); }), first);
    }
    std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
  };
  EXPECT_EXIT(run(), ::testing::ExitedWithCode(0), "");
}

// _____________________________________________________________________________
TEST_F(IdMapBatchIoFailureDeathTest, largeWriteFailureDuringUnwinding) {
  auto run = [] {
    expectIoFailure([] {
      std::vector<std::string> filenames{"/dev/full"};
      IdMapBatchWriter writer{ad_utility::InputRangeTypeErased{filenames}};
      writer.writeBatch(makeLargeBatch());
    });
    // The original I/O exception reached the catch outside the writer scope.
    std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
  };
  EXPECT_EXIT(run(), ::testing::ExitedWithCode(0), "");
}

// _____________________________________________________________________________
TEST_F(IdMapBatchIoFailureDeathTest, bufferedFinishFailure) {
  auto run = [] {
    {
      std::vector<std::string> filenames{"/dev/full"};
      IdMapBatchWriter writer{ad_utility::InputRangeTypeErased{filenames}};
      writer.writeBatch(makeBatch({{0, 0, L(7)}}, {V(10)}));
      auto first = expectIoFailure([&] { writer.finish(); });
      EXPECT_EQ(expectIoFailure([&] { writer.finish(); }), first);
      EXPECT_EQ(expectIoFailure([&] {
                  writer.writeBatch(makeBatch({{0, 0, L(8)}}, {V(11)}));
                }),
                first);
    }
    std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
  };
  EXPECT_EXIT(run(), ::testing::ExitedWithCode(0), "");
}

// _____________________________________________________________________________
TEST_F(IdMapBatchIoFailureDeathTest, partialConstructionFailure) {
  auto run = [] {
    auto [ordinaryFiles, cleanup] =
        makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
    // Only the fresh ordinary directory is cleaned up; /dev/full never is.
    std::vector<std::string> filenames{
        "/dev/full", "nonexistent-directory/" + ordinaryFiles.idMapFiles_[0]};
    try {
      IdMapBatchWriter writer{ad_utility::InputRangeTypeErased{filenames}};
      ADD_FAILURE() << "Expected the second file to fail to open";
    } catch (const std::runtime_error& error) {
      EXPECT_THAT(error.what(), ::testing::HasSubstr("Could not open file"));
      EXPECT_THAT(error.what(), ::testing::HasSubstr("nonexistent-directory/"));
    }
    // Run cleanup before exiting the subprocess.
  };
  EXPECT_EXIT(
      {
        run();
        std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
      },
      ::testing::ExitedWithCode(0), "");
}

// _____________________________________________________________________________
TEST_F(IdMapBatchIoFailureDeathTest, moveAssignmentReplacesFailedDestination) {
  auto run = [] {
    auto [ordinaryFiles, cleanup] =
        makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
    {
      std::vector<std::string> fullFiles{"/dev/full"};
      IdMapBatchWriter destination{ad_utility::InputRangeTypeErased{fullFiles}};
      expectIoFailure([&] { destination.writeBatch(makeLargeBatch()); });
      IdMapBatchWriter source{
          ad_utility::InputRangeTypeErased{ordinaryFiles.idMapFiles_}};
      source.writeBatch(makeBatch({{0, 0, L(7)}}, {V(10)}));
      destination = std::move(source);
      source.finish();
      destination.writeBatch(makeBatch({{0, 0, L(8)}}, {V(11)}));
      destination.finish();
    }
    EXPECT_THAT(getIdMapFromFile(ordinaryFiles.idMapFiles_[0]),
                ::testing::ElementsAre(IdMapEntry{L(7), V(10)},
                                       IdMapEntry{L(8), V(11)}));
  };
  EXPECT_EXIT(
      {
        run();
        std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
      },
      ::testing::ExitedWithCode(0), "");
}

// _____________________________________________________________________________
TEST(IdMapBatchWriterDeathTest, moreMapsThanFileDescriptorLimit) {
  struct rlimit limit {};
  ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &limit), 0);
  if (limit.rlim_max < 128) {
    GTEST_SKIP() << "The hard descriptor limit is below 128";
  }
  const auto oldStyle = ::testing::FLAGS_gtest_death_test_style;
  absl::Cleanup restoreStyle{
      [oldStyle] { ::testing::FLAGS_gtest_death_test_style = oldStyle; }};
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  auto checkDescriptorLimit = [] {
    struct rlimit childLimit {};
    if (getrlimit(RLIMIT_NOFILE, &childLimit) != 0) {
      std::_Exit(10);
    }
    childLimit.rlim_cur = 128;
    if (setrlimit(RLIMIT_NOFILE, &childLimit) != 0) {
      std::_Exit(11);
    }
    // The helper destroys the writers and cleans up its directory before the
    // process exits, including when construction or writing throws.
    const int result = exerciseManyIdMaps(256, true);
    std::_Exit(result);
  };
  EXPECT_EXIT(checkDescriptorLimit(), ::testing::ExitedWithCode(0), "");
}
#endif

// _____________________________________________________________________________
TEST(IdMapBatchWriter, createdAndFinishedDuringUnwinding) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
  const std::string originalMessage = "Original outer error";
  bool callbackRan = false;
  try {
    absl::Cleanup writeDuringUnwinding{[&] {
      callbackRan = true;
      EXPECT_GT(std::uncaught_exceptions(), 0);
      IdMapBatchWriter writer{
          ad_utility::InputRangeTypeErased{filenames.idMapFiles_}};
      writer.writeBatch(makeBatch({{0, 0, L(7)}}, {V(10)}));
      writer.finish();
    }};
    throw std::runtime_error{originalMessage};
  } catch (const std::runtime_error& error) {
    EXPECT_EQ(error.what(), originalMessage);
  }
  EXPECT_TRUE(callbackRan);
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[0]),
              ::testing::ElementsAre(IdMapEntry{L(7), V(10)}));
}

// _____________________________________________________________________________
TEST(IdMapBatchWriter, moveConstructionAndAssignment) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 2);
  {
    std::vector<std::string> sourceFiles{filenames.idMapFiles_[0]};
    std::vector<std::string> destinationFiles{filenames.idMapFiles_[1]};
    IdMapBatchWriter source{ad_utility::InputRangeTypeErased{sourceFiles}};
    source.writeBatch(makeBatch({{0, 0, L(7)}}, {V(10)}));
    IdMapBatchWriter moved{std::move(source)};
    source.finish();
    moved.writeBatch(makeBatch({{0, 0, L(8)}}, {V(11)}));
    IdMapBatchWriter destination{
        ad_utility::InputRangeTypeErased{destinationFiles}};
    destination.writeBatch(makeBatch({{0, 0, L(9)}}, {V(12)}));
    destination = std::move(moved);
    moved.finish();
    // Self assignment must not finish or discard the transferred writers.
    auto* alias = &destination;
    destination = std::move(*alias);
    destination.writeBatch(makeBatch({{0, 0, L(10)}}, {V(13)}));
    destination.finish();
    destination.finish();
  }
  EXPECT_THAT(
      getIdMapFromFile(filenames.idMapFiles_[0]),
      ::testing::ElementsAre(IdMapEntry{L(7), V(10)}, IdMapEntry{L(8), V(11)},
                             IdMapEntry{L(10), V(13)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[1]),
              ::testing::ElementsAre(IdMapEntry{L(9), V(12)}));
}

// _____________________________________________________________________________
// The `IdMapBatchWriter` distributes the mappings of its batches over one ID
// map per partial vocabulary, resolves the `indexOfWordInBatch_` of each
// mapping via the `globalIds_` of its batch, and keeps the order in which the
// mappings were pushed.
TEST(IdMapBatchWriter, writeSeveralBatches) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 3);

  {
    IdMapBatchWriter writer{
        ad_utility::InputRangeTypeErased{filenames.idMapFiles_}};
    // The first batch has two distinct words with the global IDs `10` and
    // `11`. The first word occurs in the partial vocabularies `0` and `2`, the
    // second one only in `0`.
    writer.writeBatch(makeBatch(
        {LocalIdxToBatchMapping{0, 0, L(7)}, LocalIdxToBatchMapping{2, 0, L(8)},
         LocalIdxToBatchMapping{0, 1, L(9)}},
        {V(10), V(11)}));
    // The second batch has a single word with the global ID `12`, which occurs
    // in all three partial vocabularies.
    writer.writeBatch(makeBatch({LocalIdxToBatchMapping{0, 0, L(100)},
                                 LocalIdxToBatchMapping{1, 0, L(101)},
                                 LocalIdxToBatchMapping{2, 0, L(102)}},
                                {V(12)}));
    writer.finish();
  }

  EXPECT_THAT(
      getIdMapFromFile(filenames.idMapFiles_[0]),
      ::testing::ElementsAre(IdMapEntry{L(7), V(10)}, IdMapEntry{L(9), V(11)},
                             IdMapEntry{L(100), V(12)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[1]),
              ::testing::ElementsAre(IdMapEntry{L(101), V(12)}));
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[2]),
              ::testing::ElementsAre(IdMapEntry{L(8), V(10)},
                                     IdMapEntry{L(102), V(12)}));
}

// _____________________________________________________________________________
// An `IdMapBatchWriter` to which no batch was written creates one empty ID map
// per partial vocabulary. Its destructor closes those maps, so an explicit
// call to `finish()` is not required.
TEST(IdMapBatchWriter, noBatches) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
  {
    IdMapBatchWriter writer{
        ad_utility::InputRangeTypeErased{filenames.idMapFiles_}};
  }
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[0]), ::testing::IsEmpty());
}

// _____________________________________________________________________________
// Only the first `numMappings_` of the `mappings_` of a batch are valid; the
// remaining (uninitialized) ones must not be written.
TEST(IdMapBatchWriter, onlyValidMappingsAreWritten) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);

  auto batch = makeBatch({LocalIdxToBatchMapping{0, 0, L(42)}}, {V(43)});
  // Allocate (but do not initialize) space for many more mappings, exactly as
  // the `WordBatchBuilder` does.
  batch.localIdxMappings_.mappings_.resize(1000);
  {
    IdMapBatchWriter writer{
        ad_utility::InputRangeTypeErased{filenames.idMapFiles_}};
    writer.writeBatch(batch);
  }
  EXPECT_THAT(getIdMapFromFile(filenames.idMapFiles_[0]),
              ::testing::ElementsAre(IdMapEntry{L(42), V(43)}));
}

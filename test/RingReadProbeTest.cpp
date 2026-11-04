// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/IoUringManagerTestHelpers.h"
#include "util/io_uring/RingReadProbe.h"

namespace {

using namespace ::testing;
using ad_utility::CachedRingRead;
using ad_utility::PageCacheFastPathMode;
using ad_utility::probeCachedRingRead;
using ioUringManagerTestHelpers::makeTempFile;

// A `NowaitRingRead` that returns the given results in order (the last one
// repeatedly) and records the arguments of every read.
class ScriptedRingRead : public ad_utility::NowaitRingRead {
 public:
  explicit ScriptedRingRead(std::vector<int> results)
      : results_{std::move(results)} {}

  int read(int fd, char* buffer, size_t numberOfBytes,
           uint64_t offset) override {
    (void)fd;
    (void)buffer;
    reads_.emplace_back(numberOfBytes, offset);
    const size_t index = std::min(reads_.size(), results_.size()) - 1;
    return results_.at(index);
  }

  // The `(numberOfBytes, offset)` of every read.
  std::vector<std::pair<size_t, uint64_t>> reads_;

 private:
  std::vector<int> results_;
};

// _____________________________________________________________________________
TEST(ProbeCachedRingRead, aReadThatSucceedsMeansInline) {
  auto [tmp, fd] = makeTempFile(std::string(10'000, 'x'));
  ScriptedRingRead ring{{4096}};
  EXPECT_EQ(probeCachedRingRead(ring, fd), CachedRingRead::Inline);
  // The first page of the file, once.
  EXPECT_THAT(ring.reads_, ElementsAre(std::pair<size_t, uint64_t>{4096, 0}));
}

// _____________________________________________________________________________
TEST(ProbeCachedRingRead, eagainInEveryTrialMeansPunted) {
  auto [tmp, fd] = makeTempFile(std::string(10'000, 'x'));
  ScriptedRingRead ring{{-EAGAIN}};
  EXPECT_EQ(probeCachedRingRead(ring, fd, 3), CachedRingRead::Punted);
  EXPECT_EQ(ring.reads_.size(), 3u);
}

// _____________________________________________________________________________
TEST(ProbeCachedRingRead, oneEagainIsRetried) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  ScriptedRingRead ring{{-EAGAIN, 8}};
  EXPECT_EQ(probeCachedRingRead(ring, fd), CachedRingRead::Inline);
  // A file smaller than a page is read completely.
  EXPECT_THAT(ring.reads_, ElementsAre(std::pair<size_t, uint64_t>{8, 0},
                                       std::pair<size_t, uint64_t>{8, 0}));
}

// _____________________________________________________________________________
TEST(ProbeCachedRingRead, otherErrorsAndEmptyFilesAreUnknown) {
  {
    auto [tmp, fd] = makeTempFile("AAAA");
    ScriptedRingRead ring{{-EOPNOTSUPP}};
    EXPECT_EQ(probeCachedRingRead(ring, fd), CachedRingRead::Unknown);
    EXPECT_EQ(ring.reads_.size(), 1u);
  }
  {
    auto [tmp, fd] = makeTempFile("");
    ScriptedRingRead ring{{4096}};
    EXPECT_EQ(probeCachedRingRead(ring, fd), CachedRingRead::Unknown);
    EXPECT_THAT(ring.reads_, IsEmpty());
  }
  {
    // Not a valid file descriptor.
    ScriptedRingRead ring{{4096}};
    EXPECT_EQ(probeCachedRingRead(ring, -1), CachedRingRead::Unknown);
    EXPECT_THAT(ring.reads_, IsEmpty());
  }
}

// _____________________________________________________________________________
TEST(UsePageCacheFastPath, onAndOffOverrideTheProbe) {
  using Enum = PageCacheFastPathMode::Enum;
  for (auto cachedRingRead : {CachedRingRead::Inline, CachedRingRead::Punted,
                              CachedRingRead::Unknown}) {
    EXPECT_TRUE(ad_utility::usePageCacheFastPath(
        PageCacheFastPathMode{Enum::On}, cachedRingRead));
    EXPECT_FALSE(ad_utility::usePageCacheFastPath(
        PageCacheFastPathMode{Enum::Off}, cachedRingRead));
  }
  // `auto` uses the fast path unless the ring reads page-cache hits inline.
  const PageCacheFastPathMode automatic{Enum::Auto};
  EXPECT_FALSE(
      ad_utility::usePageCacheFastPath(automatic, CachedRingRead::Inline));
  EXPECT_TRUE(
      ad_utility::usePageCacheFastPath(automatic, CachedRingRead::Punted));
  EXPECT_TRUE(
      ad_utility::usePageCacheFastPath(automatic, CachedRingRead::Unknown));
}

// _____________________________________________________________________________
TEST(PageCacheFastPathMode, fromAndToString) {
  using Enum = PageCacheFastPathMode::Enum;
  EXPECT_EQ(PageCacheFastPathMode::fromString("auto").value(), Enum::Auto);
  EXPECT_EQ(PageCacheFastPathMode::fromString("on").value(), Enum::On);
  EXPECT_EQ(PageCacheFastPathMode::fromString("off").value(), Enum::Off);
  EXPECT_EQ(PageCacheFastPathMode{Enum::Off}.toString(), "off");
  AD_EXPECT_THROW_WITH_MESSAGE(
      std::ignore = PageCacheFastPathMode::fromString("true"),
      HasSubstr("is not a valid page-cache fast-path mode"));
  // The parameter also accepts the old boolean values.
  const PageCacheFastPathMode::FromStringWithBooleans parse;
  EXPECT_EQ(parse("true").value(), Enum::On);
  EXPECT_EQ(parse("false").value(), Enum::Off);
  EXPECT_EQ(parse("auto").value(), Enum::Auto);
  AD_EXPECT_THROW_WITH_MESSAGE(std::ignore = parse("yes"),
                               HasSubstr("is not a valid"));
}

// _____________________________________________________________________________
// The real probe on a temporary file: its result depends on the kernel and the
// file system, but it is the same for the second file on the same device,
// which is not probed again.
TEST(CachedRingReadOfDevice, isCachedPerDevice) {
  auto [tmp, fd] = makeTempFile(std::string(10'000, 'x'));
  auto [tmp2, fd2] = makeTempFile("AAAA");
  const CachedRingRead first = ad_utility::cachedRingReadOfDevice(fd);
  EXPECT_EQ(ad_utility::cachedRingReadOfDevice(fd2), first);
}

}  // namespace

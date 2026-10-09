// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "util/File.h"
#include "util/ResidentFileMapping.h"

namespace {
using ad_utility::ResidentFileMapping;

// Write `contents` to `filename` and return it opened for reading.
ad_utility::File writeAndOpen(const std::string& filename,
                              const std::string& contents) {
  {
    ad_utility::File file(filename, "w");
    file.write(contents.data(), contents.size());
  }
  return ad_utility::File(filename, "r");
}

// Three and a half pages of distinct bytes.
std::string makeContents() {
  std::string contents;
  for (size_t i = 0; i < 3 * ResidentFileMapping::pageSize + 2048; ++i) {
    contents.push_back(static_cast<char>('a' + i % 26));
  }
  return contents;
}
}  // namespace

// _____________________________________________________________________________
TEST(ResidentFileMapping, servesOnlyMarkedPages) {
  constexpr size_t P = ResidentFileMapping::pageSize;
  const std::string filename = "residentFileMappingTest.dat";
  const std::string contents = makeContents();
  // Delete the temporary file on every exit path (including the early return
  // below and assertion failures). Declared before `file`, so it runs after
  // `file` and `mapping` are destroyed.
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  auto file = writeAndOpen(filename, contents);
  ResidentFileMapping mapping(file.fd(), contents.size());
#ifdef __linux__
  ASSERT_TRUE(mapping.isMapped());
#endif
  std::string target(100, 'X');
  // Nothing is marked yet; the target is untouched.
  EXPECT_FALSE(mapping.tryRead(10, 20, target.data()));
  EXPECT_EQ(target, std::string(100, 'X'));
  // An empty read is always served.
  EXPECT_TRUE(mapping.tryRead(10, 0, target.data()));

  if (!mapping.isMapped()) {
    return;
  }
  // Mark a range within page 0 (the whole page is then known resident).
  mapping.markResident(5, 3);
  EXPECT_TRUE(mapping.tryRead(10, 20, target.data()));
  EXPECT_EQ(target.substr(0, 20), contents.substr(10, 20));
  // A range that reaches into page 1 is not served ...
  EXPECT_FALSE(mapping.tryRead(P - 4, 8, target.data()));
  // ... until page 1 is marked as well.
  mapping.markResident(P + 100, 1);
  EXPECT_TRUE(mapping.tryRead(P - 4, 8, target.data()));
  EXPECT_EQ(target.substr(0, 8), contents.substr(P - 4, 8));
  // The last, partial page.
  mapping.markResident(contents.size() - 1, 1);
  EXPECT_TRUE(mapping.tryRead(contents.size() - 10, 10, target.data()));
  EXPECT_EQ(target.substr(0, 10), contents.substr(contents.size() - 10));
  // Ranges beyond the end are never served and never marked.
  EXPECT_FALSE(mapping.tryRead(contents.size() - 5, 10, target.data()));
  EXPECT_FALSE(mapping.tryRead(contents.size() + 1, 1, target.data()));
  mapping.markResident(contents.size(), 10);
  mapping.markResident(contents.size() + P, 1);

  // `tryReadAll` and `markAllResident`.
  std::vector<size_t> numBytes{4, 4, 4};
  std::vector<uint64_t> offsets{1, 2 * P + 1, P + 1};
  std::vector<std::string> buffers(3, std::string(4, 'X'));
  std::vector<char*> targets{buffers[0].data(), buffers[1].data(),
                             buffers[2].data()};
  EXPECT_THAT(mapping.tryReadAll(numBytes, offsets, targets),
              ::testing::ElementsAre(1));
  std::vector<size_t> positions{1};
  mapping.markAllResident(numBytes, offsets, positions);
  EXPECT_THAT(mapping.tryReadAll(numBytes, offsets, targets),
              ::testing::IsEmpty());
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(buffers[i], contents.substr(offsets[i], 4));
  }

  // A moved-to mapping keeps the marks; the moved-from one serves nothing.
  ResidentFileMapping moved = std::move(mapping);
  EXPECT_TRUE(moved.tryRead(1, 4, target.data()));
  EXPECT_FALSE(mapping.isMapped());
  EXPECT_FALSE(mapping.tryRead(1, 4, target.data()));
  ResidentFileMapping assigned;
  assigned = std::move(moved);
  EXPECT_TRUE(assigned.tryRead(1, 4, target.data()));
}

// _____________________________________________________________________________
TEST(ResidentFileMapping, emptyAndUnmappableFiles) {
  // An empty file is not mapped.
  const std::string filename = "residentFileMappingEmpty.dat";
  // Declared before `file`, so the removal runs after `file` is destroyed.
  absl::Cleanup removeFile{[&filename]() { ad_utility::deleteFile(filename); }};
  auto file = writeAndOpen(filename, "");
  ResidentFileMapping empty(file.fd(), 0);
  EXPECT_FALSE(empty.isMapped());
  char c = 'X';
  EXPECT_FALSE(empty.tryRead(0, 1, &c));
  empty.markResident(0, 1);
  EXPECT_FALSE(empty.tryRead(0, 1, &c));
  // An invalid descriptor cannot be mapped.
  ResidentFileMapping invalid(-1, 4096);
  EXPECT_FALSE(invalid.isMapped());
  EXPECT_FALSE(invalid.tryRead(0, 1, &c));
}

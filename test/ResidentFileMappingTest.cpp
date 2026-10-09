// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "util/File.h"
#include "util/ResidentFileMapping.h"

#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#endif

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
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TEST(ResidentFileMapping, capBoundsMarkedPages) {
  constexpr size_t P = ResidentFileMapping::pageSize;
  // Eight full pages.
  std::string contents(8 * P, 'q');
  auto file = writeAndOpen("residentFileMappingCap.dat", contents);
  ResidentFileMapping mapping(file.fd(), contents.size());
  if (!mapping.isMapped()) {
    ad_utility::deleteFile("residentFileMappingCap.dat");
    return;
  }
  EXPECT_EQ(mapping.residentCapPages(), 0);
  // Unbounded by default: all marks stick.
  for (size_t page = 0; page < 8; ++page) {
    mapping.markResident(page * P, 1);
  }
  std::string target(4, 'X');
  for (size_t page = 0; page < 8; ++page) {
    EXPECT_TRUE(mapping.tryRead(page * P, 4, target.data()));
  }
  // A cap of two pages evicts down immediately, oldest first.
  mapping.setResidentCapPages(2);
  EXPECT_EQ(mapping.residentCapPages(), 2);
  size_t served = 0;
  for (size_t page = 0; page < 8; ++page) {
    served += mapping.tryRead(page * P, 4, target.data()) ? 1 : 0;
  }
  EXPECT_LE(served, 2);
  // Marking beyond the cap keeps at most the cap (the CLOCK hand evicts
  // oldest first; no survival promise for any single page).
  mapping.markResident(0, 1);
  served = 0;
  for (size_t page = 0; page < 8; ++page) {
    served += mapping.tryRead(page * P, 4, target.data()) ? 1 : 0;
  }
  EXPECT_LE(served, 2);
  // Lifting the cap stops eviction; lowering it to zero disables the bound.
  mapping.setResidentCapPages(0);
  for (size_t page = 0; page < 8; ++page) {
    mapping.markResident(page * P, 1);
  }
  for (size_t page = 0; page < 8; ++page) {
    EXPECT_TRUE(mapping.tryRead(page * P, 4, target.data()));
  }
  // The cap survives a move.
  mapping.setResidentCapPages(3);
  ResidentFileMapping moved = std::move(mapping);
  EXPECT_EQ(moved.residentCapPages(), 3);
  ad_utility::deleteFile("residentFileMappingCap.dat");
}

#ifdef __linux__
// _____________________________________________________________________________
// Evicted pages are demoted from the page cache, verified with `mincore`
// through the mapping itself. This must stay the single live mapping of the
// file: a second mapping pins the pages against cross-mapping pageout
// (probed directly), which would make the check meaningless.
TEST(ResidentFileMapping, evictedPagesAreDemoted) {
  constexpr size_t P = ResidentFileMapping::pageSize;
  std::string contents(4 * P, 'z');
  auto file = writeAndOpen("residentFileMappingDemote.dat", contents);
  // Demotion only applies to clean pages (freshly written dirty pages are
  // left for writeback, as in production where vocabulary files are synced
  // at index build), so sync before mapping.
  ASSERT_EQ(::fsync(file.fd()), 0);
  ResidentFileMapping mapping(file.fd(), contents.size());
  ASSERT_TRUE(mapping.isMapped());
  const char* probe = mapping.mappingForTesting();
  // Fault all pages into the page cache by reading them.
  volatile char sink = 0;
  for (size_t i = 0; i < contents.size(); i += P) {
    sink += probe[i];
  }
  (void)sink;
  auto mincoreResident = [&](size_t& out) {
    std::vector<unsigned char> vec(4, 0);
    EXPECT_EQ(::mincore(const_cast<char*>(probe), contents.size(), vec.data()),
              0);
    out = 0;
    for (auto b : vec) {
      out += (b & 1) ? 1 : 0;
    }
  };
  size_t resident = 0;
  mincoreResident(resident);
  ASSERT_EQ(resident, 4);
  // Mark everything, then cap at one page: three pages must demote.
  for (size_t page = 0; page < 4; ++page) {
    mapping.markResident(page * P, 1);
  }
  mapping.setResidentCapPages(1);
  mincoreResident(resident);
  EXPECT_EQ(resident, 1);
  ad_utility::deleteFile("residentFileMappingDemote.dat");
}
#endif

// _____________________________________________________________________________
TEST(ResidentFileMapping, emptyAndUnmappableFiles) {
  // An empty file is not mapped.
  auto file = writeAndOpen("residentFileMappingEmpty.dat", "");
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
  ad_utility::deleteFile("residentFileMappingEmpty.dat");
}

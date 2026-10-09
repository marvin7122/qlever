// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "util/File.h"
#include "util/ResidentFileMapping.h"

#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace {
using ad_utility::FrameKey;
using ad_utility::HotStringCache;
using ad_utility::ResidentFileMapping;
using ad_utility::ScanRingBuffer;
using ad_utility::VocabFramePool;

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
  // A cap of two pages evicts down immediately in CLOCK order.
  mapping.setResidentCapPages(2);
  EXPECT_EQ(mapping.residentCapPages(), 2);
  size_t served = 0;
  for (size_t page = 0; page < 8; ++page) {
    served += mapping.tryRead(page * P, 4, target.data()) ? 1 : 0;
  }
  EXPECT_LE(served, 2);
  // A newly marked page gets a second chance while the cap is enforced.
  mapping.markResident(0, 1);
  EXPECT_TRUE(mapping.tryRead(0, 4, target.data()));
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

// _____________________________________________________________________________
TEST(ResidentFileMapping, clockGivesAccessedPagesASecondChanceAfterMove) {
  constexpr size_t P = ResidentFileMapping::pageSize;
  const std::string filename = "residentFileMappingClock.dat";
  // Cross a bitmap word boundary, with a partial last page.
  const std::string contents(65 * P + 123, 'c');
  auto file = writeAndOpen(filename, contents);
  for (bool useTryRead : {false, true}) {
    SCOPED_TRACE(useTryRead);
    ResidentFileMapping mapping(file.fd(), contents.size());
    if (!mapping.isMapped()) {
      break;
    }
    mapping.markResident(61 * P, 4 * P);
    mapping.setResidentCapPages(3);
    // The sweep evicts page 61 and leaves the hand at page 62. The surviving
    // pages have had their reference bits cleared on the first revolution.
    char target = 'X';
    EXPECT_FALSE(mapping.tryRead(61 * P, 1, &target));
    if (useTryRead) {
      ASSERT_TRUE(mapping.tryRead(62 * P, 1, &target));
      EXPECT_EQ(target, 'c');
    } else {
      mapping.markResident(62 * P, 1);
    }
    ResidentFileMapping moved = std::move(mapping);
    ResidentFileMapping assigned;
    assigned = std::move(moved);
    assigned.markResident(contents.size() - 1, 1);
    // Page 62 gets a second chance; the next page in CLOCK order is evicted.
    EXPECT_TRUE(assigned.tryRead(62 * P, 1, &target));
    EXPECT_FALSE(assigned.tryRead(63 * P, 1, &target));
    EXPECT_TRUE(assigned.tryRead(64 * P, 1, &target));
    EXPECT_TRUE(assigned.tryRead(contents.size() - 1, 1, &target));
  }
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
TEST(ResidentFileMapping, capUsesBitmapAfterConcurrentUpdates) {
  constexpr size_t P = ResidentFileMapping::pageSize;
  const std::string filename = "residentFileMappingConcurrent.dat";
  const std::string contents(70 * P, 't');
  auto file = writeAndOpen(filename, contents);
  ResidentFileMapping mapping(file.fd(), contents.size());
  if (mapping.isMapped()) {
    mapping.setResidentCapPages(3);
    std::vector<std::thread> workers;
    for (size_t worker = 0; worker < 4; ++worker) {
      workers.emplace_back([&, worker]() {
        char target;
        for (size_t i = 0; i < 200; ++i) {
          const size_t offset = ((i + worker * 17) % 70) * P;
          mapping.markResident(offset, 1);
          mapping.tryRead(offset, 1, &target);
          mapping.setResidentCapPages(i % 2 == 0 ? 3 : 5);
        }
      });
    }
    for (auto& worker : workers) {
      worker.join();
    }
    // Reapply the cap after concurrent changes, including an unchanged cap.
    for (size_t cap : {size_t{3}, size_t{3}, size_t{1}}) {
      mapping.setResidentCapPages(cap);
      size_t served = 0;
      char target;
      for (size_t page = 0; page < 70; ++page) {
        served += mapping.tryRead(page * P, 1, &target) ? 1 : 0;
      }
      EXPECT_LE(served, cap);
    }
  }
  ad_utility::deleteFile(filename);
}

#ifdef __linux__
// Probe whether this kernel honors partial-range `MADV_PAGEOUT` demotion
// (verified working on 6.19, silently ignored on Ural's 7.0.0-28, where only
// full-mapping ranges demote and `MADV_DONTNEED` never does).
bool kernelSupportsPartialDemotion() {
  const std::string filename = "residentFileMappingProbe.dat";
  {
    ad_utility::File file(filename, "w");
    std::string filler(2 * ResidentFileMapping::pageSize, 'p');
    file.write(filler.data(), filler.size());
  }
  ad_utility::File file(filename, "r");
  // Demotion applies to clean pages only; sync the fresh file first.
  if (::fsync(file.fd()) != 0) {
    ad_utility::deleteFile(filename);
    return false;
  }
  void* probe = ::mmap(nullptr, 2 * ResidentFileMapping::pageSize, PROT_READ,
                       MAP_SHARED, file.fd(), 0);
  if (probe == MAP_FAILED) {
    ad_utility::deleteFile(filename);
    return false;
  }
  volatile char sink = 0;
  for (size_t i = 0; i < 2 * ResidentFileMapping::pageSize;
       i += ResidentFileMapping::pageSize) {
    sink += static_cast<const char*>(probe)[i];
  }
  (void)sink;
  ::madvise(probe, ResidentFileMapping::pageSize,
#ifdef MADV_PAGEOUT
            MADV_PAGEOUT
#else
            MADV_DONTNEED
#endif
  );
  std::vector<unsigned char> vec(2, 0);
  const bool ok =
      ::mincore(probe, 2 * ResidentFileMapping::pageSize, vec.data()) == 0;
  const bool demoted = ok && (vec[0] & 1) == 0 && (vec[1] & 1) == 1;
  ::munmap(probe, 2 * ResidentFileMapping::pageSize);
  ad_utility::deleteFile(filename);
  return demoted;
}

// _____________________________________________________________________________
// Evicted pages are demoted from the page cache, verified with `mincore`
// through the mapping itself. This must stay the single live mapping of the
// file: a second mapping pins the pages against cross-mapping pageout
// (probed directly), which would make the check meaningless.
TEST(ResidentFileMapping, evictedPagesAreDemoted) {
  if (!kernelSupportsPartialDemotion()) {
    GTEST_SKIP() << "kernel ignores partial-range demotion (e.g. Ural)";
  }
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
// Frame table unit test: (file, page) keys map to distinct 4 KiB frames with
// free/loading/resident state, and the admission filter keeps scan traffic
// out of the pool.
TEST(ResidentFileMapping, frameTableMapsFilePageToFrames) {
  VocabFramePool pool{4};
  EXPECT_EQ(pool.capacity(), 4);
  EXPECT_EQ(pool.residentCount(), 0);
  // Admission filter (Postgres discipline): scans never enter the pool.
  EXPECT_FALSE(VocabFramePool::admit(true));
  EXPECT_TRUE(VocabFramePool::admit(false));

  const FrameKey key{0, 7};
  EXPECT_EQ(pool.tryPin(key), nullptr);
  VocabFramePool::Frame* loading = pool.allocate(key);
  ASSERT_NE(loading, nullptr);
  EXPECT_EQ(loading->state, VocabFramePool::FrameState::Loading);
  // A loading frame is not yet readable.
  char byte = 'X';
  EXPECT_FALSE(pool.copyPinned(key, 0, 1, &byte));
  std::fill(loading->data.begin(), loading->data.end(), 'f');
  pool.markLoaded(key);
  EXPECT_EQ(pool.residentCount(), 1);
  VocabFramePool::Frame* pinned = pool.tryPin(key);
  ASSERT_NE(pinned, nullptr);
  EXPECT_TRUE(pool.copyPinned(key, 0, 1, &byte));
  EXPECT_EQ(byte, 'f');
  pool.unpin(key);
  pool.unpin(key);
  EXPECT_EQ(pool.pinnedCount(), 0);

  // The offsets file is a distinct key space over the same table shape.
  const FrameKey offsetsKey{1, 7};
  EXPECT_EQ(pool.tryPin(offsetsKey), nullptr);
  VocabFramePool::Frame* offsetsLoading = pool.allocate(offsetsKey);
  ASSERT_NE(offsetsLoading, nullptr);
  EXPECT_NE(offsetsLoading, loading);
  pool.abandon(offsetsKey);
  EXPECT_EQ(pool.residentCount(), 1);
}

// _____________________________________________________________________________
// Pooled reads are byte-identical with the file: fills go through the reader
// callback (production: the io_uring addBatch/wait path; here: pread), and
// `tryReadPooled` serves pinned copies.
TEST(ResidentFileMapping, pooledReadsAreByteIdentical) {
  constexpr size_t P = ResidentFileMapping::pageSize;
  const std::string filename = "residentFileMappingPooled.dat";
  const std::string contents = makeContents();
  auto file = writeAndOpen(filename, contents);
  ResidentFileMapping mapping(file.fd(), contents.size());
  if (!mapping.isMapped()) {
    ad_utility::deleteFile(filename);
    return;
  }
  EXPECT_FALSE(mapping.framePoolEnabled());
  std::string target(16, 'X');
  EXPECT_FALSE(mapping.tryReadPooled(10, 4, target.data()));

  mapping.enableFramePool(8);
  EXPECT_TRUE(mapping.framePoolEnabled());
  auto preadReader = [&](char* dst, uint64_t fileOffset,
                         size_t numBytes) -> bool {
    return file.read(dst, numBytes, static_cast<off_t>(fileOffset)) ==
           static_cast<ssize_t>(numBytes);
  };
  // Fill every page of the file through the reader.
  const size_t numPages = (contents.size() + P - 1) / P;
  for (size_t page = 0; page < numPages; ++page) {
    ASSERT_TRUE(mapping.fillFrameFromFile(page, preadReader));
  }
  // Single-page and cross-page reads match the file byte for byte.
  EXPECT_TRUE(mapping.tryReadPooled(10, 20, target.data()));
  EXPECT_EQ(target.substr(0, 20), contents.substr(10, 20));
  EXPECT_TRUE(mapping.tryReadPooled(P - 4, 8, target.data()));
  EXPECT_EQ(target.substr(0, 8), contents.substr(P - 4, 8));
  // A failing fill leaves the pool usable and the page missing.
  ResidentFileMapping failing(file.fd(), contents.size());
  failing.enableFramePool(8);
  EXPECT_FALSE(failing.fillFrameFromFile(
      0, [](char*, uint64_t, size_t) { return false; }));
  EXPECT_FALSE(failing.tryReadPooled(0, 1, target.data()));
  EXPECT_TRUE(failing.fillFrameFromFile(0, preadReader));
  EXPECT_TRUE(failing.tryReadPooled(0, 1, target.data()));
  EXPECT_EQ(target[0], contents[0]);
  // Out-of-range pages and ranges are rejected; empty reads are served.
  EXPECT_FALSE(mapping.fillFrameFromFile(numPages + 1, preadReader));
  EXPECT_FALSE(mapping.tryReadPooled(contents.size() - 5, 10, target.data()));
  EXPECT_TRUE(mapping.tryReadPooled(10, 0, target.data()));
  // Disabling the pool disables pooled reads; the resident-bit path is
  // untouched (nothing marked, so it misses).
  mapping.disableFramePool();
  EXPECT_FALSE(mapping.framePoolEnabled());
  EXPECT_FALSE(mapping.tryReadPooled(10, 4, target.data()));
  EXPECT_FALSE(mapping.tryRead(10, 4, target.data()));
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
// Pin discipline: eviction only touches unpinned frames, so a pinned page
// survives forced evictions with intact bytes; when every frame is pinned,
// allocation refuses instead of evicting.
TEST(ResidentFileMapping, noEvictWhilePinned) {
  constexpr size_t P = ResidentFileMapping::pageSize;
  const std::string filename = "residentFileMappingPinned.dat";
  std::string contents(8 * P, 'x');
  for (size_t i = 0; i < contents.size(); ++i) {
    contents[i] = static_cast<char>('a' + (i / P) % 26);
  }
  auto file = writeAndOpen(filename, contents);
  ResidentFileMapping mapping(file.fd(), contents.size());
  if (!mapping.isMapped()) {
    ad_utility::deleteFile(filename);
    return;
  }
  mapping.enableFramePool(4, 1);
  auto preadReader = [&](char* dst, uint64_t fileOffset,
                         size_t numBytes) -> bool {
    return file.read(dst, numBytes, static_cast<off_t>(fileOffset)) ==
           static_cast<ssize_t>(numBytes);
  };
  for (size_t page = 0; page < 4; ++page) {
    ASSERT_TRUE(mapping.fillFrameFromFile(page, preadReader));
  }
  VocabFramePool* pool = mapping.framePool();
  ASSERT_NE(pool, nullptr);
  // Pin page 0 and hold it across forced evictions of pages 4..6.
  const FrameKey pinnedKey{1, 0};
  ASSERT_NE(pool->tryPin(pinnedKey), nullptr);
  for (size_t page = 4; page < 7; ++page) {
    ASSERT_TRUE(mapping.fillFrameFromFile(page, preadReader));
  }
  // The pinned page survived with intact bytes (no evict-while-pinned).
  char byte = 'X';
  EXPECT_TRUE(pool->copyPinned(pinnedKey, 0, 1, &byte));
  EXPECT_EQ(byte, contents[0]);
  pool->unpin(pinnedKey);
  EXPECT_EQ(pool->pinnedCount(), 0);
  // Pin every frame of a fresh pool: allocation must refuse instead of
  // evicting a pinned frame.
  mapping.enableFramePool(4, 1);
  for (size_t page = 0; page < 4; ++page) {
    ASSERT_TRUE(mapping.fillFrameFromFile(page, preadReader));
  }
  pool = mapping.framePool();
  ASSERT_NE(pool, nullptr);
  std::vector<FrameKey> allPinned;
  for (size_t page = 0; page < 4; ++page) {
    const FrameKey key{1, page};
    EXPECT_NE(pool->tryPin(key), nullptr);
    allPinned.push_back(key);
  }
  EXPECT_EQ(pool->allocate(FrameKey{1, 7}), nullptr);
  for (const auto& key : allPinned) {
    pool->unpin(key);
  }
  EXPECT_EQ(pool->pinnedCount(), 0);
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
// Eviction race (fault injection): concurrent fills, pins, and pooled reads
// never produce a use-after-unpin; every served read is byte-identical.
TEST(ResidentFileMapping, pooledReadsStayByteIdenticalUnderEvictionRace) {
  constexpr size_t P = ResidentFileMapping::pageSize;
  const std::string filename = "residentFileMappingPoolRace.dat";
  std::string contents(16 * P, 'r');
  for (size_t i = 0; i < contents.size(); ++i) {
    contents[i] = static_cast<char>('A' + i % 26);
  }
  auto file = writeAndOpen(filename, contents);
  ResidentFileMapping mapping(file.fd(), contents.size());
  if (!mapping.isMapped()) {
    ad_utility::deleteFile(filename);
    return;
  }
  mapping.enableFramePool(4);
  std::atomic<bool> failed{false};
  std::vector<std::thread> workers;
  for (size_t worker = 0; worker < 4; ++worker) {
    workers.emplace_back([&, worker]() {
      auto reader = [&](char* dst, uint64_t fileOffset,
                        size_t numBytes) -> bool {
        return file.read(dst, numBytes, static_cast<off_t>(fileOffset)) ==
               static_cast<ssize_t>(numBytes);
      };
      std::string target(32, 'X');
      for (size_t i = 0; i < 300; ++i) {
        const size_t page = (i * 7 + worker * 5) % 16;
        // Fills may refuse under pin pressure; that is legal, never an
        // evict-while-pinned.
        mapping.fillFrameFromFile(page, reader);
        const uint64_t offset = page * P + (i % 16);
        const size_t len = std::min<size_t>(32, contents.size() - offset);
        std::fill(target.begin(), target.end(), 'X');
        if (mapping.tryReadPooled(offset, len, target.data()) &&
            target.substr(0, len) != contents.substr(offset, len)) {
          failed.store(true);
        }
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  EXPECT_FALSE(failed.load());
  EXPECT_EQ(mapping.framePool()->pinnedCount(), 0);
  ad_utility::deleteFile(filename);
}

// _____________________________________________________________________________
// Scan ring buffers stay off-pool and the hot-string LRU evicts cold entries.
TEST(ResidentFileMapping, scanRingAndHotStringCacheDiscipline) {
  ScanRingBuffer ring;
  char* first = ring.next();
  ASSERT_NE(first, nullptr);
  std::memset(first, 's', ScanRingBuffer::kSlotSize);
  // The ring holds confined slots; walking past them wraps around.
  for (size_t i = 1; i < ScanRingBuffer::kNumSlots; ++i) {
    EXPECT_NE(ring.next(), nullptr);
  }
  char* wrapped = ring.next();
  EXPECT_EQ(wrapped, first);

  HotStringCache cache{3};
  EXPECT_EQ(cache.size(), 0);
  EXPECT_FALSE(cache.lookup(1).has_value());
  cache.insert(1, "one");
  cache.insert(2, "two");
  cache.insert(3, "three");
  EXPECT_EQ(cache.lookup(1), "one");
  // Inserting a fourth entry evicts the least-recently-used (index 2: index
  // 1 was refreshed by the lookup above).
  cache.insert(4, "four");
  EXPECT_EQ(cache.size(), 3);
  EXPECT_EQ(cache.lookup(1), "one");
  EXPECT_FALSE(cache.lookup(2).has_value());
  EXPECT_EQ(cache.lookup(4), "four");
}

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

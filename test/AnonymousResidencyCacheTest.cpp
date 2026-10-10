// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <atomic>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

#include "util/AnonymousResidencyCache.h"
#include "util/File.h"
#include "util/GTestHelpers.h"

using ad_utility::AnonymousResidencyCache;
namespace {

// Deterministic pseudo-random bytes: page `p` starts with a per-page seed so
// that cross-page confusion is detectable by `memcmp`.
std::vector<char> makeFileContent(size_t numBytes) {
  std::vector<char> content(numBytes);
  std::mt19937_64 rng{0x5EED42};
  for (size_t i = 0; i < numBytes; ++i) {
    // Mix the page index into the stream so identical offsets in different
    // pages differ.
    content[i] = static_cast<char>(
        (rng() >> ((i % 8) * 8)) ^
        ((i / AnonymousResidencyCache::kPageSize) * 0x9E3779B9ull));
  }
  return content;
}

struct TestFile {
  std::string filename_;
  std::vector<char> content_;
  ad_utility::File file_;
  explicit TestFile(std::string filename, size_t numBytes)
      : filename_{std::move(filename)},
        content_{makeFileContent(numBytes)},
        file_{filename_, "w"} {
    file_.write(content_.data(), content_.size());
    file_.close();
    file_.open(filename_, "r");
  }
  ~TestFile() { std::remove(filename_.c_str()); }
};

// Fetch every page of `file` and check byte identity against `content`.
void expectByteIdentity(AnonymousResidencyCache& cache, ad_utility::File& file,
                        const std::vector<char>& content) {
  const size_t kPage = AnonymousResidencyCache::kPageSize;
  const size_t numPages = (content.size() + kPage - 1) / kPage;
  for (size_t p = 0; p < numPages; ++p) {
    auto pin = cache.fetch(file, p);
    ASSERT_TRUE(pin.has_value()) << "page " << p;
    const size_t n = std::min(kPage, content.size() - p * kPage);
    EXPECT_EQ(std::memcmp(pin->data(), content.data() + p * kPage, n), 0)
        << "page " << p;
  }
}

TEST(AnonymousResidencyCache, FillServesByteIdenticalPages) {
  TestFile tf{"anonVmcacheFill.dat",
              4 * AnonymousResidencyCache::kPageSize + 123};
  AnonymousResidencyCache cache{8, tf.content_.size()};
  ASSERT_TRUE(cache.isActive());
  expectByteIdentity(cache, tf.file_, tf.content_);
  EXPECT_EQ(cache.residentCount(), 5);
  auto stats = cache.stats();
  EXPECT_EQ(stats.hits_, 0);
  EXPECT_EQ(stats.misses_, 5);
  // Second pass is all hits.
  expectByteIdentity(cache, tf.file_, tf.content_);
  EXPECT_EQ(cache.stats().hits_, 5);
}

TEST(AnonymousResidencyCache, InertWhenNoFrames) {
  TestFile tf{"anonVmcacheInert.dat", 100};
  AnonymousResidencyCache cache{0, tf.content_.size()};
  EXPECT_FALSE(cache.isActive());
  EXPECT_FALSE(cache.fetch(tf.file_, 0).has_value());
  // `readThrough` still serves correct bytes via direct reads.
  std::string buf(100, '\0');
  EXPECT_TRUE(cache.readThrough(tf.file_, 0, 100, buf.data()));
  EXPECT_EQ(std::memcmp(buf.data(), tf.content_.data(), 100), 0);
}

TEST(AnonymousResidencyCache, OutOfRangeAndEmptyReads) {
  TestFile tf{"anonVmcacheRange.dat", AnonymousResidencyCache::kPageSize};
  AnonymousResidencyCache cache{2, tf.content_.size()};
  EXPECT_FALSE(cache.fetch(tf.file_, 1).has_value());
  std::string buf(10, '\0');
  EXPECT_TRUE(cache.readThrough(tf.file_, 0, 0, buf.data()));
  EXPECT_FALSE(
      cache.readThrough(tf.file_, 0, tf.content_.size() + 1, buf.data()));
}

TEST(AnonymousResidencyCache, EvictionKeepsByteIdentity) {
  constexpr size_t kPage = AnonymousResidencyCache::kPageSize;
  TestFile tf{"anonVmcacheEvict.dat", 6 * kPage};
  AnonymousResidencyCache cache{2, tf.content_.size()};
  // Touch all pages; the 2-frame cache must evict and refill.
  expectByteIdentity(cache, tf.file_, tf.content_);
  EXPECT_LE(cache.residentCount(), 2);
  EXPECT_GE(cache.stats().evictions_, 4);
  // Re-read everything after the eviction storm: still identical.
  expectByteIdentity(cache, tf.file_, tf.content_);
}

TEST(AnonymousResidencyCache, ClockPrefersUnreferencedVictim) {
  constexpr size_t kPage = AnonymousResidencyCache::kPageSize;
  TestFile tf{"anonVmcacheClock.dat", 3 * kPage};
  AnonymousResidencyCache cache{2, tf.content_.size()};
  // Fill frames with pages 0, 1; re-touch page 0 for a second chance.
  ASSERT_TRUE(cache.fetch(tf.file_, 0).has_value());
  ASSERT_TRUE(cache.fetch(tf.file_, 1).has_value());
  ASSERT_TRUE(cache.fetch(tf.file_, 0).has_value());
  // Fetching page 2 must evict page 1 (no reference bit), not page 0.
  ASSERT_TRUE(cache.fetch(tf.file_, 2).has_value());
  auto stats = cache.stats();
  EXPECT_EQ(stats.evictions_, 1);
  // Page 0 is still resident (hit), page 1 was evicted (miss).
  const size_t hitsBefore = cache.stats().hits_;
  const size_t missesBefore = cache.stats().misses_;
  ASSERT_TRUE(cache.fetch(tf.file_, 0).has_value());
  ASSERT_TRUE(cache.fetch(tf.file_, 1).has_value());
  EXPECT_EQ(cache.stats().hits_, hitsBefore + 1);
  EXPECT_EQ(cache.stats().misses_, missesBefore + 1);
}

TEST(AnonymousResidencyCache, AllPinnedFallsBackToDirectReads) {
  constexpr size_t kPage = AnonymousResidencyCache::kPageSize;
  TestFile tf{"anonVmcachePinned.dat", 2 * kPage};
  AnonymousResidencyCache cache{1, tf.content_.size()};
  auto pin = cache.fetch(tf.file_, 0);
  ASSERT_TRUE(pin.has_value());
  // Every frame pinned: `fetch` refuses instead of evicting in-flight data.
  EXPECT_FALSE(cache.fetch(tf.file_, 1).has_value());
  // `readThrough` still serves correct bytes via the direct path.
  std::string buf(kPage, '\0');
  EXPECT_TRUE(cache.readThrough(tf.file_, kPage, kPage, buf.data()));
  EXPECT_EQ(std::memcmp(buf.data(), tf.content_.data() + kPage, kPage), 0);
  // Releasing the pin makes the frame evictable again (pin lifecycle in the
  // guard type: destruction unpins).
  pin.reset();
  auto pin2 = cache.fetch(tf.file_, 1);
  ASSERT_TRUE(pin2.has_value());
  EXPECT_EQ(std::memcmp(pin2->data(), tf.content_.data() + kPage, kPage), 0);
}

TEST(AnonymousResidencyCache, MovedGuardTransfersPin) {
  TestFile tf{"anonVmcacheMove.dat", AnonymousResidencyCache::kPageSize};
  AnonymousResidencyCache cache{1, tf.content_.size()};
  auto pin = cache.fetch(tf.file_, 0);
  ASSERT_TRUE(pin.has_value());
  auto pin2 = std::move(pin);
  EXPECT_FALSE(pin);
  ASSERT_TRUE(pin2);
  // Still pinned exactly once: fetching another page with one frame fails.
  TestFile tf2{"anonVmcacheMove2.dat", 2 * AnonymousResidencyCache::kPageSize};
  EXPECT_FALSE(cache.fetch(tf2.file_, 1).has_value());
}

TEST(AnonymousResidencyCache, ReadThroughSpansPages) {
  constexpr size_t kPage = AnonymousResidencyCache::kPageSize;
  TestFile tf{"anonVmcacheSpan.dat", 3 * kPage + 7};
  AnonymousResidencyCache cache{4, tf.content_.size()};
  // Unaligned range crossing two page boundaries.
  const uint64_t offset = kPage - 13;
  const size_t n = 2 * kPage + 29;
  std::string buf(n, '\0');
  EXPECT_TRUE(cache.readThrough(tf.file_, offset, n, buf.data()));
  EXPECT_EQ(std::memcmp(buf.data(), tf.content_.data() + offset, n), 0);
  // Fully cached now: no further misses.
  const size_t misses = cache.stats().misses_;
  EXPECT_TRUE(cache.readThrough(tf.file_, offset, n, buf.data()));
  EXPECT_EQ(cache.stats().misses_, misses);
}

TEST(AnonymousResidencyCache, PersistentPinsSurviveClockPressure) {
  constexpr size_t kPage = AnonymousResidencyCache::kPageSize;
  TestFile tf{"anonVmcacheHotSet.dat", 20 * kPage};
  AnonymousResidencyCache cache{4, tf.content_.size()};
  // Persistent hot set: hold guards for the first two pages, as
  // `VocabularyOnDisk` does for `vocab-anon-pin-pages`.
  std::vector<AnonymousResidencyCache::PinnedFrame> hotSet;
  for (uint64_t page = 0; page < 2; ++page) {
    auto pin = cache.fetch(tf.file_, page);
    ASSERT_TRUE(pin.has_value()) << "page " << page;
    hotSet.push_back(std::move(*pin));
  }
  // Streaming pressure over the remaining pages with only two free frames.
  for (int round = 0; round < 3; ++round) {
    for (uint64_t page = 2; page < 20; ++page) {
      auto tmp = cache.fetch(tf.file_, page);
      ASSERT_TRUE(tmp.has_value()) << "page " << page;
    }
  }
  EXPECT_GE(cache.stats().evictions_, 1);
  // The hot set was never evicted: the held guards still serve identical
  // bytes, and re-fetching a hot page is a hit (no additional miss) that
  // counts as a pinned hit.
  for (uint64_t page = 0; page < 2; ++page) {
    EXPECT_EQ(std::memcmp(hotSet[page].data(),
                          tf.content_.data() + page * kPage, kPage),
              0)
        << "page " << page;
  }
  const size_t missesBefore = cache.stats().misses_;
  const size_t pinnedBefore = cache.stats().pinned_;
  for (uint64_t page = 0; page < 2; ++page) {
    auto pin = cache.fetch(tf.file_, page);
    ASSERT_TRUE(pin.has_value()) << "page " << page;
  }
  EXPECT_EQ(cache.stats().misses_, missesBefore);
  EXPECT_EQ(cache.stats().pinned_, pinnedBefore + 2);
}

TEST(AnonymousResidencyCache, PinCountHonored) {
  constexpr size_t kPage = AnonymousResidencyCache::kPageSize;
  TestFile tf{"anonVmcachePinCount.dat", 4 * kPage};
  AnonymousResidencyCache cache{2, tf.content_.size()};
  // Two held pins consume both frames of the 2-frame cache ...
  std::vector<AnonymousResidencyCache::PinnedFrame> pins;
  for (uint64_t page = 0; page < 2; ++page) {
    auto pin = cache.fetch(tf.file_, page);
    ASSERT_TRUE(pin.has_value()) << "page " << page;
    pins.push_back(std::move(*pin));
  }
  EXPECT_EQ(cache.residentCount(), 2);
  // ... so a third page has no evictable frame left and falls back.
  EXPECT_FALSE(cache.fetch(tf.file_, 2).has_value());
  // Releasing one pin frees exactly one frame for the next page.
  pins.pop_back();
  auto pin = cache.fetch(tf.file_, 2);
  ASSERT_TRUE(pin.has_value());
  EXPECT_EQ(std::memcmp(pin->data(), tf.content_.data() + 2 * kPage, kPage), 0);
}

TEST(AnonymousResidencyCache, StatsCountPinnedHits) {
  constexpr size_t kPage = AnonymousResidencyCache::kPageSize;
  TestFile tf{"anonVmcachePinnedStats.dat", 2 * kPage};
  AnonymousResidencyCache cache{2, tf.content_.size()};
  // Plain sequential use never overlaps guards: a hit, but no pinned hit.
  ASSERT_TRUE(cache.fetch(tf.file_, 0).has_value());
  ASSERT_TRUE(cache.fetch(tf.file_, 0).has_value());
  EXPECT_EQ(cache.stats().hits_, 1);
  EXPECT_EQ(cache.stats().pinned_, 0);
  // With a persistent guard held, re-fetching the same page is a pinned hit.
  auto pin = cache.fetch(tf.file_, 1);
  ASSERT_TRUE(pin.has_value());
  ASSERT_TRUE(cache.fetch(tf.file_, 1).has_value());
  EXPECT_EQ(cache.stats().hits_, 2);
  EXPECT_EQ(cache.stats().pinned_, 1);
}

TEST(AnonymousResidencyCache, ConcurrentFetchesStayIdentical) {
  constexpr size_t kPage = AnonymousResidencyCache::kPageSize;
  TestFile tf{"anonVmcacheThreads.dat", 32 * kPage};
  AnonymousResidencyCache cache{8, tf.content_.size()};
  std::atomic<size_t> failures{0};
  auto worker = [&](unsigned seed) {
    std::mt19937 rng{seed};
    std::string buf(kPage, '\0');
    for (int i = 0; i < 200; ++i) {
      const uint64_t page = rng() % 32;
      if (auto pin = cache.fetch(tf.file_, page)) {
        if (std::memcmp(pin->data(), tf.content_.data() + page * kPage,
                        kPage) != 0) {
          failures.fetch_add(1);
        }
      } else if (!cache.readThrough(tf.file_, page * kPage, kPage,
                                    buf.data()) ||
                 std::memcmp(buf.data(), tf.content_.data() + page * kPage,
                             kPage) != 0) {
        failures.fetch_add(1);
      }
    }
  };
  std::vector<std::thread> threads;
  for (unsigned t = 0; t < 8; ++t) {
    threads.emplace_back(worker, t);
  }
  for (auto& th : threads) {
    th.join();
  }
  EXPECT_EQ(failures.load(), 0);
}

}  // namespace

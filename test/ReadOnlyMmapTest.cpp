// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "util/File.h"
#include "util/ReadOnlyMmap.h"

namespace ad_utility {

// Write `content` to a fresh file and return its name; the returned
// `TempFileGuard` removes it on scope exit, including on an early return
// from `ASSERT_*` or an exception unwinding the test.
class TempFileGuard {
 public:
  explicit TempFileGuard(std::string filename)
      : filename_{std::move(filename)} {}
  // Never throwing: a `filesystem_error` during stack unwinding would
  // otherwise call `std::terminate`.
  ~TempFileGuard() noexcept {
    std::error_code ec;
    std::filesystem::remove(filename_, ec);
  }
  TempFileGuard(const TempFileGuard&) = delete;
  TempFileGuard& operator=(const TempFileGuard&) = delete;
  TempFileGuard(TempFileGuard&& other) noexcept
      : filename_{std::exchange(other.filename_, {})} {}
  TempFileGuard& operator=(TempFileGuard&& other) noexcept {
    if (this != &other) {
      std::error_code ec;
      std::filesystem::remove(filename_, ec);
      filename_ = std::exchange(other.filename_, {});
    }
    return *this;
  }

  const std::string& name() const { return filename_; }

 private:
  std::string filename_;
};

TempFileGuard writeTempFile(std::string_view content) {
  // Unique per process and per call: parallel `ctest` shards share the
  // working directory, and a crashed run may leave its file behind.
  static unsigned counter = 0;
  std::filesystem::path filename =
      std::filesystem::temp_directory_path() /
      (::testing::UnitTest::GetInstance()->current_test_info()->name() +
       std::string{"-"} + std::to_string(::getpid()) + std::string{"-"} +
       std::to_string(counter++) + std::string{".tmp"});
  File file{filename.string(), "w"};
  file.write(content.data(), content.size());
  file.close();
  return TempFileGuard{filename.string()};
}

TEST(ReadOnlyMmap, DefaultIsUnmapped) {
  ReadOnlyMmap mapping;
  EXPECT_FALSE(mapping.isMapped());
  EXPECT_EQ(mapping.size(), 0u);
}

TEST(ReadOnlyMmap, MapsFileContents) {
  const std::string payload = "0123456789abcdef";
  const TempFileGuard tempFile = writeTempFile(payload);
  File file{tempFile.name(), "r"};

  ReadOnlyMmap mapping;
  ASSERT_TRUE(mapping.map(file.fd(), payload.size()));
  EXPECT_TRUE(mapping.isMapped());
  EXPECT_EQ(mapping.size(), payload.size());
  ASSERT_NE(mapping.data(), nullptr);
  std::string_view mapped{static_cast<const char*>(mapping.data()),
                          mapping.size()};
  EXPECT_EQ(mapped, payload);

  // A second `map` of the same region is a no-op success.
  EXPECT_TRUE(mapping.map(file.fd(), payload.size()));
}

TEST(ReadOnlyMmap, RemapDifferentRegionIsRejected) {
  const std::string payload = "0123456789abcdef";
  const TempFileGuard tempFile = writeTempFile(payload);
  File file{tempFile.name(), "r"};

  ReadOnlyMmap mapping;
  ASSERT_TRUE(mapping.map(file.fd(), payload.size()));
  // A different region on the mapped instance is rejected, and the
  // original mapping is left untouched.
  EXPECT_FALSE(mapping.map(file.fd(), 4, 4));
  EXPECT_TRUE(mapping.isMapped());
  EXPECT_EQ(mapping.size(), payload.size());
  std::string_view mapped{static_cast<const char*>(mapping.data()),
                          mapping.size()};
  EXPECT_EQ(mapped, payload);
  // After `unmap` the other region maps fine.
  mapping.unmap();
  ASSERT_TRUE(mapping.map(file.fd(), 6, 4));
  std::string_view suffix{static_cast<const char*>(mapping.data()),
                          mapping.size()};
  EXPECT_EQ(suffix, "456789");
}

TEST(ReadOnlyMmap, MapsSuffixAtOffset) {
  const std::string payload = "0123456789abcdef";
  const TempFileGuard tempFile = writeTempFile(payload);
  File file{tempFile.name(), "r"};

  ReadOnlyMmap mapping;
  ASSERT_TRUE(mapping.map(file.fd(), 6, 4));
  std::string_view mapped{static_cast<const char*>(mapping.data()),
                          mapping.size()};
  EXPECT_EQ(mapped, "456789");
}

TEST(ReadOnlyMmap, FailedMapStaysUnmapped) {
  ReadOnlyMmap mapping;
  // Invalid file descriptor and empty range must both fail gracefully.
  EXPECT_FALSE(mapping.map(-1, 8));
  EXPECT_FALSE(mapping.isMapped());
  EXPECT_FALSE(mapping.map(0, 0));
  EXPECT_FALSE(mapping.isMapped());
}

TEST(ReadOnlyMmap, MoveTransfersMapping) {
  const std::string payload = "0123456789abcdef";
  const TempFileGuard tempFile = writeTempFile(payload);
  File file{tempFile.name(), "r"};

  ReadOnlyMmap source;
  ASSERT_TRUE(source.map(file.fd(), payload.size()));
  const void* base = source.data();

  ReadOnlyMmap moved{std::move(source)};
  EXPECT_FALSE(source.isMapped());
  EXPECT_TRUE(moved.isMapped());
  EXPECT_EQ(moved.data(), base);

  ReadOnlyMmap assigned;
  assigned = std::move(moved);
  EXPECT_FALSE(moved.isMapped());
  EXPECT_TRUE(assigned.isMapped());
  EXPECT_EQ(assigned.data(), base);
  std::string_view mapped{static_cast<const char*>(assigned.data()),
                          assigned.size()};
  EXPECT_EQ(mapped, payload);
}

}  // namespace ad_utility

// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>

#include "util/File.h"
#include "util/ReadOnlyMmap.h"

namespace ad_utility {

// Write `content` to a fresh file and return its name; the caller removes it.
std::string writeTempFile(std::string_view content) {
  std::string filename =
      ::testing::UnitTest::GetInstance()->current_test_info()->name();
  filename += ".tmp";
  File file{filename, "w"};
  file.write(content.data(), content.size());
  file.close();
  return filename;
}

TEST(ReadOnlyMmap, DefaultIsUnmapped) {
  ReadOnlyMmap mapping;
  EXPECT_FALSE(mapping.isMapped());
  EXPECT_EQ(mapping.size(), 0u);
}

TEST(ReadOnlyMmap, MapsFileContents) {
  const std::string payload = "0123456789abcdef";
  const std::string filename = writeTempFile(payload);
  File file{filename, "r"};

  ReadOnlyMmap mapping;
  ASSERT_TRUE(mapping.map(file.fd(), payload.size()));
  EXPECT_TRUE(mapping.isMapped());
  EXPECT_EQ(mapping.size(), payload.size());
  ASSERT_NE(mapping.data(), nullptr);
  EXPECT_EQ(std::string_view{static_cast<const char*>(mapping.data()),
                             mapping.size()},
            payload);

  // A second `map` on an already mapped instance is a no-op success.
  EXPECT_TRUE(mapping.map(file.fd(), payload.size()));

  std::filesystem::remove(filename);
}

TEST(ReadOnlyMmap, MapsSuffixAtOffset) {
  const std::string payload = "0123456789abcdef";
  const std::string filename = writeTempFile(payload);
  File file{filename, "r"};

  ReadOnlyMmap mapping;
  ASSERT_TRUE(mapping.map(file.fd(), 6, 4));
  EXPECT_EQ(std::string_view{static_cast<const char*>(mapping.data()),
                             mapping.size()},
            "456789");

  std::filesystem::remove(filename);
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
  const std::string filename = writeTempFile(payload);
  File file{filename, "r"};

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
  EXPECT_EQ(std::string_view{static_cast<const char*>(assigned.data()),
                             assigned.size()},
            payload);

  std::filesystem::remove(filename);
}

}  // namespace ad_utility

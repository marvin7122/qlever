// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#include "./util/GTestHelpers.h"
#include "backports/span.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/RegisteredIoUringReader.h"

using namespace ad_utility::export_prototypes;

namespace {
// Buffered (non-`O_DIRECT`) configuration without registered files or
// buffers, so that the test works on every file system.
RegisteredReaderConfig bufferedConfig() {
  RegisteredReaderConfig config;
  config.ringEntries = 8;
  config.useDirectIo = false;
  config.useRegisteredFiles = false;
  config.useRegisteredBuffers = false;
  return config;
}
}  // namespace

// Two batches are in flight at the same time and are waited for in reverse
// order. Each `waitBatch` must report exactly the completions of its own
// batch, also when they were reaped while waiting for the other batch.
TEST(RegisteredIoUringReader, WaitBatchOutOfOrderKeepsPerBatchResults) {
  const std::string filename = gtestCurrentTestName() + ".dat";
  std::string content;
  for (size_t i = 0; i < 4096; ++i) {
    content.push_back(static_cast<char>('a' + i % 26));
  }
  {
    std::ofstream out(filename, std::ios::binary);
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
  }
  auto removeFile = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
      [&filename]() { std::remove(filename.c_str()); });

  const int fd = ::open(filename.c_str(), O_RDONLY);
  ASSERT_GE(fd, 0);
  auto closeFd = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
      [fd]() { ::close(fd); });

  RegisteredIoUringReader reader{bufferedConfig()};
  std::vector<char> destination(4096);
  auto makeRequest = [&](size_t offset, size_t numBytes) {
    return BlockReadRequest{static_cast<uint32_t>(fd),
                            offset,
                            0,
                            0,
                            static_cast<uint32_t>(numBytes),
                            destination.data() + offset,
                            false};
  };
  std::vector<BlockReadRequest> first{makeRequest(0, 1000),
                                      makeRequest(1000, 1000)};
  std::vector<BlockReadRequest> second{
      makeRequest(2000, 500), makeRequest(2500, 500), makeRequest(3000, 1096)};

  auto firstId = reader.submitBatch(ql::span<const BlockReadRequest>{first});
  auto secondId = reader.submitBatch(ql::span<const BlockReadRequest>{second});

  BatchResult secondResult = reader.waitBatch(secondId);
  EXPECT_EQ(secondResult.requestsCompleted, 3u);
  EXPECT_EQ(secondResult.totalBytesRead, 2096u);

  BatchResult firstResult = reader.waitBatch(firstId);
  EXPECT_EQ(firstResult.requestsCompleted, 2u);
  EXPECT_EQ(firstResult.totalBytesRead, 2000u);

  EXPECT_EQ(std::string(destination.data(), destination.size()), content);

  // A batch that was already waited for reports nothing anymore.
  BatchResult again = reader.waitBatch(firstId);
  EXPECT_EQ(again.requestsCompleted, 0u);
  EXPECT_EQ(again.totalBytesRead, 0u);
}

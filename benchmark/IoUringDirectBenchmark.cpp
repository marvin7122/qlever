// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Per-read overhead of the strategies of `RegisteredIoUringReader`: `pread`
// through the page cache or with `O_DIRECT`, and `io_uring` with plain
// descriptors and buffers, with registered files, and with registered files
// plus registered buffers. Every strategy reads the same 1 GiB file in 4 KiB
// blocks, once in file order and once with the batches shuffled.

#include <absl/strings/str_cat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/RegisteredIoUringReader.h"

namespace ad_benchmark {
namespace {

using ad_utility::export_prototypes::BatchResult;
using ad_utility::export_prototypes::BlockReadRequest;
using ad_utility::export_prototypes::DirectIoFile;
using ad_utility::export_prototypes::kDirectIoAlignment;
using ad_utility::export_prototypes::kDirectIoBlockSize;
using ad_utility::export_prototypes::PinnedArena;
using ad_utility::export_prototypes::RegisteredIoUringReader;
using ad_utility::export_prototypes::RegisteredReaderConfig;

// The file is larger than the CPU caches, so every read touches memory, and
// small enough for a temporary directory.
constexpr size_t kFileSizeBytes = size_t{1} << 30;
// Every read is one `O_DIRECT` block, the unit of a vocabulary block read.
constexpr size_t kBlockSizeBytes = kDirectIoBlockSize;
// A batch is 1 MiB. The ring has room for two batches, so a batch never waits
// for a free submission entry.
constexpr size_t kBlocksPerBatch = 256;
constexpr unsigned kRingEntries = 2 * kBlocksPerBatch;
constexpr size_t kBatchSizeBytes = kBlocksPerBatch * kBlockSizeBytes;
static_assert(kFileSizeBytes % kBatchSizeBytes == 0,
              "every batch lies completely inside the file");
constexpr size_t kNumBatches = kFileSizeBytes / kBatchSizeBytes;

// The message of the current `errno`. Unlike `strerror`, this is thread-safe.
std::string errnoMessage() { return std::system_category().message(errno); }

// Owner of a buffer from `posix_memalign`.
struct FreeDeleter {
  void operator()(char* buffer) const noexcept { std::free(buffer); }
};
using AlignedBuffer = std::unique_ptr<char, FreeDeleter>;

// Allocate `numBytes` bytes aligned for `O_DIRECT`.
AlignedBuffer allocateAlignedBuffer(size_t numBytes) {
  void* buffer = nullptr;
  if (posix_memalign(&buffer, kDirectIoAlignment, numBytes) != 0) {
    AD_THROW(absl::StrCat("posix_memalign failed for ", numBytes, " bytes"));
  }
  return AlignedBuffer{static_cast<char*>(buffer)};
}

// A temporary file of `kFileSizeBytes` pseudo-random bytes that is deleted
// when this object is destroyed. Construction writes and syncs the whole
// file, so create it once per benchmark run. Every reader of the file must
// be done before the destructor runs.
class TemporaryRandomFile {
  std::string path_;

 public:
  TemporaryRandomFile() {
    // `mkstemp` replaces the trailing `XXXXXX` with a unique suffix.
    path_ = (std::filesystem::temp_directory_path() /
             "qlever-iouring-benchmark-XXXXXX")
                .string();
    const int descriptor = mkstemp(path_.data());
    if (descriptor < 0) {
      AD_THROW(
          absl::StrCat("mkstemp failed for ", path_, ": ", errnoMessage()));
    }
    try {
      fill(descriptor);
    } catch (...) {
      ::close(descriptor);
      std::filesystem::remove(path_);
      throw;
    }
    if (::close(descriptor) != 0) {
      const std::string message = errnoMessage();
      std::filesystem::remove(path_);
      AD_THROW(absl::StrCat("close failed for ", path_, ": ", message));
    }
  }

  // A failed removal leaves a file in the temporary directory; a destructor
  // cannot report it, so the error code is ignored on purpose.
  ~TemporaryRandomFile() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

  TemporaryRandomFile(const TemporaryRandomFile&) = delete;
  TemporaryRandomFile& operator=(const TemporaryRandomFile&) = delete;
  TemporaryRandomFile(TemporaryRandomFile&&) = delete;
  TemporaryRandomFile& operator=(TemporaryRandomFile&&) = delete;

  const std::string& path() const { return path_; }

 private:
  // Write `kFileSizeBytes` pseudo-random bytes (fixed seed, so every run reads
  // the same content) in chunks of one batch, then flush them to the device.
  static void fill(int descriptor) {
    const AlignedBuffer chunk = allocateAlignedBuffer(kBatchSizeBytes);
    ql::span<uint64_t> chunkWords{reinterpret_cast<uint64_t*>(chunk.get()),
                                  kBatchSizeBytes / sizeof(uint64_t)};
    std::mt19937_64 randomWords{42};
    for (size_t batch = 0; batch < kNumBatches; ++batch) {
      std::generate(chunkWords.begin(), chunkWords.end(),
                    [&randomWords] { return randomWords(); });
      const ssize_t numBytesWritten =
          ::write(descriptor, chunk.get(), kBatchSizeBytes);
      if (numBytesWritten != static_cast<ssize_t>(kBatchSizeBytes)) {
        AD_THROW(absl::StrCat("write of the benchmark file failed: ",
                              errnoMessage()));
      }
    }
    if (::fsync(descriptor) != 0) {
      AD_THROW(
          absl::StrCat("fsync of the benchmark file failed: ", errnoMessage()));
    }
  }
};

// One way to read the file.
struct ReadStrategy {
  std::string_view label;
  bool directIo;
  // `false`: one synchronous `pread` per block. `true`: one `io_uring` batch
  // of `kBlocksPerBatch` reads at a time.
  bool useRing;
  bool registeredFiles;
  bool registeredBuffers;
};

constexpr std::array<ReadStrategy, 6> kReadStrategies{{
    {"pread, page cache", false, false, false, false},
    {"pread, O_DIRECT", true, false, false, false},
    {"io_uring, page cache", false, true, false, false},
    {"io_uring, O_DIRECT", true, true, false, false},
    {"io_uring, O_DIRECT, registered file", true, true, true, false},
    {"io_uring, O_DIRECT, registered file and buffers", true, true, true, true},
}};

enum class BatchOrder { FileOrder, Shuffled };

// The start offset of every batch, in file order or shuffled with a fixed
// seed, so that every strategy reads the batches in the same order.
std::vector<uint64_t> batchStartOffsets(BatchOrder order) {
  std::vector<uint64_t> offsets(kNumBatches);
  uint64_t nextOffset = 0;
  std::generate(offsets.begin(), offsets.end(), [&nextOffset] {
    return std::exchange(nextOffset, nextOffset + kBatchSizeBytes);
  });
  if (order == BatchOrder::Shuffled) {
    std::mt19937_64 shuffleEngine{1337};
    std::shuffle(offsets.begin(), offsets.end(), shuffleEngine);
  }
  return offsets;
}

// Read the whole file at `path` with `strategy`, in batches in `order`, and
// return the number of bytes read. Every block of a batch goes to its own
// slot of one arena, so all strategies write to the same aligned memory
// (`O_DIRECT` and registered buffers need that alignment; the others do not
// mind it). Only one batch is in flight at a time, as in a vocabulary lookup
// that needs a batch's words before it continues.
size_t readFile(const ReadStrategy& strategy, BatchOrder order,
                const std::string& path) {
  const DirectIoFile benchmarkFile{path, strategy.directIo};
  PinnedArena blockSlots{kBlocksPerBatch, kBlockSizeBytes};
  const std::vector<uint64_t> batchOffsets = batchStartOffsets(order);
  size_t numBytesRead = 0;

  if (!strategy.useRing) {
    for (const uint64_t batchOffset : batchOffsets) {
      for (size_t block = 0; block < kBlocksPerBatch; ++block) {
        RegisteredIoUringReader::readSync(
            benchmarkFile.fd(), batchOffset + block * kBlockSizeBytes,
            blockSlots.getSlotSpan(block), strategy.directIo);
        numBytesRead += kBlockSizeBytes;
      }
    }
    return numBytesRead;
  }

  RegisteredReaderConfig readerConfig;
  readerConfig.ringEntries = kRingEntries;
  readerConfig.useDirectIo = strategy.directIo;
  readerConfig.useRegisteredFiles = strategy.registeredFiles;
  readerConfig.useRegisteredBuffers = strategy.registeredBuffers;
  RegisteredIoUringReader reader{readerConfig};
  // Without a ring the reader silently falls back to `pread`, which would
  // measure the wrong strategy.
  if (!reader.isRingInitialized()) {
    AD_THROW(absl::StrCat("io_uring is not available, cannot measure \"",
                          strategy.label, "\""));
  }
  const int descriptor = benchmarkFile.fd();
  if (strategy.registeredFiles) {
    reader.registerFiles(ql::span<const int>{&descriptor, 1});
  }
  if (strategy.registeredBuffers) {
    reader.registerBuffers(blockSlots.iovecs());
  }
  // With a registered file, a request names its slot in the file table (the
  // only one, `0`); otherwise it names the descriptor itself.
  const auto fileIndex =
      static_cast<uint32_t>(strategy.registeredFiles ? 0 : descriptor);

  std::vector<BlockReadRequest> requests;
  requests.reserve(kBlocksPerBatch);
  for (const uint64_t batchOffset : batchOffsets) {
    requests.clear();
    for (size_t block = 0; block < kBlocksPerBatch; ++block) {
      // Block `block` of the batch goes to arena slot `block`, which is also
      // registered buffer `block` (see `PinnedArena::iovecs`).
      requests.emplace_back(fileIndex, batchOffset + block * kBlockSizeBytes,
                            static_cast<uint32_t>(block), 0, kBlockSizeBytes,
                            blockSlots.getSlotSpan(block).data(),
                            strategy.directIo);
    }
    const BatchResult batchResult =
        reader.waitBatch(reader.submitBatch(requests));
    numBytesRead += batchResult.totalBytesRead;
  }
  return numBytesRead;
}

}  // namespace

// The page-cache strategies read the file warm (it was just written), the
// `O_DIRECT` ones read it from the device. The groups therefore compare the
// per-read overhead of the strategies, not device throughput.
class IoUringDirectBenchmark : public BenchmarkInterface {
 public:
  std::string name() const override {
    return "io_uring registered files, registered buffers and O_DIRECT";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results;
    const TemporaryRandomFile benchmarkFile;
    for (const BatchOrder order :
         {BatchOrder::FileOrder, BatchOrder::Shuffled}) {
      ResultGroup& strategyGroup = results.addGroup(absl::StrCat(
          "4 KiB block reads of a 1 GiB file, batches in ",
          order == BatchOrder::FileOrder ? "file order" : "shuffled order"));
      for (const ReadStrategy& strategy : kReadStrategies) {
        size_t numBytesRead = 0;
        ResultEntry& measurement =
            strategyGroup.addMeasurement(std::string{strategy.label}, [&] {
              numBytesRead = readFile(strategy, order, benchmarkFile.path());
            });
        AD_CORRECTNESS_CHECK(numBytesRead == kFileSizeBytes);
        measurement.metadata().addKeyValuePair("bytesRead", numBytesRead);
      }
    }
    return results;
  }
};

AD_REGISTER_BENCHMARK(IoUringDirectBenchmark);

}  // namespace ad_benchmark

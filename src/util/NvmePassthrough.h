// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_NVMEPASSTHROUGH_H
#define QLEVER_SRC_UTIL_NVMEPASSTHROUGH_H

#include <sys/ioctl.h>
#include <sys/stat.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
// The NVMe command layout for `IORING_OP_URING_CMD` comes from the kernel
// headers. When they are unavailable, only the translation, planning and
// probing helpers below are compiled; SQE preparation is compiled out (see
// `kUringCmdSupported`), so every read keeps the plain path. The header alone
// is not enough: older kernel headers ship `linux/nvme_ioctl.h` without the
// `uring_cmd` interface, and older liburing versions lack the 128-byte SQE and
// 32-byte CQE ring flags that the NVMe driver requires, so the feature macro
// also requires those symbols.
#if defined(__has_include)
#if __has_include(<linux/nvme_ioctl.h>)
#include <linux/nvme_ioctl.h>
#if defined(NVME_URING_CMD_IO) && defined(IORING_SETUP_SQE128) && \
    defined(IORING_SETUP_CQE32)
#define QLEVER_HAS_NVME_URING_CMD 1
#endif
#endif
#endif
#endif

namespace ad_utility::nvmePassthrough {

// Options for NVMe passthrough reads. Disabled by default: unless an operator
// explicitly enables passthrough after validating their device and kernel
// combination, every read uses the plain block-layer path.
struct Options {
  bool enabled = false;
  // NVMe namespace id that the data device exposes (must be nonzero when
  // `enabled`).
  uint32_t namespaceId = 0;
  // Logical block size of the namespace in bytes, e.g. 512 or 4096 (must be
  // nonzero when `enabled`).
  uint32_t logicalBlockSize = 0;
};

// True iff this build can prepare a passthrough SQE, i.e. io_uring support is
// compiled in and the kernel headers provide the NVMe `uring_cmd` layout. This
// is a compile-time constant, so the unsupported branch of the per-request
// routing folds away.
#ifdef QLEVER_HAS_NVME_URING_CMD
inline constexpr bool kUringCmdSupported = true;
#else
inline constexpr bool kUringCmdSupported = false;
#endif

// NVMe NVM command-set opcode for Read (02h; 01h is Write and must never be
// submitted by the read path: a successful write completion carries res == 0
// exactly like a read, so the inversion would be silent and destroy the
// namespace contents).
inline constexpr uint8_t kNvmReadOpcode = 0x02;

// Bytes of command payload a 128-byte SQE (`IORING_SETUP_SQE128`) provides
// after the fixed 48-byte SQE prefix.
inline constexpr size_t kUringCmdDataSize = 80;

// Resolved NVMe addressing for a single read: the namespace, the starting
// logical block address, the 0-based block count for CDW12 (0 means one
// block), and the byte length the device will transfer.
struct ReadParams {
  uint32_t namespaceId;
  uint64_t startLba;
  uint32_t numBlocksZeroBased;
  uint32_t transferBytes;
};

// Maximum number of blocks of a single NVMe read (2^16): CDW12 carries the
// 0-based count in its low 16 bits, so its largest value 0xFFFF means 2^16
// blocks.
inline constexpr uint64_t kMaxBlocksPerRead = 0x10000;

// Translate (`fileOffset`, `numBytes`) into NVMe addressing for a namespace
// whose logical blocks are `logicalBlockSize` bytes wide and whose LBA
// `lbaBase` corresponds to file offset 0 (the file image sits linearly in the
// namespace, as when the vocabulary words file was written contiguously to a
// raw namespace). Return `std::nullopt` when the read cannot be expressed as
// whole blocks (unaligned offset or length, empty read, more than 2^16 blocks,
// a transfer length that does not fit 32 bits, or an LBA translation that
// would overflow), in which case the caller must use the plain block-layer
// read path, so the bytes read stay identical.
inline std::optional<ReadParams> translateToReadParams(
    uint64_t fileOffset, size_t numBytes, uint32_t namespaceId,
    uint32_t logicalBlockSize, uint64_t lbaBase = 0) noexcept {
  if (namespaceId == 0 || logicalBlockSize == 0 || numBytes == 0) {
    return std::nullopt;
  }
  if (fileOffset % logicalBlockSize != 0 || numBytes % logicalBlockSize != 0) {
    return std::nullopt;
  }
  const uint64_t numBlocks = static_cast<uint64_t>(numBytes) / logicalBlockSize;
  if (numBlocks == 0 || numBlocks > kMaxBlocksPerRead) {
    return std::nullopt;
  }
  if (numBytes > 0xFFFFFFFFULL) {
    return std::nullopt;
  }
  const uint64_t offsetLba = fileOffset / logicalBlockSize;
  if (offsetLba > std::numeric_limits<uint64_t>::max() - lbaBase) {
    return std::nullopt;
  }
  return ReadParams{namespaceId, lbaBase + offsetLba,
                    static_cast<uint32_t>(numBlocks - 1),
                    static_cast<uint32_t>(numBytes)};
}

// Coalescing granularity for `planBlockReads`. Runs built from 512-byte blocks
// are in general not aligned to larger logical blocks, so the coalesced path
// only supports namespaces with 512-byte logical blocks.
inline constexpr uint64_t kCoalesceBlockSize = 512;

// Maximum run length for `planBlockReads`: 256 blocks (128 KiB, the default
// readahead window of the Linux block layer). Bounds the staging buffer per
// run and keeps every run far below `kMaxBlocksPerRead`, so a run always
// translates to exactly one command.
inline constexpr uint64_t kCoalesceMaxRunBlocks = 256;

// A block-aligned read plan for a batch of byte ranges ("words"). The `runs`
// cover every input range with whole blocks (sorted, disjoint; runs may
// include swallowed gap blocks) and are read into one staging buffer in order;
// `slices[i]` locates input word `i` inside that staging buffer. Zero-length
// words cover no block and slice to `{0, 0}`; the caller skips their copy.
struct BlockReadPlan {
  struct Run {
    uint64_t fileOffset;
    size_t numBytes;
    size_t stagingOffset;
  };
  struct Slice {
    size_t stagingOffset;
    size_t numBytes;
  };
  std::vector<Run> runs;
  std::vector<Slice> slices;
  size_t stagingBytes = 0;
};

// Plan whole-block reads for the words `fileOffsets[i]`/`sizes[i]`. Blocks that
// are contiguous merge into one run; additionally, gaps of at most
// `maxGapBlocks` uncovered blocks between needed blocks are swallowed into the
// run (software readahead: one command covers near-neighbor words). Runs stop
// before exceeding `kCoalesceMaxRunBlocks`, so staging stays bounded and every
// run translates to a single NVMe command. When `readLimit` is set (the end of
// a regular file, or the size of a device), the last run is shortened to end
// there, so it does not ask for bytes that cannot be read. Every word must then
// end at or before `readLimit` (checked).
inline BlockReadPlan planBlockReads(
    ql::span<const uint64_t> fileOffsets, ql::span<const size_t> sizes,
    uint64_t maxGapBlocks = 0,
    std::optional<uint64_t> readLimit = std::nullopt) {
  AD_CONTRACT_CHECK(fileOffsets.size() == sizes.size());
  BlockReadPlan plan;
  std::vector<uint64_t> firstBlocks(sizes.size());
  std::vector<uint64_t> lastBlocks(sizes.size());
  std::vector<uint64_t> blocks;
  for (size_t i = 0; i < sizes.size(); ++i) {
    if (sizes[i] == 0) {
      // Empty marker (`last < first`); the slice is appended in order in the
      // second pass below.
      firstBlocks[i] = 1;
      lastBlocks[i] = 0;
      continue;
    }
    AD_CONTRACT_CHECK(fileOffsets[i] <=
                      std::numeric_limits<uint64_t>::max() - sizes[i]);
    // A word past the read limit would slice unread staging bytes, so it must
    // fail here instead of returning wrong bytes.
    AD_CONTRACT_CHECK(!readLimit.has_value() ||
                      fileOffsets[i] + sizes[i] <= readLimit.value());
    const uint64_t first = fileOffsets[i] / kCoalesceBlockSize;
    const uint64_t last = (fileOffsets[i] + sizes[i] - 1) / kCoalesceBlockSize;
    firstBlocks[i] = first;
    lastBlocks[i] = last;
    for (uint64_t b = first; b <= last; ++b) {
      blocks.push_back(b);
    }
  }
  std::sort(blocks.begin(), blocks.end());
  blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());
  // Merge blocks into runs and assign staging offsets. A run extends over
  // needed blocks and swallows gaps of at most `maxGapBlocks` uncovered blocks,
  // stopping before it would exceed `kCoalesceMaxRunBlocks`.
  std::vector<uint64_t> runFirstBlocks;
  size_t stagingBytes = 0;
  for (size_t i = 0; i < blocks.size();) {
    const uint64_t runFirst = blocks[i];
    uint64_t runLast = runFirst;
    size_t j = i;
    while (j + 1 < blocks.size() &&
           blocks[j + 1] - runLast - 1 <= maxGapBlocks &&
           blocks[j + 1] - runFirst + 1 <= kCoalesceMaxRunBlocks) {
      ++j;
      runLast = blocks[j];
    }
    runFirstBlocks.push_back(runFirst);
    const size_t runBytes =
        static_cast<size_t>(runLast - runFirst + 1) * kCoalesceBlockSize;
    plan.runs.push_back(
        {runFirst * kCoalesceBlockSize, runBytes, stagingBytes});
    stagingBytes += runBytes;
    i = j + 1;
  }
  plan.stagingBytes = stagingBytes;
  // Runs are sorted and every word ends at or before `readLimit`, so only the
  // last run can extend past it.
  if (readLimit.has_value() && !plan.runs.empty()) {
    auto& lastRun = plan.runs.back();
    AD_CORRECTNESS_CHECK(lastRun.fileOffset < readLimit.value());
    lastRun.numBytes =
        std::min(lastRun.numBytes,
                 static_cast<size_t>(readLimit.value() - lastRun.fileOffset));
  }
  // Exactly one slice per input word, in input order.
  plan.slices.reserve(sizes.size());
  for (size_t i = 0; i < sizes.size(); ++i) {
    if (lastBlocks[i] < firstBlocks[i]) {
      plan.slices.push_back({0, 0});
      continue;
    }
    const size_t run = std::upper_bound(runFirstBlocks.begin(),
                                        runFirstBlocks.end(), firstBlocks[i]) -
                       runFirstBlocks.begin() - 1;
    const auto& coveringRun = plan.runs[run];
    plan.slices.push_back(
        {coveringRun.stagingOffset +
             static_cast<size_t>(fileOffsets[i] - coveringRun.fileOffset),
         sizes[i]});
  }
  return plan;
}

// The median distance in bytes between the words `fileOffsets[i]`/`sizes[i]`
// of a batch, in file order: after sorting the non-empty words by offset, the
// gap after a word is the number of bytes between its end (the largest end
// seen so far, so overlapping or duplicate words have gap zero) and the start
// of the next word. Return `std::nullopt` if the batch has fewer than two
// non-empty words, so there is no gap.
inline std::optional<uint64_t> medianGapBytes(
    ql::span<const uint64_t> fileOffsets, ql::span<const size_t> sizes) {
  AD_CONTRACT_CHECK(fileOffsets.size() == sizes.size());
  std::vector<std::pair<uint64_t, uint64_t>> ranges;
  ranges.reserve(sizes.size());
  for (size_t i = 0; i < sizes.size(); ++i) {
    if (sizes[i] != 0) {
      ranges.emplace_back(fileOffsets[i], fileOffsets[i] + sizes[i]);
    }
  }
  if (ranges.size() < 2) {
    return std::nullopt;
  }
  std::sort(ranges.begin(), ranges.end());
  std::vector<uint64_t> gaps;
  gaps.reserve(ranges.size() - 1);
  uint64_t end = ranges.front().second;
  for (size_t i = 1; i < ranges.size(); ++i) {
    const auto& [begin, nextEnd] = ranges[i];
    gaps.push_back(begin > end ? begin - end : 0);
    end = std::max(end, nextEnd);
  }
  auto middle = gaps.begin() + static_cast<std::ptrdiff_t>(gaps.size() / 2);
  std::nth_element(gaps.begin(), middle, gaps.end());
  return *middle;
}

// The name of the NVMe block device (`nvmeXnY`) that exposes the same
// namespace as the NVMe generic character device `ngXnY`, or `std::nullopt` if
// `charDeviceName` does not have that form. The kernel names both nodes of a
// namespace with the same controller instance `X` and namespace index `Y`.
inline std::optional<std::string> blockDeviceNameForGenericCharDevice(
    std::string_view charDeviceName) {
  auto isDigits = [](std::string_view s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), [](char c) {
      return c >= '0' && c <= '9';
    });
  };
  constexpr std::string_view prefix = "ng";
  if (charDeviceName.substr(0, prefix.size()) != prefix) {
    return std::nullopt;
  }
  const std::string_view rest = charDeviceName.substr(prefix.size());
  const size_t n = rest.find('n');
  if (n == std::string_view::npos || !isDigits(rest.substr(0, n)) ||
      !isDigits(rest.substr(n + 1))) {
    return std::nullopt;
  }
  return std::string{"nvme"} + std::string{rest};
}

// The namespace id that the NVMe device (generic character device or block
// device) behind `fd` reports via `NVME_IOCTL_ID`, or `std::nullopt` if `fd`
// is not an NVMe namespace device (or this build lacks the NVMe `uring_cmd`
// headers). Never throws.
inline std::optional<uint32_t> nvmeNamespaceIdOf(
    [[maybe_unused]] int fd) noexcept {
#ifdef QLEVER_HAS_NVME_URING_CMD
  const int reportedNamespaceId = ::ioctl(fd, NVME_IOCTL_ID);
  if (reportedNamespaceId > 0) {
    return static_cast<uint32_t>(reportedNamespaceId);
  }
#endif
  return std::nullopt;
}

// True iff `fd` may receive NVMe passthrough reads: it must be a character
// device (`uring_cmd` passthrough submits native NVMe commands to an NVMe
// generic character device `/dev/ngXnY`, never to a regular file) that reports
// the configured `namespaceId`. The identity check rejects other character
// devices (such as `/dev/null`), which keep the plain read path, and a
// misconfigured namespace id, so passthrough never reads another namespace's
// blocks. Never throws: any failure means "not capable", so a failed probe
// disables passthrough for the fd without failing the batch.
inline bool isPassthroughCandidate(int fd, uint32_t namespaceId) noexcept {
  struct stat sb {};
  if (::fstat(fd, &sb) != 0 || S_ISCHR(sb.st_mode) == 0) {
    return false;
  }
  return nvmeNamespaceIdOf(fd) == std::optional<uint32_t>{namespaceId};
}

#ifdef QLEVER_HAS_NVME_URING_CMD
// Copy `n` bytes to the SQE128 command tail. Not inlined: FORTIFY would
// otherwise bound the destination by the 64-byte `io_uring_sqe` type, while the
// bytes live in the 128-byte ring slot (or a 128-byte test buffer).
[[gnu::noinline]] inline void copyIntoSqe128Tail(io_uring_sqe* sqe,
                                                 const unsigned char* src,
                                                 size_t n) noexcept {
  auto* dst =
      reinterpret_cast<unsigned char*>(sqe) + offsetof(io_uring_sqe, cmd);
  for (size_t i = 0; i < n; ++i) {
    dst[i] = src[i];
  }
}

// Prepare `sqe` (claimed via `io_uring_get_sqe` from a ring created with
// `IORING_SETUP_SQE128`; the 80-byte command area only exists there) as a
// native NVMe read submitted to `deviceFd` (an NVMe generic character device).
// The caller keeps the request-id tagging (`user_data`) and the in-flight
// bookkeeping exactly as for a plain read, so completion handling is shared.
inline void preparePassthroughRead(io_uring_sqe* sqe, int deviceFd,
                                   const ReadParams& params,
                                   void* targetBuffer) {
  AD_CORRECTNESS_CHECK(sqe != nullptr);
  AD_CORRECTNESS_CHECK(targetBuffer != nullptr);
  static_assert(sizeof(struct nvme_uring_cmd) <= kUringCmdDataSize,
                "nvme_uring_cmd must fit the SQE command area");
  // Same discipline as `io_uring_prep_read`: set every field the submission
  // owns. The NVMe read command travels in the SQE's command area, where the
  // kernel's NVMe driver picks it up. The target buffer's address is carried
  // in the NVMe command (`addr`) with the same lifetime requirement as a plain
  // read's buffer: it must stay valid until the completion is reaped.
  sqe->opcode = IORING_OP_URING_CMD;
  sqe->fd = deviceFd;
  sqe->off = 0;
  sqe->addr = 0;
  sqe->len = 0;
  sqe->cmd_op = NVME_URING_CMD_IO;
  struct nvme_uring_cmd cmd {};
  cmd.opcode = kNvmReadOpcode;
  cmd.nsid = params.namespaceId;
  cmd.addr = reinterpret_cast<__u64>(targetBuffer);
  cmd.data_len = params.transferBytes;
  cmd.cdw10 = static_cast<__u32>(params.startLba & 0xFFFFFFFFULL);
  cmd.cdw11 = static_cast<__u32>(params.startLba >> 32);
  cmd.cdw12 = params.numBlocksZeroBased;
  // `sqe->cmd` is a zero-length array at the end of the 64-byte SQE type; the
  // 80 command bytes sit in the SQE128 slot past that type. A direct memset of
  // `sqe->cmd` is a FORTIFY overflow on any object the compiler can see is
  // only `sizeof(io_uring_sqe)`.
  unsigned char area[kUringCmdDataSize]{};
  std::memcpy(area, &cmd, sizeof(cmd));
  copyIntoSqe128Tail(sqe, area, kUringCmdDataSize);
}
#endif

}  // namespace ad_utility::nvmePassthrough

#endif  // QLEVER_SRC_UTIL_NVMEPASSTHROUGH_H

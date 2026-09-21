// Copyright 2026, The QLever Authors
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_NVMEPASSTHROUGH_H
#define QLEVER_SRC_UTIL_NVMEPASSTHROUGH_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <sys/stat.h>

#include "util/Exception.h"

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
// The NVMe command layout for `IORING_OP_URING_CMD` comes from the kernel
// headers. When they are unavailable, only the translation and probing helpers
// below are compiled; SQE preparation is compiled out (see
// `kUringCmdSupported`), so every read keeps the plain path.
#if defined(__has_include)
#if __has_include(<linux/nvme_ioctl.h>)
#include <linux/nvme_ioctl.h>
#define QLEVER_HAS_NVME_URING_CMD 1
#endif
#endif
#endif

namespace ad_utility::nvmePassthrough {

// Runtime options for NVMe passthrough reads. Disabled by default: unless an
// operator explicitly enables passthrough after validating their device and
// kernel combination, every read uses the plain block-layer path.
struct Options {
  bool enabled = false;
  // NVMe namespace id that the data device exposes (must be nonzero when
  // enabled).
  uint32_t namespaceId = 0;
  // Logical block size of the namespace in bytes, e.g. 512 or 4096 (must be
  // nonzero when enabled).
  uint32_t logicalBlockSize = 0;
};

// True iff this build can even prepare a passthrough SQE, i.e. io_uring
// support is compiled in and the kernel headers provide the NVMe `uring_cmd`
// layout. This is a compile-time constant, so the unsupported branch of the
// per-request routing folds away.
#ifdef QLEVER_HAS_NVME_URING_CMD
inline constexpr bool kUringCmdSupported = true;
#else
inline constexpr bool kUringCmdSupported = false;
#endif

// NVMe NVM command-set opcode for Read (01h; 02h would be Write, which stays
// out of scope: reads only).
inline constexpr uint8_t kNvmReadOpcode = 0x01;

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

// Maximum block count of a single NVMe read: CDW12 carries the 0-based count
// in its low 16 bits.
inline constexpr uint64_t kMaxBlocksPerRead = 0x10000;

// Translate (`fileOffset`, `numBytes`) into NVMe addressing for a namespace
// whose logical blocks are `logicalBlockSize` bytes wide and whose LBA
// `lbaBase` corresponds to file offset 0 (the file image sits linearly in the
// namespace, as on a rig where the vocabulary image was written contiguously
// to a raw namespace). Returns `std::nullopt` when the read cannot be
// expressed as whole blocks (unaligned offset or length, empty read, more
// than 2^16 blocks, or a transfer length that does not fit 32 bits), in which
// case the caller must use the plain block-layer read path, so the bytes read
// stay identical.
inline std::optional<ReadParams> translateToReadParams(
    uint64_t fileOffset, size_t numBytes, uint32_t namespaceId,
    uint32_t logicalBlockSize, uint64_t lbaBase = 0) {
  if (namespaceId == 0 || logicalBlockSize == 0 || numBytes == 0) {
    return std::nullopt;
  }
  if (fileOffset % logicalBlockSize != 0 ||
      numBytes % logicalBlockSize != 0) {
    return std::nullopt;
  }
  const uint64_t numBlocks =
      static_cast<uint64_t>(numBytes) / logicalBlockSize;
  if (numBlocks == 0 || numBlocks > kMaxBlocksPerRead) {
    return std::nullopt;
  }
  if (numBytes > 0xFFFFFFFFULL) {
    return std::nullopt;
  }
  return ReadParams{namespaceId, lbaBase + fileOffset / logicalBlockSize,
                    static_cast<uint32_t>(numBlocks - 1),
                    static_cast<uint32_t>(numBytes)};
}

// True iff `fd` is a character device. This is the necessary (but not
// sufficient) condition for NVMe passthrough: `uring_cmd` passthrough submits
// native NVMe commands to an NVMe character device (`/dev/ngXnY`), never to a
// regular file. Regular vocabulary files therefore always fail this probe and
// keep the plain read path. Never throws: any `fstat` failure means "not
// capable", so a failed probe disables passthrough for the fd without failing
// the batch.
inline bool isPassthroughCandidate(int fd) noexcept {
  struct stat sb{};
  if (::fstat(fd, &sb) != 0) {
    return false;
  }
  return S_ISCHR(sb.st_mode) != 0;
}

#ifdef QLEVER_HAS_NVME_URING_CMD
// Prepare `sqe` (claimed via `io_uring_get_sqe` from a ring created with
// `IORING_SETUP_SQE128`; the 80-byte command area only exists there) as a
// native NVMe read submitted to `deviceFd` (an NVMe character device). The
// caller keeps the request-id tagging (`user_data`) and the in-flight
// bookkeeping exactly as for a plain read, so completion handling is shared.
inline void preparePassthroughRead(io_uring_sqe* sqe, int deviceFd,
                                   const ReadParams& params,
                                   void* targetBuffer) {
  AD_CORRECTNESS_CHECK(sqe != nullptr);
  AD_CORRECTNESS_CHECK(targetBuffer != nullptr);
  static_assert(sizeof(struct nvme_uring_cmd) <= kUringCmdDataSize,
                "nvme_uring_cmd must fit the SQE command area");
  // Same discipline as `io_uring_prep_read`: set every field the submission
  // owns. The SQE body follows liburing's `uring_cmd` preparation (opcode, fd,
  // command operation); the NVMe read command itself travels in the SQE's
  // command area, where the kernel's NVMe driver picks it up. The target
  // buffer's address is carried in the NVMe command (`addr`) with the same
  // lifetime requirement as a plain read's buffer: it must stay valid until
  // the completion is reaped.
  sqe->opcode = IORING_OP_URING_CMD;
  sqe->fd = deviceFd;
  sqe->off = 0;
  sqe->addr = 0;
  sqe->len = 0;
  sqe->cmd_op = NVME_URING_CMD_IO;
  struct nvme_uring_cmd cmd{};
  cmd.opcode = kNvmReadOpcode;
  cmd.nsid = params.namespaceId;
  cmd.addr = reinterpret_cast<__u64>(targetBuffer);
  cmd.data_len = params.transferBytes;
  cmd.cdw10 = static_cast<__u32>(params.startLba & 0xFFFFFFFFULL);
  cmd.cdw11 = static_cast<__u32>(params.startLba >> 32);
  cmd.cdw12 = params.numBlocksZeroBased;
  std::memset(sqe->cmd, 0, kUringCmdDataSize);
  std::memcpy(sqe->cmd, &cmd, sizeof(cmd));
}
#endif

}  // namespace ad_utility::nvmePassthrough

#endif  // QLEVER_SRC_UTIL_NVMEPASSTHROUGH_H

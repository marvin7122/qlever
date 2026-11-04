// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/io_uring/RingReadProbe.h"

#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "util/Exception.h"
#include "util/Log.h"

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
// See `IoUringManager.h`: `liburing.h` pulls in a `BLOCK_SIZE` macro.
#undef BLOCK_SIZE
#endif

namespace ad_utility {

namespace {
// The number of bytes that the probe reads: the first page of the file, or the
// whole file if it is smaller.
constexpr size_t probeSize = 4096;

#if defined(QLEVER_HAS_IO_URING) && defined(RWF_NOWAIT)
// `NowaitRingRead` with a ring of its own, set up like `IoUringPolicy`'s (no
// setup flags), so that the probe sees the same submission path.
class LiburingNowaitRead : public NowaitRingRead {
 public:
  LiburingNowaitRead() {
    if (io_uring_queue_init(2, &ring_, 0) != 0) {
      throw std::runtime_error{"io_uring_queue_init failed in the ring probe"};
    }
  }
  ~LiburingNowaitRead() override { io_uring_queue_exit(&ring_); }
  LiburingNowaitRead(const LiburingNowaitRead&) = delete;
  LiburingNowaitRead& operator=(const LiburingNowaitRead&) = delete;

  int read(int fd, char* buffer, size_t numberOfBytes,
           uint64_t offset) override {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    AD_CORRECTNESS_CHECK(sqe != nullptr);
    io_uring_prep_read(sqe, fd, buffer, static_cast<unsigned>(numberOfBytes),
                       offset);
    sqe->rw_flags = RWF_NOWAIT;
    if (int submitted = io_uring_submit(&ring_); submitted != 1) {
      return submitted < 0 ? submitted : -EIO;
    }
    io_uring_cqe* cqe = nullptr;
    if (int ret = io_uring_wait_cqe(&ring_, &cqe); ret < 0) {
      return ret;
    }
    const int result = cqe->res;
    io_uring_cqe_seen(&ring_, cqe);
    return result;
  }

 private:
  io_uring ring_{};
};
#endif

//______________________________________________________________________________
std::string_view describe(CachedRingRead cachedRingRead) {
  switch (cachedRingRead) {
    case CachedRingRead::Inline:
      return "inline";
    case CachedRingRead::Punted:
      return "handed to io-wq worker threads";
    case CachedRingRead::Unknown:
      return "unknown";
  }
  AD_FAIL();
}
}  // namespace

//______________________________________________________________________________
CachedRingRead probeCachedRingRead(NowaitRingRead& ring, int fd,
                                   size_t numberOfTrials) {
  struct stat fileStatus {};
  if (fstat(fd, &fileStatus) != 0 || fileStatus.st_size <= 0) {
    return CachedRingRead::Unknown;
  }
  const size_t numberOfBytes =
      std::min(probeSize, static_cast<size_t>(fileStatus.st_size));
  std::vector<char> buffer(numberOfBytes);
  // Bring the page into the page cache.
  if (pread(fd, buffer.data(), numberOfBytes, 0) !=
      static_cast<ssize_t>(numberOfBytes)) {
    return CachedRingRead::Unknown;
  }
  for ([[maybe_unused]] size_t trial = 0; trial < numberOfTrials; ++trial) {
    const int result = ring.read(fd, buffer.data(), numberOfBytes, 0);
    if (result > 0) {
      return CachedRingRead::Inline;
    }
    if (result != -EAGAIN) {
      return CachedRingRead::Unknown;
    }
  }
  return CachedRingRead::Punted;
}

//______________________________________________________________________________
CachedRingRead cachedRingReadOfDevice(int fd) {
  struct stat fileStatus {};
  if (fstat(fd, &fileStatus) != 0) {
    return CachedRingRead::Unknown;
  }
  static std::mutex mutex;
  static std::unordered_map<dev_t, CachedRingRead> resultPerDevice;
  std::lock_guard lock{mutex};
  if (auto it = resultPerDevice.find(fileStatus.st_dev);
      it != resultPerDevice.end()) {
    return it->second;
  }
  CachedRingRead result = CachedRingRead::Unknown;
#if defined(QLEVER_HAS_IO_URING) && defined(RWF_NOWAIT)
  try {
    LiburingNowaitRead ring;
    result = probeCachedRingRead(ring, fd);
  } catch (const std::exception& e) {
    AD_LOG_INFO << "The io_uring probe for page-cache hits failed: " << e.what()
                << std::endl;
  }
#endif
  resultPerDevice.emplace(fileStatus.st_dev, result);
  AD_LOG_INFO << "io_uring reads of page-cache hits on device "
              << fileStatus.st_dev << ": " << describe(result)
              << "; the page-cache fast path is "
              << (usePageCacheFastPath(
                      PageCacheFastPathMode{PageCacheFastPathMode::Enum::Auto},
                      result)
                      ? "on"
                      : "off")
              << " by default (`vocabulary-page-cache-fast-path=auto`)"
              << std::endl;
  return result;
}

//______________________________________________________________________________
bool usePageCacheFastPath(PageCacheFastPathMode mode,
                          CachedRingRead cachedRingRead) {
  switch (mode.value()) {
    case PageCacheFastPathMode::Enum::On:
      return true;
    case PageCacheFastPathMode::Enum::Off:
      return false;
    case PageCacheFastPathMode::Enum::Auto:
      return cachedRingRead != CachedRingRead::Inline;
  }
  AD_FAIL();
}

}  // namespace ad_utility

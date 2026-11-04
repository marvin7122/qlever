// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_IO_URING_RINGREADPROBE_H
#define QLEVER_SRC_UTIL_IO_URING_RINGREADPROBE_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "util/EnumWithStrings.h"

namespace ad_utility {

namespace detail {
enum class PageCacheFastPathModeEnum { Auto, On, Off };
}  // namespace detail

// Whether the vocabulary reads try the page cache with `preadv2(RWF_NOWAIT)`
// before the batch manager (`PageCacheFirstPolicy`): always (`on`), never
// (`off`), or only where the io_uring ring cannot serve page-cache hits without
// a worker thread (`auto`, see `probeCachedRingRead`).
class PageCacheFastPathMode
    : public EnumWithStrings<PageCacheFastPathMode,
                             detail::PageCacheFastPathModeEnum> {
 public:
  using Enum = detail::PageCacheFastPathModeEnum;
  static constexpr std::array<std::pair<Enum, std::string_view>, 3>
      descriptions_{
          {{Enum::Auto, "auto"}, {Enum::On, "on"}, {Enum::Off, "off"}}};
  static constexpr std::string_view typeName() {
    return "page-cache fast-path mode";
  }
  using EnumWithStrings::EnumWithStrings;

  // `fromString`, but `true` and `false`, the values from the time when the
  // parameter was a boolean, are accepted as `on` and `off`, so that existing
  // configurations keep working.
  struct FromStringWithBooleans {
    PageCacheFastPathMode operator()(const std::string& s) const {
      if (s == "true") {
        return PageCacheFastPathMode{Enum::On};
      }
      if (s == "false") {
        return PageCacheFastPathMode{Enum::Off};
      }
      return fromString(s);
    }
  };
};

// How an io_uring read of data that is in the page cache is carried out on this
// kernel and file system.
enum class CachedRingRead {
  // The ring copies the data while the read is submitted, so a page-cache hit
  // costs no more than the submission and the completion.
  Inline,
  // The ring cannot read the file without blocking, so it hands every read,
  // page-cache hits included, to an io-wq worker thread (`iou-wrk-*`).
  Punted,
  // The probe could not tell (no io_uring, an empty file, `RWF_NOWAIT` not
  // supported for the file, or another error).
  Unknown
};

// The one read of the probe: an io_uring read of `numberOfBytes` bytes at
// `offset` of `fd` into `buffer` with `RWF_NOWAIT` in `sqe->rw_flags`, from
// submission to completion. Returns the `res` of the completion: the number of
// bytes read, or `-errno`. Tests fake it.
class NowaitRingRead {
 public:
  virtual ~NowaitRingRead() = default;
  virtual int read(int fd, char* buffer, size_t numberOfBytes,
                   uint64_t offset) = 0;
};

// Find out how the ring reads a page-cache hit of `fd`. First read the first
// page of the file with a blocking `pread`, so that it is in the page cache.
// Then read it with up to `numberOfTrials` calls of `ring.read`.
//
// The probe is deterministic because of `RWF_NOWAIT`: a read that the ring
// would have handed to a worker thread fails with `-EAGAIN` instead (the
// request is marked `REQ_F_NOWAIT`, and `-EAGAIN` with that flag completes the
// request instead of queueing it to io-wq). So a positive result means that the
// ring read the cached page inline; `-EAGAIN` in every trial means that it
// cannot (or, much less likely, that the page was evicted between the trials).
// Any other result gives `Unknown`.
CachedRingRead probeCachedRingRead(NowaitRingRead& ring, int fd,
                                   size_t numberOfTrials = 4);

// `probeCachedRingRead` with a small io_uring ring of its own, at most once per
// device (`st_dev` of `fd`) and process, and logged. `Unknown` where io_uring
// is not compiled in or cannot be set up.
CachedRingRead cachedRingReadOfDevice(int fd);

// Whether to use the page-cache fast path for `mode`, given the probe result
// `cachedRingRead`. `auto` uses it unless the ring reads page-cache hits
// inline: then a hit costs the ring little, and one `preadv2` per run of reads
// costs more (measured on Linux 6.8).
bool usePageCacheFastPath(PageCacheFastPathMode mode,
                          CachedRingRead cachedRingRead);

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IO_URING_RINGREADPROBE_H

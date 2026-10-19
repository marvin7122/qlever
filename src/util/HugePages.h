// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_HUGEPAGES_H
#define QLEVER_SRC_UTIL_HUGEPAGES_H

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace ad_utility {

// The selected mode of transparent huge pages ("always", "madvise" or
// "never"), read from `/sys/kernel/mm/transparent_hugepage/enabled`, or
// "unknown" if that file cannot be read.
std::string transparentHugePagesMode();

// The selected mode in the content of the file above, for example "madvise"
// for "always [madvise] never". Returns "unknown" if no mode is selected.
std::string selectedTransparentHugePagesMode(std::string_view content);

// An upper bound on the number of bytes backed by anonymous transparent
// huge pages (`AnonHugePages` in `/proc/self/smaps`) inside `[begin, begin +
// size)`: each overlapping mapping contributes at most its intersection
// with the range. Returns `std::nullopt` if `/proc/self/smaps` cannot be
// read.
std::optional<size_t> anonHugePageBytes(const void* begin, size_t size);

// The result of `adviseHugePages`: the number of bytes of the 2 MiB-aligned
// part of the range that was advised, and the `errno` of a failed
// `madvise(MADV_HUGEPAGE)` or `madvise(MADV_COLLAPSE)` (0 on success or if the
// call was not made).
struct HugePageAdvice {
  size_t alignedBytes = 0;
  int hugePageErrno = 0;
  int collapseErrno = 0;
};

// Mark the 2 MiB-aligned part of the existing memory `[begin, begin + size)`
// with `madvise(MADV_HUGEPAGE)` and, if `collapse` is set, collapse it into
// transparent huge pages right away with `madvise(MADV_COLLAPSE)` (Linux 6.1
// or newer). `MADV_COLLAPSE` copies the already populated small pages into
// huge pages; it ignores the system-wide THP mode (see the kernel's
// Documentation/admin-guide/mm/transhuge.rst), so it also works in "madvise"
// and "never" mode. Only a hint: if a call fails, the memory simply stays (in
// part) on regular pages, which the returned `errno` values report.
HugePageAdvice adviseHugePages(const void* begin, size_t size, bool collapse);

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HUGEPAGES_H

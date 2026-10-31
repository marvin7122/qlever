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

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HUGEPAGES_H

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/HugePages.h"

#include <cstdint>
#include <fstream>
#include <sstream>

namespace ad_utility {

// _____________________________________________________________________________
std::string selectedTransparentHugePagesMode(std::string_view content) {
  const auto open = content.find('[');
  const auto close = content.find(']', open);
  if (open == std::string_view::npos || close == std::string_view::npos) {
    return "unknown";
  }
  return std::string{content.substr(open + 1, close - open - 1)};
}

// _____________________________________________________________________________
std::string transparentHugePagesMode() {
  std::ifstream file{"/sys/kernel/mm/transparent_hugepage/enabled"};
  std::stringstream content;
  content << file.rdbuf();
  return file ? selectedTransparentHugePagesMode(content.str()) : "unknown";
}

// _____________________________________________________________________________
std::optional<size_t> anonHugePageBytes(const void* begin, size_t size) {
  std::ifstream smaps{"/proc/self/smaps"};
  if (!smaps) {
    return std::nullopt;
  }
  const auto rangeBegin = reinterpret_cast<uintptr_t>(begin);
  const uintptr_t rangeEnd = rangeBegin + size;
  bool inOverlappingMapping = false;
  size_t result = 0;
  std::string line;
  while (std::getline(smaps, line)) {
    // A mapping starts with a line `start-end perms ...` (hex addresses).
    uintptr_t start = 0;
    uintptr_t end = 0;
    char dash = 0;
    std::istringstream header{line};
    if (header >> std::hex >> start >> dash >> end && dash == '-') {
      inOverlappingMapping = start < rangeEnd && rangeBegin < end;
      continue;
    }
    constexpr std::string_view key = "AnonHugePages:";
    if (inOverlappingMapping && line.rfind(key, 0) == 0) {
      std::istringstream value{line.substr(key.size())};
      size_t kiloBytes = 0;
      if (value >> kiloBytes) {
        result += kiloBytes * 1024;
      }
    }
  }
  return result;
}

}  // namespace ad_utility

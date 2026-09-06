// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_STRING_H
#define QLEVER_SRC_BACKPORTS_STRING_H

#include <string>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include <cstddef>

/**
 * @brief Resize a string and overwrite its contents via a user-provided operation.
 *
 * This function provides a C++17-compatible backport of C++23's std::basic_string::resize_and_overwrite.
 * 
 * @tparam CharT   Character type of the string.
 * @tparam Traits  Traits type (defaults to std::char_traits<CharT>).
 * @tparam Allocator Allocator type (defaults to std::allocator<CharT>).
 * @tparam Operation Callable that receives a pointer to a buffer and a size, and returns the number of characters written.
 * @param str      The string to resize.
 * @param count    Desired size after operation.
 * @param op       Operation callable; must be invocable as size_t op(CharT* buffer, size_t count).
 *                 The operation must not write more than count characters.
 * @post str.size() == newSize where newSize is the value returned by op, guaranteed <= count.
 */

#include "util/Exception.h"

namespace ql {

// Provide a C++17-compatible backport of C++23's `std::basic_string::resize_and_overwrite` as a free function that takes the string as the first parameter.

#if defined(__cpp_lib_string_resize_and_overwrite) && \
    __cpp_lib_string_resize_and_overwrite >= 202110L
  str.resize_and_overwrite(count, std::forward<Operation>(op));
#else
  std::vector<CharT> buffer(count);
  const size_t newSize = std::forward<Operation>(op)(buffer.data(), count);
  AD_CONTRACT_CHECK(newSize <= count);
  str.assign(buffer.data(), newSize);
#endif
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_STRING_H

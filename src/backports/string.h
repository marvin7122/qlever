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

#include "backports/concepts.h"
#include "util/Exception.h"
#include <cstddef>

namespace ql {

// Provide a C++17-compatible backport of C++23's `std::basic_string::resize_and_overwrite` as a free
// function that takes the string as the first parameter.
CPP_template(typename CharT, typename Traits, typename Allocator,
             typename Operation)(
    requires ql::concepts::invocable<
        Operation, CharT*, size_t> &&
    ql::concepts::convertible_to<
        decltype(std::declval<Operation>()(std::declval<CharT*>(),
                                           std::declval<size_t>())),
        size_t>) void resize_and_overwrite(std::basic_string<CharT, Traits,
                                                             Allocator>& str,
                                           size_t count, Operation&& op) {
#if defined(__cpp_lib_string_resize_and_overwrite) && \
    __cpp_lib_string_resize_and_overwrite >= 202110L
  str.resize_and_overwrite(count, std::forward<Operation>(op));
#else
  str.resize(count);
  const size_t newSize = std::forward<Operation>(op)(str.data(), count);
  AD_CONTRACT_CHECK(newSize <= count);
  str.resize(newSize);
#endif
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_STRING_H

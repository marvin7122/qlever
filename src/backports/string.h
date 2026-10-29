// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_STRING_H
#define QLEVER_SRC_BACKPORTS_STRING_H

#include <cstddef>
#include <string>
#include <utility>

#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

namespace ql {

// C++17-compatible backport of C++23's
// `std::basic_string::resize_and_overwrite` as a free function taking the
// string first. Like the standard, `op` is taken by value, called once as an
// rvalue with `(data, count)`, must write at most `count` characters (of type
// `CharT`), must not throw, and returns the new size. Unlike the standard (a
// result larger than `count` is UB), the result is checked with
// `AD_CONTRACT_CHECK` after the call. The content of `str` is unspecified
// after such a violation. Before C++23 there is no performance benefit: the
// fallback calls `resize(count)`, which value-initializes the characters that
// it appends, and the buffer passed to `op` holds the old content of `str`
// (the C++23 branch passes indeterminate values, so `op` must not read it).
CPP_template(typename CharT, typename Traits, typename Allocator,
             typename Operation)(
    requires ad_utility::InvocableWithConvertibleReturnType<Operation, size_t,
                                                            CharT*, size_t>)
void resize_and_overwrite(std::basic_string<CharT, Traits, Allocator>& str,
                          size_t count, Operation op) {
  // Forward to the standard member when the feature-test macro
  // `__cpp_lib_string_resize_and_overwrite` is at least `202110L` (C++23,
  // P1072R10). Otherwise, call `resize`, overwrite, and then shrink.
  // See P1072R10 for `basic_string::resize_and_overwrite`:
  // https://web.archive.org/web/20260102024635/https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2021/p1072r10.html
  // See [version.syn] of the C++ draft for the feature-test macro value:
  // https://web.archive.org/web/20251223023007/http://eel.is/c++draft/version.syn
#if defined(__cpp_lib_string_resize_and_overwrite) && \
    __cpp_lib_string_resize_and_overwrite >= 202110L
  // Throwing inside the operation passed to the standard member is UB, so the
  // result is only recorded there (an out-of-range result is replaced by 0,
  // so the standard member never sees an invalid size) and checked after the
  // call returns.
  size_t newSize = 0;
  str.resize_and_overwrite(
      count, [&newSize, op = std::move(op)](CharT* buffer, size_t n) mutable {
        newSize = std::move(op)(buffer, n);
        return newSize <= n ? newSize : size_t{0};
      });
  AD_CONTRACT_CHECK(newSize <= count);
#else
  str.resize(count);
  const size_t newSize = std::move(op)(str.data(), count);
  AD_CONTRACT_CHECK(newSize <= count);
  str.resize(newSize);
#endif
}

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_STRING_H

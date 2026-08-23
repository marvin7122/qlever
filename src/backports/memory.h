// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_MEMORY_H
#define QLEVER_SRC_BACKPORTS_MEMORY_H

#include <memory>
#include <type_traits>

namespace ql {

#ifndef QLEVER_CPP_17

using std::make_unique_for_overwrite;

#else

// C++17 backport of std::make_unique_for_overwrite (C++20)

// 1. For non-array types:
template <typename T>
std::enable_if_t<!std::is_array_v<T>, std::unique_ptr<T> >
make_unique_for_overwrite() {
  return std::unique_ptr<T>(new T);
}

// 2. For arrays of unknown bound: `make_unique_for_overwrite<T[]>(size)`
template <typename T>
std::enable_if_t<std::is_unbounded_array_v<T>, std::unique_ptr<T> >
make_unique_for_overwrite(size_t size) {
  return std::unique_ptr<T>(new std::remove_extent_t<T>[size]);
}

// 3. Disallowed for arrays of known bound (e.g.
// `make_unique_for_overwrite<T[N]>`):
template <typename T, typename... Args>
std::enable_if_t<std::is_bounded_array_v<T> > make_unique_for_overwrite(
    Args&&...) = delete;

#endif

}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_MEMORY_H

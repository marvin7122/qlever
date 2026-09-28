// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_CPUFEATURES_H
#define QLEVER_SRC_UTIL_CPUFEATURES_H

namespace ad_utility {

// Return true iff the CPU that runs the program supports AVX2. Code that is
// compiled with `__attribute__((target("avx2")))` must only be called if this
// returns true, because the build itself does not require AVX2.
inline bool cpuHasAvx2() noexcept {
#if (defined(__x86_64__) || defined(__i386__)) && \
    (defined(__GNUC__) || defined(__clang__))
  static const bool hasAvx2 = [] {
    __builtin_cpu_init();
    return __builtin_cpu_supports("avx2") != 0;
  }();
  return hasAvx2;
#else
  return false;
#endif
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CPUFEATURES_H

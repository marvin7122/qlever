// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_DANGLINGVIEWTESTHELPERS_H
#define QLEVER_TEST_UTIL_DANGLINGVIEWTESTHELPERS_H

#include <cstddef>

// _____________________________________________________________________________
// STRICTLY TEST-LOCAL BEST-EFFORT TRIPWIRE — NOT A CORRECTNESS MECHANISM.
// Overwrite the current stack frame with sentinel bytes, so that stale stack
// contents (e.g. from a destroyed local object that a dangling view still
// points into) become implausible to survive. Call it multiple times to also
// clobber deeper frames. Returns the last byte written, read back through the
// `volatile` buffer, so callers can assert that the stack was actually
// overwritten with the sentinel.
template <size_t NumBytes = 4096>
[[gnu::noinline]] char clobberStack(char sentinel = '#') {
  // `volatile` prevents the compiler from optimizing the stack writes away.
  static_assert(NumBytes > 0, "clobberStack requires a non-empty buffer");
  volatile char buffer[NumBytes];
  for (size_t i = 0; i < NumBytes; ++i) {
    buffer[i] = sentinel;
  }
  // Compiler barrier: prevents the optimizer from eliding the stack writes or
  // reordering them past the return. Not a hardware memory fence.
  asm volatile("" : : "r"(buffer) : "memory");
  return buffer[NumBytes - 1];
}

#endif  // QLEVER_TEST_UTIL_DANGLINGVIEWTESTHELPERS_H

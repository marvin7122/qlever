// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <chrono>
#include <iostream>
#include <vector>

#include "engine/LeapfrogTriejoin.h"
#include "global/Id.h"

using namespace ql::engine::wcoj;

int main() {
  constexpr size_t N = 1'000'000;
  std::cout << "Benchmarking Leapfrog Triejoin (WCOJ) on 3 streams of " << N << " sorted elements...\n";

  std::vector<Id> listA;
  std::vector<Id> listB;
  std::vector<Id> listC;

  listA.reserve(N);
  listB.reserve(N);
  listC.reserve(N);

  for (size_t i = 0; i < N; ++i) {
    listA.push_back(Id::makeFromInt(static_cast<int>(i * 2)));
    listB.push_back(Id::makeFromInt(static_cast<int>(i * 3)));
    listC.push_back(Id::makeFromInt(static_cast<int>(i * 6)));
  }

  auto t0 = std::chrono::high_resolution_clock::now();
  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);
  iterators.emplace_back(listC);

  auto matches = LeapfrogJoin::intersect(iterators);
  auto t1 = std::chrono::high_resolution_clock::now();

  double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  std::cout << "Leapfrog 3-Way Intersection: " << ms << " ms ("
            << (N / (ms / 1000.0)) / 1e6 << " M items/sec, matches: " << matches.size() << ")\n";

  return 0;
}

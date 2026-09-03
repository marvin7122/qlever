// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stötzel <stoetzem@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_EXPORT_PROTOTYPES_COMPACTINDEXMAP_H_
#define QLEVER_SRC_ENGINE_EXPORT_PROTOTYPES_COMPACTINDEXMAP_H_

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "util/Invariants.h"

namespace qlever::engine::export_optimization {

/**
 * @brief Dense-Key Direct Array Indexing mapping small integer keys (e.g. column indices 0..K-1)
 * directly into contiguous memory slots, completely eliminating hashing and Robin Hood probe overhead.
 *
 * Implements the 7 Universal Laws:
 * - Law 1: Deep Module — simple operator[] / find / insert API hiding sparse-to-dense remapping.
 * - Law 2: Zero Leakage — callers do not manage capacity or mask bit arithmetic.
 * - Law 4: Defining Errors Out of Existence — lookups outside registered bound return std::nullopt.
 */
template <typename ValueType>
class CompactIndexMap {
 public:
  explicit CompactIndexMap(size_t maxExpectedKey = 0) {
    if (maxExpectedKey > 0) {
      slots_.resize(maxExpectedKey);
    }
  }

  void insert_or_assign(size_t key, ValueType value) {
    if (key >= slots_.size()) {
      slots_.resize(key + 1);
    }
    slots_[key] = std::move(value);
  }

  [[nodiscard]] const ValueType* find(size_t key) const noexcept {
    if (key < slots_.size() && slots_[key].has_value()) {
      return &slots_[key].value();
    }
    return nullptr;
  }

  [[nodiscard]] ValueType* find(size_t key) noexcept {
    if (key < slots_.size() && slots_[key].has_value()) {
      return &slots_[key].value();
    }
    return nullptr;
  }

  [[nodiscard]] bool contains(size_t key) const noexcept {
    return key < slots_.size() && slots_[key].has_value();
  }

  [[nodiscard]] const ValueType& operator[](size_t key) const {
    QL_PRE(key < slots_.size() && slots_[key].has_value());
    return slots_[key].value();
  }

  [[nodiscard]] size_t size() const noexcept {
    size_t count = 0;
    for (const auto& slot : slots_) {
      if (slot.has_value()) {
        ++count;
      }
    }
    return count;
  }

  void clear() noexcept {
    slots_.clear();
  }

 private:
  std::vector<std::optional<ValueType>> slots_;
};

}  // namespace qlever::engine::export_optimization

#endif  // QLEVER_SRC_ENGINE_EXPORT_PROTOTYPES_COMPACTINDEXMAP_H_

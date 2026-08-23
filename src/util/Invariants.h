// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_INVARIANTS_H
#define QLEVER_SRC_UTIL_INVARIANTS_H

#include <concepts>
#include <utility>

#include "util/Exception.h"

namespace ad_utility {

// Concept requiring a class to provide a callable invariant check method.
template <typename T>
concept HasCheckInvariants = requires(const T& t) {
  { t.checkInvariants() } -> std::same_as<void>;
};

// _____________________________________________________________________________
// CRTP mixin that enforces at compile-time that `Derived` implements
// `void checkInvariants() const`, and provides a zero-overhead RAII guard
// (`makeInvariantGuard()`) to automatically verify invariants upon method entry
// and exit.
template <typename Derived>
class WithInvariants {
 protected:
  // ___________________________________________________________________________
  // RAII Guard that asserts class invariants on scope entry and scope exit.
  class InvariantGuard {
   private:
    const Derived* self_;

   public:
    explicit InvariantGuard(const Derived* self) : self_{self} {
      AD_CORRECTNESS_CHECK(self_ != nullptr);
      self_->checkInvariants();
    }

    ~InvariantGuard() { self_->checkInvariants(); }

    InvariantGuard(const InvariantGuard&) = delete;
    InvariantGuard& operator=(const InvariantGuard&) = delete;
    InvariantGuard(InvariantGuard&&) noexcept = default;
    InvariantGuard& operator=(InvariantGuard&&) noexcept = delete;
  };

  // ___________________________________________________________________________
  // Instantiate an InvariantGuard verifying the derived instance.
  [[nodiscard]] InvariantGuard makeInvariantGuard() const {
    static_assert(
        HasCheckInvariants<Derived>,
        "Classes inheriting from WithInvariants<T> must implement `void "
        "checkInvariants() const`");
    return InvariantGuard{static_cast<const Derived*>(this)};
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_INVARIANTS_H

// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_INVARIANTS_H
#define QLEVER_SRC_UTIL_INVARIANTS_H

#include <exception>

#include "backports/concepts.h"
#include "util/Exception.h"

namespace ad_utility {

namespace detail {
template <typename T>
CPP_requires(is_invariant_stateful_class_,
             requires(const T& t)(t.checkInvariants()));
}  // namespace detail

// _____________________________________________________________________________
template <typename T>
CPP_concept InvariantStatefulClass =
    CPP_requires_ref(detail::is_invariant_stateful_class_, T);

// _____________________________________________________________________________
// Assert class invariants on scope entry and scope exit, unless exiting via an
// active exception, for any class that satisfies the `InvariantStatefulClass`
// concept.
CPP_template(typename T)(
    requires InvariantStatefulClass<T>) class InvariantGuard {
 private:
  // Non-owning pointer to the guarded object; the guard must not outlive this
  // object.
  const T* self_;
  // Exception count at construction distinguishes normal scope exit from
  // stack unwinding, allowing the exit invariant check to be skipped when
  // unwinding through this scope.
  int uncaughtExceptionsAtConstruction_;

 public:
  explicit InvariantGuard(const T* self)
      : self_{self},
        uncaughtExceptionsAtConstruction_{std::uncaught_exceptions()} {
    AD_CORRECTNESS_CHECK(self_ != nullptr);
    self_->checkInvariants();
  }

  ~InvariantGuard() noexcept(false) {
    AD_CORRECTNESS_CHECK(self_ != nullptr);
    if (std::uncaught_exceptions() <= uncaughtExceptionsAtConstruction_) {
      self_->checkInvariants();
    }
  }

  InvariantGuard(const InvariantGuard&) = delete;
  InvariantGuard& operator=(const InvariantGuard&) = delete;
  InvariantGuard(InvariantGuard&&) noexcept = delete;
  InvariantGuard& operator=(InvariantGuard&&) noexcept = delete;
};

// _____________________________________________________________________________
// Provide a parameterless `makeInvariantGuard()` member function and enforce
// `InvariantStatefulClass` for the derived type.
template <typename Derived>
class WithInvariants {
 public:
  // ___________________________________________________________________________
  // Lvalue-qualified so the guard can never be created for a temporary:
  // it stores a raw pointer to `this`, which must outlive the guard's scope.
  [[nodiscard]] auto makeInvariantGuard() const& {
    static_assert(
        InvariantStatefulClass<Derived>,
        "Class inheriting from WithInvariants<Derived> must satisfy the "
        "`ad_utility::InvariantStatefulClass` concept (implement `void "
        "checkInvariants() const`).");
    return InvariantGuard<Derived>{static_cast<const Derived*>(this)};
  }

  // Deleted rvalue overload: calling on a temporary would leave the guard
  // holding a pointer to an object destroyed at the end of the full
  // expression, so reject this at compile time.
  [[nodiscard]] auto makeInvariantGuard() const&& = delete;
};

// _____________________________________________________________________________
// C++26 Contracts emulation: Preconditions, Postconditions, and Assertions.
//
// In native C++26 (P2900 / GCC 16+), `pre(...)`, `post(...)`, and
// `contract_assert(...)` are first-class language keywords on function
// declarations and bodies. In C++20, we emulate them at the top of function
// bodies:
// - `QL_PRE(cond)`: Checks the precondition immediately upon entry.
// - `QL_CONTRACT_ASSERT(cond)`: Checks internal contract assertions at specific checkpoints.
// - `QL_POST(predicate)`: Registers an RAII scope-exit check that verifies the
//   predicate automatically upon normal function exit, while safely bypassing the check
//   if the scope is exiting due to stack unwinding from an in-flight exception.

namespace detail {

template <typename Predicate>
class PostconditionGuard {
 private:
  Predicate predicate_;
  int uncaughtExceptionsAtConstruction_;

 public:
  explicit PostconditionGuard(Predicate&& predicate)
      : predicate_{std::forward<Predicate>(predicate)},
        uncaughtExceptionsAtConstruction_{std::uncaught_exceptions()} {}

  ~PostconditionGuard() noexcept(false) {
    if (std::uncaught_exceptions() <= uncaughtExceptionsAtConstruction_) {
      AD_CONTRACT_CHECK(predicate_());
    }
  }

  PostconditionGuard(const PostconditionGuard&) = delete;
  PostconditionGuard& operator=(const PostconditionGuard&) = delete;
  PostconditionGuard(PostconditionGuard&&) = delete;
  PostconditionGuard& operator=(PostconditionGuard&&) = delete;
};

template <typename Predicate>
[[nodiscard]] auto makePostconditionGuard(Predicate&& predicate) {
  return PostconditionGuard<std::decay_t<Predicate>>{
      std::forward<Predicate>(predicate)};
}

}  // namespace detail

}  // namespace ad_utility

#define QLEVER_CONCAT_IMPL(a, b) a##b
#define QLEVER_CONCAT(a, b) QLEVER_CONCAT_IMPL(a, b)

// Precondition check: evaluated immediately upon execution.
#define QL_PRE(condition) AD_CONTRACT_CHECK(condition)

// Internal contract assertion: evaluated immediately at the checkpoint.
#define QL_CONTRACT_ASSERT(condition) AD_CONTRACT_CHECK(condition)

// Postcondition check: registered at function entry and evaluated upon normal scope exit.
#define QL_POST(...)                                                           \
  auto QLEVER_CONCAT(ql_postcondition_guard_, __LINE__) =                      \
      ::ad_utility::detail::makePostconditionGuard([&]() -> bool {             \
        return static_cast<bool>(__VA_ARGS__);                                 \
      })

#endif  // QLEVER_SRC_UTIL_INVARIANTS_H

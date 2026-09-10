// Copyright 2026, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#pragma once

#include <type_traits>
#include <utility>

// Mechanical invariant discipline for stateful classes. A class opts in by
// inheriting publicly from `ad_utility::WithInvariants<Derived>` and
// providing a public `void checkInvariants() const` that asserts its
// structural invariants with `AD_CORRECTNESS_CHECK`. Mutating methods open
// with `auto guard = makeInvariantGuard();` (or
// `this->makeInvariantGuard()` in templates); the guard verifies the
// invariants on entry and on exit, so a method that corrupts state fails at
// the method boundary instead of surfacing far downstream.
//
// Deliberately C++17: headers in `engine/export_v2` still compile under the
// GCC 8 CI job, so this facility uses variable templates and SFINAE rather
// than concepts.

namespace ad_utility {

// CRTP base for invariant-bearing stateful classes. `Derived` must provide
// `void checkInvariants() const`.
template <typename Derived>
class WithInvariants {
 protected:
  // Verifies the owning object's invariants on construction and on
  // destruction. Non-copyable and non-movable: exactly one guard spans each
  // checked scope.
  class InvariantGuard {
   public:
    explicit InvariantGuard(const Derived* self) : self_{self} {
      self_->checkInvariants();
    }
    ~InvariantGuard() { self_->checkInvariants(); }

    InvariantGuard(const InvariantGuard&) = delete;
    InvariantGuard& operator=(const InvariantGuard&) = delete;
    InvariantGuard(InvariantGuard&&) = delete;
    InvariantGuard& operator=(InvariantGuard&&) = delete;

   private:
    const Derived* self_;
  };

  // Open a checked scope over `*this`. `const` so guards also work in
  // const observers that transiently re-lock internal mutexes.
  [[nodiscard]] InvariantGuard makeInvariantGuard() const {
    return InvariantGuard(static_cast<const Derived*>(this));
  }

  ~WithInvariants() = default;
};

namespace detail {
template <typename T, typename = void>
struct HasCheckInvariants : std::false_type {};
template <typename T>
struct HasCheckInvariants<
    T, std::void_t<decltype(std::declval<const T&>().checkInvariants())>>
    : std::true_type {};
}  // namespace detail

// True when `T` participates in the mechanical invariant discipline: it
// inherits the guard facility and provides the `checkInvariants` hook the
// guard calls. Use in `static_assert` next to each opt-in class.
template <typename T>
inline constexpr bool InvariantStatefulClass =
    std::is_base_of<WithInvariants<T>, T>::value &&
    detail::HasCheckInvariants<T>::value;

}  // namespace ad_utility

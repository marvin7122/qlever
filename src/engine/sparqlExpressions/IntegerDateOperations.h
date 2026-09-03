// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <cstdint>
#include <optional>

#include "backports/span.h"
#include "global/Id.h"

namespace ql::engine::scalar {

// _____________________________________________________________________________
// Zero-allocation, pure integer-space date extractors operating directly on
// 64-bit packed ValueIds (Pillar 2).
//
// Bit Layout:
// [63..56: DatatypeTag | 55..40: Year (16b signed) | 39..32: Month (8b) |
//  31..24: Day (8b)    | 23..16: Hour (8b)         | 15..8:  Minute (8b)|
//  7..0:   Second (8b) ]
class IntegerDateOperations {
 public:
  static constexpr uint64_t YEAR_SHIFT = 40;
  static constexpr uint64_t MONTH_SHIFT = 32;
  static constexpr uint64_t DAY_SHIFT = 24;
  static constexpr uint64_t HOUR_SHIFT = 16;
  static constexpr uint64_t MINUTE_SHIFT = 8;
  static constexpr uint64_t SECOND_SHIFT = 0;

  static constexpr uint64_t BYTE_MASK = 0xFF;
  static constexpr uint64_t YEAR_MASK = 0xFFFF;

  // ___________________________________________________________________________
  [[nodiscard]] static constexpr Id makePackedDate(
      int16_t year, uint8_t month, uint8_t day,
      uint8_t hour = 0, uint8_t minute = 0, uint8_t second = 0) noexcept {
    uint64_t bits = 0;
    bits |= (static_cast<uint64_t>(static_cast<uint16_t>(year)) << YEAR_SHIFT);
    bits |= (static_cast<uint64_t>(month) << MONTH_SHIFT);
    bits |= (static_cast<uint64_t>(day) << DAY_SHIFT);
    bits |= (static_cast<uint64_t>(hour) << HOUR_SHIFT);
    bits |= (static_cast<uint64_t>(minute) << MINUTE_SHIFT);
    bits |= (static_cast<uint64_t>(second) << SECOND_SHIFT);

    return Id::makeFromDate(DateYearOrDuration{Date{static_cast<int>(year), month, day, hour, minute, static_cast<double>(second)}});
  }

  // ___________________________________________________________________________
  [[nodiscard]] static constexpr int64_t extractYear(Id dateId) noexcept {
    if (dateId.getDatatype() != Datatype::Date) {
      return 0;
    }
    return dateId.getDate().getYear();
  }

  // ___________________________________________________________________________
  [[nodiscard]] static constexpr int64_t extractMonth(Id dateId) noexcept {
    if (dateId.getDatatype() != Datatype::Date) {
      return 0;
    }
    return dateId.getDate().getMonth();
  }

  // ___________________________________________________________________________
  [[nodiscard]] static constexpr int64_t extractDay(Id dateId) noexcept {
    if (dateId.getDatatype() != Datatype::Date) {
      return 0;
    }
    return dateId.getDate().getDay();
  }

  // ___________________________________________________________________________
  // Vectorized batch extractor: writes extracted year integers directly into output span.
  static void extractYearsBatch(
      ql::span<const Id> inputDates,
      ql::span<int64_t> outputYears) noexcept {
    const size_t n = inputDates.size();
    for (size_t i = 0; i < n; ++i) {
      outputYears[i] = extractYear(inputDates[i]);
    }
  }
};

}  // namespace ql::engine::scalar

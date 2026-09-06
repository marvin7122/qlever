// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "engine/Result.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/Exception.h"
#include "util/stream_generator.h"

namespace ql::engine::export_v2 {

// One half-open row window `[begin_, end_)` of a lazy result block selected
// for export. Shared ownership keeps the block alive for async scheduler
// workers after the driver has moved on to later blocks.
struct ExportMorselSegment {
  std::shared_ptr<Result::IdTableVocabPair> block_;
  uint64_t begin_ = 0;
  uint64_t end_ = 0;
};

// Up to `rowsPerMorsel` export rows, gathered across lazy result blocks. Every
// segment serializes straight from its block, so planning and serializing a
// morsel copies no `Id`: there is no `sliced` table and no second rechunk
// copy.
struct ExportMorsel {
  std::vector<ExportMorselSegment> segments_;
  uint64_t numRows_ = 0;
  [[nodiscard]] bool empty() const { return numRows_ == 0; }
};

// Stream morsel plans over the lazy result blocks, applying OFFSET, LIMIT,
// and the export (send) limit exactly like Legacy `getRowIndices`: the offset
// skips rows, both limits count down by *counted* rows, exports never exceed
// either limit, rows past the export limit are still counted but never
// serialized, and pulling stops as soon as both limits are exhausted (LIMIT 0
// pulls no block at all).
inline cppcoro::generator<ExportMorsel> planExportMorsels(
    Result::LazyResult idTables, const LimitOffsetClause& limitOffset,
    uint64_t rowsPerMorsel) {
  AD_CONTRACT_CHECK(rowsPerMorsel > 0);
  constexpr uint64_t unbounded = std::numeric_limits<uint64_t>::max();
  auto reduce = [](uint64_t& value, uint64_t subtrahend) {
    if (value != unbounded) {
      value = value > subtrahend ? value - subtrahend : 0;
    }
  };
  uint64_t offset = limitOffset._offset;
  uint64_t limit = limitOffset.limitOrDefault();
  uint64_t exportLimit = std::min(limitOffset.exportLimitOrDefault(), limit);
  if (limit == 0 && exportLimit == 0) {
    co_return;
  }
  ExportMorsel morsel;
  for (auto&& pair : idTables) {
    const uint64_t rows = pair.idTable_.numRows();
    if (offset >= rows) {
      offset -= rows;
      continue;
    }
    const uint64_t begin = std::exchange(offset, 0);
    const uint64_t counted = std::min(limit, rows - begin);
    const uint64_t exported = std::min(exportLimit, rows - begin);
    if (exported > 0) {
      auto block = std::make_shared<Result::IdTableVocabPair>(std::move(pair));
      uint64_t cursor = begin;
      uint64_t remaining = exported;
      while (remaining > 0) {
        const uint64_t take =
            std::min(remaining, rowsPerMorsel - morsel.numRows_);
        morsel.segments_.push_back({block, cursor, cursor + take});
        morsel.numRows_ += take;
        cursor += take;
        remaining -= take;
        if (morsel.numRows_ == rowsPerMorsel) {
          co_yield std::move(morsel);
          morsel = ExportMorsel{};
        }
      }
    }
    reduce(limit, counted);
    reduce(exportLimit, counted);
    if (limit == 0 && exportLimit == 0) {
      break;
    }
  }
  if (!morsel.empty()) {
    co_yield std::move(morsel);
  }
}

}  // namespace ql::engine::export_v2
